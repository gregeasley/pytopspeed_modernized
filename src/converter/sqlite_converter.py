"""
SQLite Converter for migrating data from TopSpeed .phd files to SQLite databases
"""

import sqlite3
import os
import sys
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pytopspeed import TPS
from converter.schema_mapper import TopSpeedToSQLiteMapper


class SqliteConverter:
    """Converts TopSpeed .phd files to SQLite databases with full data migration"""
    
    def __init__(self, batch_size: int = 1000, progress_callback=None):
        """
        Initialize the SQLite converter
        
        Args:
            batch_size: Number of records to process in each batch
            progress_callback: Optional callback function for progress updates
        """
        self.batch_size = batch_size
        self.progress_callback = progress_callback
        self.schema_mapper = TopSpeedToSQLiteMapper()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging for the converter"""
        logger = logging.getLogger('SqliteConverter')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _update_progress(self, current: int, total: int, message: str = ""):
        """Update progress if callback is provided"""
        if self.progress_callback:
            self.progress_callback(current, total, message)
        else:
            percentage = (current / total * 100) if total > 0 else 0
            self.logger.info(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")
    
    def _convert_field_value(self, field, value) -> Any:
        """
        Convert TopSpeed field value to SQLite-compatible value
        
        Args:
            field: TopSpeed field definition
            value: Raw field value
            
        Returns:
            Converted value suitable for SQLite
        """
        if value is None:
            return None
            
        # Handle different data types
        if field.type == 'STRING':
            # Convert to string and strip null bytes
            if isinstance(value, bytes):
                return value.decode('ascii', errors='replace').rstrip('\x00')
            return str(value).rstrip('\x00') if value else None
            
        elif field.type in ['CSTRING', 'PSTRING']:
            # Null-terminated strings
            if isinstance(value, bytes):
                return value.decode('ascii', errors='replace').rstrip('\x00')
            return str(value).rstrip('\x00') if value else None
            
        elif field.type in ['BYTE', 'SHORT', 'USHORT', 'LONG', 'ULONG']:
            # Integer types
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
                
        elif field.type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
            # Floating point types
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
                
        elif field.type == 'DATE':
            # Date format: 0xYYYYMMDD
            if isinstance(value, int) and value > 0:
                # Convert from TopSpeed date format
                year = (value >> 16) & 0xFFFF
                month = (value >> 8) & 0xFF
                day = value & 0xFF
                if year > 1900 and 1 <= month <= 12 and 1 <= day <= 31:
                    return f"{year:04d}-{month:02d}-{day:02d}"
            return None
            
        elif field.type == 'TIME':
            # Time format: 0xHHMMSSHS
            if isinstance(value, int) and value > 0:
                # Convert from TopSpeed time format
                hour = (value >> 24) & 0xFF
                minute = (value >> 16) & 0xFF
                second = (value >> 8) & 0xFF
                if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
                    return f"{hour:02d}:{minute:02d}:{second:02d}"
            return None
            
        elif field.type == 'GROUP':
            # Binary data
            if isinstance(value, bytes):
                return value
            return None
            
        else:
            # Default: convert to string
            return str(value) if value is not None else None
    
    def _convert_record_to_tuple(self, record, table_def) -> Tuple:
        """
        Convert a TopSpeed record to a tuple of values for SQLite insertion
        
        Args:
            record: TopSpeed record (dict or object)
            table_def: Table definition
            
        Returns:
            Tuple of converted values
        """
        values = []
        
        # Convert regular fields
        for field in table_def.fields:
            # Handle both dict and object record types
            if isinstance(record, dict):
                field_value = record.get(field.name, None)
            else:
                field_value = getattr(record, field.name, None)
            
            converted_value = self._convert_field_value(field, field_value)
            values.append(converted_value)
        
        # Convert memo fields
        for memo in table_def.memos:
            memo_value = None
            try:
                # Handle both dict and object record types
                if isinstance(record, dict):
                    # For dict records, memo data should already be included
                    memo_value = record.get(memo.name, None)
                else:
                    # Try to get memo data using the enhanced memo handling
                    if hasattr(record, '_get_memo_data') and hasattr(record, 'record_number'):
                        memo_data = record._get_memo_data(record.record_number, memo)
                        if memo_data:
                            memo_value = memo_data
            except:
                memo_value = None
            values.append(memo_value)
        
        return tuple(values)
    
    def _create_schema(self, tps: TPS, conn: sqlite3.Connection, file_prefix: str = "") -> Dict[str, str]:
        """
        Create SQLite schema from TopSpeed file
        
        Args:
            tps: TopSpeed file object
            conn: SQLite connection
            file_prefix: Optional prefix for table names to avoid collisions
            
        Returns:
            Dictionary mapping table names to sanitized names
        """
        cursor = conn.cursor()
        table_mapping = {}
        
        self.logger.info("Creating SQLite schema...")
        
        # Process each table
        for table_number in tps.tables._TpsTablesList__tables:
            table = tps.tables._TpsTablesList__tables[table_number]
            
            if table.name and table.name != '':
                try:
                    # Get table definition
                    table_def = tps.tables.get_definition(table_number)
                    
                    # Map schema
                    schema = self.schema_mapper.map_table_schema(table.name, table_def)
                    sanitized_table_name = schema['table_name']
                    
                    # Apply file prefix if provided
                    if file_prefix:
                        sanitized_table_name = f"{file_prefix}{sanitized_table_name}"
                        # Update the schema with the new table name
                        schema['table_name'] = sanitized_table_name
                        schema['create_table'] = schema['create_table'].replace(
                            f"CREATE TABLE {schema['table_name'].replace(file_prefix, '')}",
                            f"CREATE TABLE {sanitized_table_name}"
                        )
                        # Update index names to include the prefix
                        new_indexes = []
                        for index_sql in schema['create_indexes']:
                            # Extract index name and update it
                            if "CREATE INDEX" in index_sql:
                                parts = index_sql.split(" ON ")
                                if len(parts) == 2:
                                    index_name_part = parts[0].replace("CREATE INDEX ", "")
                                    table_part = parts[1].split(" (")[0]
                                    new_index_name = f"{file_prefix}{index_name_part}"
                                    new_table_name = f"{file_prefix}{table_part}"
                                    new_index_sql = f"CREATE INDEX {new_index_name} ON {new_table_name} ({parts[1].split(' (')[1]}"
                                    new_indexes.append(new_index_sql)
                                else:
                                    new_indexes.append(index_sql)
                            else:
                                new_indexes.append(index_sql)
                        schema['create_indexes'] = new_indexes
                    
                    table_mapping[table.name] = sanitized_table_name
                    
                    # Create table
                    cursor.execute(schema['create_table'])
                    self.logger.info(f"Created table: {sanitized_table_name}")
                    
                    # Create indexes
                    for index_sql in schema['create_indexes']:
                        cursor.execute(index_sql)
                        self.logger.info(f"Created index for: {sanitized_table_name}")
                        
                except Exception as e:
                    self.logger.error(f"Error creating schema for table {table.name}: {e}")
                    continue
        
        conn.commit()
        self.logger.info("Schema creation completed")
        return table_mapping
    
    def _migrate_table_data(self, tps: TPS, table_name: str, sanitized_table_name: str, 
                           conn: sqlite3.Connection) -> int:
        """
        Migrate data for a single table
        
        Args:
            tps: TopSpeed file object
            table_name: Original table name
            sanitized_table_name: Sanitized table name
            conn: SQLite connection
            
        Returns:
            Number of records migrated
        """
        cursor = conn.cursor()
        
        try:
            # Set current table
            tps.set_current_table(table_name)
            table_def = tps.tables.get_definition(tps.current_table_number)
            
            # Get field names for INSERT statement
            field_names = []
            for field in table_def.fields:
                sanitized_field_name = self.schema_mapper.sanitize_field_name(field.name)
                field_names.append(sanitized_field_name)
            
            for memo in table_def.memos:
                sanitized_memo_name = self.schema_mapper.sanitize_field_name(memo.name)
                field_names.append(sanitized_memo_name)
            
            # Create INSERT statement
            placeholders = ', '.join(['?' for _ in field_names])
            insert_sql = f"INSERT INTO {sanitized_table_name} ({', '.join(field_names)}) VALUES ({placeholders})"
            
            # Process records in batches
            batch = []
            record_count = 0
            
            for record in tps:
                try:
                    # Convert record to tuple
                    record_tuple = self._convert_record_to_tuple(record, table_def)
                    batch.append(record_tuple)
                    
                    # Insert batch when it reaches batch_size
                    if len(batch) >= self.batch_size:
                        cursor.executemany(insert_sql, batch)
                        record_count += len(batch)
                        self._update_progress(record_count, 0, f"Migrated {record_count} records from {table_name}")
                        batch = []
                        
                except Exception as e:
                    self.logger.warning(f"Error processing record in {table_name}: {e}")
                    continue
            
            # Insert remaining records
            if batch:
                cursor.executemany(insert_sql, batch)
                record_count += len(batch)
            
            conn.commit()
            self.logger.info(f"Migrated {record_count} records from {table_name}")
            return record_count
            
        except Exception as e:
            self.logger.error(f"Error migrating data for table {table_name}: {e}")
            conn.rollback()
            return 0
    
    def convert(self, phd_file: str, output_file: str) -> Dict[str, Any]:
        """
        Convert TopSpeed .phd file to SQLite database
        
        Args:
            phd_file: Path to input .phd file
            output_file: Path to output SQLite file
            
        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'tables_created': 0,
            'total_records': 0,
            'errors': [],
            'duration': 0
        }
        
        try:
            self.logger.info(f"Starting conversion: {phd_file} -> {output_file}")
            
            # Load TopSpeed file
            self.logger.info("Loading TopSpeed file...")
            tps = TPS(phd_file, encoding='cp1251', cached=True, check=True)
            
            # Create SQLite database
            if os.path.exists(output_file):
                os.remove(output_file)
            
            conn = sqlite3.connect(output_file)
            conn.execute("PRAGMA journal_mode=WAL")  # Use WAL mode for better performance
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
            
            try:
                # Create schema
                table_mapping = self._create_schema(tps, conn)
                results['tables_created'] = len(table_mapping)
                
                # Migrate data
                self.logger.info("Starting data migration...")
                total_tables = len(table_mapping)
                
                for i, (table_name, sanitized_table_name) in enumerate(table_mapping.items()):
                    self._update_progress(i, total_tables, f"Migrating table: {table_name}")
                    
                    record_count = self._migrate_table_data(tps, table_name, sanitized_table_name, conn)
                    results['total_records'] += record_count
                
                results['success'] = True
                self.logger.info("Conversion completed successfully")
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Conversion failed: {e}")
            results['errors'].append(str(e))
        
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def convert_multiple(self, input_files: List[str], output_file: str) -> Dict[str, Any]:
        """
        Convert multiple TopSpeed files (.phd, .mod, .tps) to a single SQLite database
        
        Args:
            input_files: List of paths to input files
            output_file: Path to output SQLite file
            
        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'tables_created': 0,
            'total_records': 0,
            'duration': 0,
            'errors': [],
            'files_processed': 0,
            'file_results': {}
        }
        
        try:
            # Create SQLite database
            if os.path.exists(output_file):
                os.remove(output_file)
            
            conn = sqlite3.connect(output_file)
            
            # Configure SQLite for better performance
            conn.execute("PRAGMA journal_mode=WAL")  # Use WAL mode for better performance
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed
            
            try:
                all_table_mapping = {}
                total_tables_processed = 0
                
                # Process each input file
                for file_idx, input_file in enumerate(input_files):
                    self.logger.info(f"Processing file {file_idx + 1}/{len(input_files)}: {input_file}")
                    
                    if not os.path.exists(input_file):
                        error_msg = f"Input file not found: {input_file}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        continue
                    
                    try:
                        # Load TopSpeed file
                        self.logger.info(f"Loading TopSpeed file: {input_file}")
                        tps = TPS(input_file, encoding='cp1251', cached=True, check=True)
                        
                        # Determine file type and appropriate prefix
                        file_ext = os.path.splitext(input_file)[1].lower()
                        if file_ext == '.phd':
                            file_prefix = "phd_"
                        elif file_ext == '.mod':
                            file_prefix = "mod_"
                        elif file_ext == '.tps':
                            file_prefix = "tps_"
                        else:
                            file_prefix = f"file_{file_idx + 1}_"
                        
                        # Create schema for this file
                        file_table_mapping = self._create_schema(tps, conn, file_prefix=file_prefix)
                        
                        # Check for table name collisions
                        for table_name, sanitized_name in file_table_mapping.items():
                            if sanitized_name in all_table_mapping:
                                # Table name collision - add file prefix
                                original_sanitized = sanitized_name
                                sanitized_name = f"{file_prefix}{sanitized_name}"
                                file_table_mapping[table_name] = sanitized_name
                                self.logger.warning(f"Table name collision detected: {table_name} -> {sanitized_name}")
                        
                        # Add to overall mapping
                        all_table_mapping.update(file_table_mapping)
                        
                        # Migrate data for this file
                        self.logger.info(f"Starting data migration for {input_file}...")
                        file_record_count = 0
                        
                        for table_name, sanitized_table_name in file_table_mapping.items():
                            record_count = self._migrate_table_data(tps, table_name, sanitized_table_name, conn)
                            file_record_count += record_count
                        
                        # Store file results
                        results['file_results'][input_file] = {
                            'tables_created': len(file_table_mapping),
                            'records_migrated': file_record_count,
                            'success': True
                        }
                        
                        results['files_processed'] += 1
                        total_tables_processed += len(file_table_mapping)
                        results['total_records'] += file_record_count
                        
                        self.logger.info(f"Completed {input_file}: {len(file_table_mapping)} tables, {file_record_count} records")
                        
                    except Exception as e:
                        error_msg = f"Error processing {input_file}: {e}"
                        self.logger.error(error_msg)
                        results['errors'].append(error_msg)
                        results['file_results'][input_file] = {
                            'tables_created': 0,
                            'records_migrated': 0,
                            'success': False,
                            'error': str(e)
                        }
                
                results['tables_created'] = len(all_table_mapping)
                results['success'] = results['files_processed'] > 0 and len(results['errors']) == 0
                
                if results['success']:
                    self.logger.info(f"Conversion completed successfully: {results['files_processed']} files, {results['tables_created']} tables, {results['total_records']} records")
                else:
                    self.logger.error(f"Conversion completed with errors: {len(results['errors'])} errors")
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"Conversion failed: {e}")
            results['errors'].append(str(e))
        
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
