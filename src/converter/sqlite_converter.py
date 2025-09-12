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
    
    def _convert_multidimensional_record_to_tuple(self, record, table_def, analysis) -> Tuple:
        """
        Convert a TopSpeed record with multi-dimensional arrays to a tuple for SQLite insertion
        
        Args:
            record: TopSpeed record (dict or object)
            table_def: Table definition
            analysis: Multidimensional analysis result
            
        Returns:
            Tuple of converted values
        """
        values = []
        
        try:
            # Handle dictionary records (from TPS iterator)
            if isinstance(record, dict):
                # Build values in the correct order for field_names
                for field_name in analysis['regular_fields']:
                    sanitized_name = self.schema_mapper.sanitize_field_name(field_name.name)
                    values.append(record.get(field_name.name, None))
                
                # Handle array fields - combine individual array elements into JSON
                for array_info in analysis['array_fields']:
                    array_values = []
                    for i in range(array_info.array_size):
                        # Look for individual array elements in the record
                        element_name = f"{array_info.base_name}{i+1}"
                        element_value = record.get(element_name, None)
                        array_values.append(element_value)
                    
                    # Convert to JSON
                    import json
                    values.append(json.dumps(array_values))
                
                return tuple(values)
            
            # Handle raw record objects
            elif hasattr(record, 'data') and hasattr(record.data, 'data'):
                raw_data = record.data.data
                
                # Handle Container objects
                if hasattr(raw_data, 'data'):
                    raw_data = raw_data.data
                
                if isinstance(raw_data, bytes):
                    # Parse regular fields directly from raw data
                    for field in analysis['regular_fields']:
                        try:
                            if field.offset < len(raw_data):
                                field_size = getattr(field, 'size', 8)
                                field_data = raw_data[field.offset:field.offset + field_size]
                                
                                if field.type == 'SHORT':
                                    import struct
                                    value = struct.unpack('<h', field_data)[0]
                                elif field.type == 'LONG':
                                    import struct
                                    value = struct.unpack('<l', field_data)[0]
                                elif field.type == 'DOUBLE':
                                    import struct
                                    value = struct.unpack('<d', field_data)[0]
                                elif field.type == 'BYTE':
                                    # Convert BYTE to boolean: 0 = False, non-zero = True
                                    value = bool(field_data[0])
                                elif field.type in ['BOOL', 'BOOLEAN']:
                                    # Convert BOOL/BOOLEAN to boolean: 0 = False, non-zero = True
                                    value = bool(field_data[0])
                                else:
                                    value = field_data.decode('utf-8', errors='ignore').rstrip('\x00')
                                
                                values.append(value)
                            else:
                                values.append(None)
                        except Exception as e:
                            self.logger.warning(f"Error parsing field {field.name}: {e}")
                            values.append(None)
                    
                    # Parse array fields directly from raw data
                    for array_info in analysis['array_fields']:
                        try:
                            array_values = []
                            
                            # Parse array elements from raw data
                            for i in range(array_info.array_size):
                                offset = array_info.start_offset + i * array_info.element_size
                                if offset + array_info.element_size <= len(raw_data):
                                    element_data = raw_data[offset:offset + array_info.element_size]
                                    
                                    if array_info.element_type == 'DOUBLE':
                                        import struct
                                        value = struct.unpack('<d', element_data)[0]
                                    elif array_info.element_type == 'SHORT':
                                        import struct
                                        value = struct.unpack('<h', element_data)[0]
                                    elif array_info.element_type == 'LONG':
                                        import struct
                                        value = struct.unpack('<l', element_data)[0]
                                    elif array_info.element_type == 'BYTE':
                                        # Convert BYTE to boolean: 0 = False, non-zero = True
                                        value = bool(element_data[0])
                                    elif array_info.element_type in ['BOOL', 'BOOLEAN']:
                                        # Convert BOOL/BOOLEAN to boolean: 0 = False, non-zero = True
                                        value = bool(element_data[0])
                                    else:
                                        value = element_data.decode('utf-8', errors='ignore').rstrip('\x00')
                                    
                                    array_values.append(value)
                                else:
                                    array_values.append(None)
                            
                            # Convert to JSON
                            import json
                            values.append(json.dumps(array_values))
                            
                        except Exception as e:
                            self.logger.warning(f"Error parsing array {array_info.base_name}: {e}")
                            values.append(None)
                
                # Handle memo fields for raw record parsing
                if hasattr(table_def, 'memos') and table_def.memos:
                    for memo in table_def.memos:
                        # For raw records, memo data is typically not available in the raw data
                        # Set to None for now - this could be enhanced to read from memo files if needed
                        values.append(None)
                
                return tuple(values)
            
            # Fall back to original conversion if multidimensional parsing fails
            return self._convert_record_to_tuple(record, table_def)
            
        except Exception as e:
            self.logger.warning(f"Error in multidimensional record conversion: {e}")
            # Fall back to original conversion
            return self._convert_record_to_tuple(record, table_def)
    
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
                    # Get table definition with robust error handling
                    table_def = self._get_table_definition_robust(tps, table_number, table.name)
                    if not table_def:
                        self.logger.warning(f"Skipping table {table.name}: No table definition found")
                        continue
                    
                    # Analyze table structure for multidimensional arrays
                    table_name_str = str(table.name) if hasattr(table.name, '__str__') else table.name
                    
                    try:
                        table_structure = self.schema_mapper.multidimensional_handler.analyze_table_structure(table_def)
                        print(f"DEBUG: Table {table.name} analysis: {table_structure}")
                    except Exception as e:
                        self.logger.warning(f"Skipping table {table.name}: Error analyzing table structure: {e}")
                        continue
                    
                    # Map schema using multidimensional analysis
                    schema = self.schema_mapper.map_table_schema_with_multidimensional(table_name_str, table_def, table_structure, file_prefix)
                    sanitized_table_name = schema['table_name']
                    
                    # Use the original table name as the key for data migration
                    # This ensures the data migration looks for the correct table name
                    table_mapping[table_name_str] = sanitized_table_name
                    
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
            
            # Get table definition with robust error handling
            table_def = self._get_table_definition_robust(tps, tps.current_table_number, table_name)
            if not table_def:
                self.logger.warning(f"Skipping data migration for {table_name}: No table definition")
                return 0
            
            # Check if this is an enhanced table definition (created due to parsing failure)
            # Use enhanced data migration for tables that failed to parse normally
            if hasattr(table_def, 'field_count') and table_def.field_count is not None and table_def.field_count > 30:
                return self._migrate_enhanced_table_data(tps, table_name, sanitized_table_name, conn, table_def)
            
            # Analyze table structure for multi-dimensional fields
            analysis = self.schema_mapper.multidimensional_handler.analyze_table_structure(table_def)
            
            # Get field names for INSERT statement
            field_names = []
            if analysis['has_arrays']:
                # Use multidimensional handler for tables with arrays
                # Add regular fields
                for field in analysis['regular_fields']:
                    sanitized_field_name = self.schema_mapper.sanitize_field_name(field.name)
                    field_names.append(sanitized_field_name)
                
                # Add array fields
                for array_info in analysis['array_fields']:
                    sanitized_field_name = self.schema_mapper.sanitize_field_name(array_info.base_name)
                    field_names.append(sanitized_field_name)
            else:
                # Use original logic for regular tables
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
            
            if analysis['has_arrays']:
                # Check if this table has single-field arrays (large fields) that need raw record access
                # Single-field arrays are detected when the array is stored in one large field (like 96-byte DAT:PROD1)
                # Multi-field arrays are detected when the array is stored in multiple small fields (like CUM:PROD1, CUM:PROD2, etc.)
                has_single_field_arrays = any(
                    # Check if this array was detected as a single-field array
                    # Single-field arrays need raw record access for proper parsing
                    array_info.is_single_field_array
                    for array_info in analysis['array_fields']
                )
                
                if has_single_field_arrays:
                    # For tables with large single-field arrays (like MONHIST), access raw records directly
                    # Get table number for raw record access
                    table_number = None
                    for num, table in tps.tables._TpsTablesList__tables.items():
                        if table.name == table_name:
                            table_number = num
                            break
                    
                    if table_number is None:
                        self.logger.error(f"Could not find table number for {table_name}")
                        return 0
                    
                    # Access raw records directly using TpsRecordsList
                    from pytopspeed.tpsrecord import TpsRecordsList
                    
                    for page_ref in tps.pages.list():
                        if tps.pages[page_ref].hierarchy_level == 0:
                            for record in TpsRecordsList(tps, tps.pages[page_ref], encoding='cp1251', check=True):
                                if record.type == 'DATA' and record.data.table_number == table_number:
                                    try:
                                        # Convert multidimensional record to tuple
                                        record_tuple = self._convert_multidimensional_record_to_tuple(record, table_def, analysis)
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
                else:
                    # For tables with multi-field arrays (like CUMVOL), use regular TPS iterator
                    for record in tps:
                        try:
                            # Convert multidimensional record to tuple
                            record_tuple = self._convert_multidimensional_record_to_tuple(record, table_def, analysis)
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
            else:
                # Use TPS iterator for regular tables
                for record in tps:
                    try:
                        # Convert record to tuple using original logic
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
    
    def _migrate_enhanced_table_data(self, tps, table_name: str, sanitized_table_name: str, 
                                   conn: sqlite3.Connection, table_def) -> int:
        """
        Migrate data for tables with enhanced table definitions
        Uses raw data access to extract records and store them as individual columns
        
        Args:
            tps: TopSpeed file object
            table_name: Original table name
            sanitized_table_name: Sanitized table name
            conn: SQLite connection
            table_def: Enhanced table definition
            
        Returns:
            Number of records migrated
        """
        cursor = conn.cursor()
        record_count = 0
        
        try:
            self.logger.info(f"Migrating enhanced table {table_name} using raw data access")
            
            # Access raw records directly from pages
            for page_ref in tps.pages.list():
                if tps.pages[page_ref].hierarchy_level == 0:
                    page = tps.pages[page_ref]
                    
                    # Check if this page belongs to our table
                    if hasattr(page, 'table_number') and page.table_number != tps.current_table_number:
                        continue
                    
                    # Get records from this page
                    from pytopspeed.tpsrecord import TpsRecordsList
                    records = TpsRecordsList(tps, page, encoding='cp1251', check=True)
                    
                    for record in records:
                        if record.type == 'DATA':
                            # Check if this record belongs to our table
                            record_table_number = None
                            if hasattr(record.data, 'table_number'):
                                record_table_number = record.data.table_number
                            elif hasattr(record, 'table_number'):
                                record_table_number = record.table_number
                            
                            if record_table_number != tps.current_table_number:
                                continue
                            try:
                                # Extract raw data
                                raw_data = None
                                if hasattr(record.data.data, 'data'):
                                    raw_data = record.data.data.data
                                elif hasattr(record.data, 'data'):
                                    raw_data = record.data.data
                                elif hasattr(record, 'data'):
                                    raw_data = record.data
                                
                                if raw_data is None:
                                    continue
                                
                                # Get the actual table schema to match column count
                                cursor.execute(f"PRAGMA table_info({sanitized_table_name})")
                                columns = cursor.fetchall()
                                actual_column_count = len(columns)
                                
                                print(f"DEBUG: Table {sanitized_table_name} has {actual_column_count} columns, table_def has {table_def.field_count} fields")
                                
                                # Parse the raw data using the actual field definitions
                                field_values = []
                                
                                # Use the multidimensional handler to parse the data properly
                                from .multidimensional_handler import MultidimensionalHandler
                                handler = MultidimensionalHandler()
                                
                                # First analyze the table structure
                                try:
                                    analysis = handler.analyze_table_structure(table_def)
                                    parsed_data = handler.parse_record_data(raw_data, analysis)
                                    
                                    # Extract field values in the correct order
                                    # For enhanced tables, we need to map the parsed data to the correct column order
                                    cursor.execute(f"PRAGMA table_info({sanitized_table_name})")
                                    columns = cursor.fetchall()
                                    
                                    for col in columns:
                                        col_name = col[1]
                                        if col_name in parsed_data:
                                            field_values.append(parsed_data[col_name])
                                        else:
                                            field_values.append(None)
                                            
                                except Exception as parse_error:
                                    self.logger.warning(f"Error parsing record data for {table_name}: {parse_error}")
                                    # Fallback to simple parsing
                                    for i in range(min(table_def.field_count, actual_column_count)):
                                        field_values.append(None)
                                
                                # Insert into table
                                if field_values:  # Only insert if we have values
                                    placeholders = ','.join(['?' for _ in field_values])
                                    print(f"DEBUG: Inserting into {sanitized_table_name} with {len(field_values)} values")
                                    print(f"DEBUG: Field values: {field_values[:5]}...")  # Show first 5 values
                                    
                                    # Ensure we have the right number of values
                                    if len(field_values) == actual_column_count:
                                        cursor.execute(f"INSERT INTO {sanitized_table_name} VALUES ({placeholders})", field_values)
                                        record_count += 1
                                    else:
                                        print(f"DEBUG: Skipping insert - field count mismatch: {len(field_values)} vs {actual_column_count}")
                                else:
                                    print(f"DEBUG: Skipping insert - no field values")
                                
                            except Exception as e:
                                self.logger.warning(f"Error processing record in enhanced table {table_name}: {e}")
                                continue
            
            conn.commit()
            self.logger.info(f"Migrated {record_count} records from {table_name}")
            return record_count
            
        except Exception as e:
            self.logger.error(f"Error migrating enhanced table {table_name}: {e}")
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
    
    def _get_table_definition_robust(self, tps, table_number, table_name):
        """
        Get table definition with robust error handling for problematic tables
        
        Args:
            tps: TopSpeed file object
            table_number: Table number
            table_name: Table name for logging
            
        Returns:
            Table definition object or None if parsing fails
        """
        try:
            # Try normal table definition parsing
            return tps.tables.get_definition(table_number)
        except Exception as e:
            # If parsing fails, try to create a more sophisticated table definition
            self.logger.warning(f"Failed to parse table definition for {table_name}: {e}")
            self.logger.info(f"Attempting to create enhanced table definition for {table_name}")
            
            # Try to extract information from raw definition bytes
            try:
                table = tps.tables._TpsTablesList__tables[table_number]
                if hasattr(table, 'definition_bytes'):
                    # Debug: Print raw definition bytes info
                    print(f"DEBUG: Raw definition bytes for {table_name}:")
                    print(f"  Number of portions: {len(table.definition_bytes)}")
                    for portion_num, portion_bytes in table.definition_bytes.items():
                        print(f"  Portion {portion_num}: {len(portion_bytes)} bytes")
                        print(f"  First 100 bytes: {portion_bytes[:100].hex()}")
                    
                    # For multidimensional tables, try to parse each portion separately
                    if len(table.definition_bytes) > 1:
                        print(f"DEBUG: Detected multidimensional table with {len(table.definition_bytes)} portions")
                        # Use enhanced table definition with combined bytes for multidimensional tables
                        combined_bytes = b''
                        for key in sorted(table.definition_bytes.keys()):
                            combined_bytes += table.definition_bytes[key]
                        enhanced_def = self._create_enhanced_table_definition(table_name, {0: combined_bytes})
                    else:
                        # Single portion - use standard enhanced parsing
                        enhanced_def = self._create_enhanced_table_definition(table_name, table.definition_bytes)
                    
                    if enhanced_def:
                        self.logger.info(f"Created enhanced table definition for {table_name}")
                        print(f"DEBUG: Enhanced table definition created for {table_name} with {len(enhanced_def.fields)} fields")
                        print(f"DEBUG: Field names: {[f.name for f in enhanced_def.fields[:5]]}...")  # Show first 5 field names
                        return enhanced_def
                    else:
                        print(f"DEBUG: Enhanced table definition creation failed for {table_name}")
            except Exception as enhanced_error:
                self.logger.warning(f"Enhanced parsing also failed for {table_name}: {enhanced_error}")
            
            # Fall back to minimal table definition
            self.logger.info(f"Creating minimal table definition for {table_name}")
            
            # Create a minimal table definition that can be processed
            class MinimalTableDef:
                def __init__(self, name):
                    self.name = name
                    self.fields = []
                    self.memos = []
                    self.indexes = []
                    self.record_size = 0
                    self.field_count = 0
                    self.memo_count = 0
                    self.index_count = 0
            
            return MinimalTableDef(table_name)
    
    def _migrate_large_array_table_data(self, tps, table_name: str, sanitized_table_name: str, 
                                      conn: sqlite3.Connection, table_def) -> int:
        """
        Migrate data for large array tables using raw record access
        
        Args:
            tps: TopSpeed file object
            table_name: Original table name
            sanitized_table_name: Sanitized table name
            conn: SQLite connection
            table_def: Enhanced table definition
            
        Returns:
            Number of records migrated
        """
        cursor = conn.cursor()
        record_count = 0
        
        try:
            self.logger.info(f"Migrating large array table {table_name} using raw record access")
            
            # Get the table number
            table_number = tps.current_table_number
            
            # Access raw records directly from pages
            self.logger.info(f"Looking for records in table {table_number} for {table_name}")
            
            for page_ref in tps.pages.list():
                if tps.pages[page_ref].hierarchy_level == 0:
                    page = tps.pages[page_ref]
                    self.logger.info(f"Processing page {page_ref}")
                    
                    # Get records from this page
                    from pytopspeed.tpsrecord import TpsRecordsList
                    records = TpsRecordsList(tps, page, encoding='cp1251', check=True)
                    
                    page_record_count = 0
                    for record in records:
                        if record.type == 'DATA':
                            # Check if this record belongs to our table
                            record_table_number = None
                            if hasattr(record.data, 'table_number'):
                                record_table_number = record.data.table_number
                            elif hasattr(record, 'table_number'):
                                record_table_number = record.table_number
                            
                            if record_table_number == table_number:
                                try:
                                    # Extract raw data
                                    raw_data = None
                                    if hasattr(record.data.data, 'data'):
                                        raw_data = record.data.data.data
                                    elif hasattr(record.data, 'data'):
                                        raw_data = record.data.data
                                    elif hasattr(record, 'data'):
                                        raw_data = record.data
                                    
                                    if raw_data is None:
                                        self.logger.warning(f"No raw data found for record in {table_name}")
                                        continue
                                    
                                    # For large array tables, store the entire record as JSON
                                    import json
                                    
                                    # Convert raw bytes to a more readable format
                                    parsed_data = {}
                                    
                                    # Store the raw data as base64 encoded JSON
                                    import base64
                                    raw_data_b64 = base64.b64encode(raw_data).decode('ascii')
                                    parsed_data['raw_data'] = raw_data_b64
                                    parsed_data['data_size'] = len(raw_data)
                                    
                                    # Try to extract some basic information if possible
                                    if len(raw_data) >= 4:
                                        # Try to extract first few bytes as potential IDs
                                        parsed_data['first_4_bytes'] = raw_data[:4].hex()
                                    
                                    # Insert into SQLite
                                    json_data = json.dumps(parsed_data)
                                    
                                    # Get the field name for the JSON data
                                    json_field_name = f"{table_name}_ARRAY_DATA"
                                    
                                    # Create INSERT statement
                                    insert_sql = f'INSERT INTO "{sanitized_table_name}" ("{json_field_name}") VALUES (?)'
                                    
                                    cursor.execute(insert_sql, (json_data,))
                                    record_count += 1
                                    page_record_count += 1
                                    
                                    # Commit every 100 records for performance and memory management
                                    if record_count % 100 == 0:
                                        conn.commit()
                                        self.logger.info(f"Progress: {record_count} records migrated from {table_name}")
                                        
                                        # Memory cleanup for large tables
                                        if record_count % 1000 == 0:
                                            import gc
                                            gc.collect()
                                
                                except Exception as e:
                                    self.logger.warning(f"Error processing record in {table_name}: {e}")
                                    continue
                    
                    self.logger.info(f"Page {page_ref}: {page_record_count} records found for {table_name}")
                    
                    # Don't break - process all pages to find all records
            
            conn.commit()
            self.logger.info(f"Migrated {record_count} records from {table_name}")
            return record_count
            
        except Exception as e:
            self.logger.error(f"Error migrating data for table {table_name}: {e}")
            conn.rollback()
            return 0
    
    def _create_enhanced_table_definition(self, table_name, definition_bytes):
        """
        Create an enhanced table definition by analyzing raw definition bytes
        
        Args:
            table_name: Name of the table
            definition_bytes: Raw definition bytes from the table
            
        Returns:
            Enhanced table definition or None if analysis fails
        """
        try:
            # Combine all definition bytes
            combined_bytes = b''
            for key in sorted(definition_bytes.keys()):
                combined_bytes += definition_bytes[key]
            
            # Try to extract basic information from the header
            if len(combined_bytes) < 10:
                return None
            
            # Parse the header (first 10 bytes)
            import struct
            min_version_driver = struct.unpack('<H', combined_bytes[0:2])[0]
            record_size = struct.unpack('<H', combined_bytes[2:4])[0]
            field_count = struct.unpack('<H', combined_bytes[4:6])[0]
            memo_count = struct.unpack('<H', combined_bytes[6:8])[0]
            index_count = struct.unpack('<H', combined_bytes[8:10])[0]
            
            self.logger.info(f"Enhanced parsing for {table_name}: {field_count} fields, {memo_count} memos, {index_count} indexes, record_size={record_size}")
            
            # Try to extract actual field names from the raw definition bytes
            try:
                actual_fields = self._extract_field_names_from_bytes(combined_bytes, field_count)
            except AttributeError:
                # If the method doesn't exist, create empty list
                print("DEBUG: _extract_field_names_from_bytes method not available, using empty field names")
                actual_fields = []
            except Exception as e:
                # If field extraction fails for any reason, create empty list
                print(f"DEBUG: Field name extraction failed: {e}, using empty field names")
                actual_fields = []
            
            # Create enhanced table definition
            class EnhancedTableDef:
                def __init__(self, name, field_count, memo_count, index_count, record_size, actual_fields, sanitize_func):
                    self.name = name
                    self.fields = []
                    self.memos = []
                    self.indexes = []
                    self.record_size = record_size
                    self.field_count = field_count
                    self.memo_count = memo_count
                    self.index_count = index_count
                    
                    # Create field definitions with actual names if possible, otherwise use generic names
                    limited_field_count = field_count  # Use the actual field count, not limited to 26
                    # Try to parse the actual field definitions from the raw bytes
                    try:
                        from construct import Struct, Int16ul, CString, If, Byte, Enum, BitsInteger
                        
                        # Field type structure (from pytopspeed)
                        FIELD_TYPE_STRUCT = Enum(Byte,
                            BYTE=1, SHORT=2, DATE=3, TIME=4, LONG=5, STRING=6, DECIMAL=7, MEMO=8, BLOB=9,
                            CSTRING=10, PSTRING=11, PICTURE=12, DOUBLE=9, _default_='STRING'
                        )
                        
                        # Table definition field structure (from pytopspeed)
                        TABLE_DEFINITION_FIELD_STRUCT = Struct(
                            "type" / FIELD_TYPE_STRUCT,
                            "offset" / Int16ul,
                            "name" / CString("ascii"),
                            "array_element_count" / Int16ul,
                            "size" / Int16ul,
                            "overlaps" / Int16ul,
                            "number" / Int16ul,
                            "array_element_size" / If(lambda x: x['type'] in ['STRING', 'CSTRING', 'PSTRING', 'PICTURE'], Int16ul),
                            "template" / If(lambda x: x['type'] in ['STRING', 'CSTRING', 'PSTRING', 'PICTURE'], Int16ul),
                            "decimal_count" / If(lambda x: x['type'] == 'DECIMAL', Byte),
                            "decimal_size" / If(lambda x: x['type'] == 'DECIMAL', Byte),
                        )
                        
                        # Try to parse individual field definitions
                        offset = 10  # Start after header
                        for i in range(limited_field_count):
                            try:
                                # Try to parse the field definition
                                field_def = TABLE_DEFINITION_FIELD_STRUCT.parse(combined_bytes[offset:])
                                
                                # Use actual field name if available, otherwise use parsed name
                                if i < len(actual_fields) and actual_fields[i] and len(actual_fields[i].strip()) > 0:
                                    field_name = actual_fields[i]
                                else:
                                    field_name = field_def.name if field_def.name else f'FIELD_{i+1}'
                                
                                # Sanitize field name for SQL
                                sanitized_name = sanitize_func(field_name)
                                
                                # Ensure unique field names by appending index if needed
                                if sanitized_name == "FIELD_UNKNOWN" or len(sanitized_name.strip()) == 0:
                                    sanitized_name = f"FIELD_{i+1}"
                                
                                # Debug: log field name extraction with array info
                                array_info = f" (ARRAY: {field_def.array_element_count} elements)" if field_def.array_element_count > 1 else ""
                                print(f"Field {i}: '{field_name}' -> '{sanitized_name}' (type: {field_def.type}, size: {field_def.size}, offset: {field_def.offset}){array_info}")
                                
                                field = type('Field', (), {
                                    'name': sanitized_name,
                                    'type': str(field_def.type),
                                    'size': field_def.size,
                                    'offset': field_def.offset,
                                    'array_element_count': field_def.array_element_count,
                                    'array_element_size': getattr(field_def, 'array_element_size', None),
                                    'is_enhanced_field': True  # Mark as enhanced field to prevent grouping
                                })()
                                self.fields.append(field)
                                
                                # Move to next field (approximate)
                                offset += 32  # Typical field definition size
                                
                            except Exception as e:
                                # If parsing fails, fall back to estimated values
                                if i < len(actual_fields) and actual_fields[i] and len(actual_fields[i].strip()) > 0:
                                    field_name = actual_fields[i]
                                else:
                                    field_name = f'FIELD_{i+1}'
                                
                                # Sanitize field name for SQL
                                sanitized_name = sanitize_func(field_name)
                                
                                # Ensure unique field names by appending index if needed
                                if sanitized_name == "FIELD_UNKNOWN" or len(sanitized_name.strip()) == 0:
                                    sanitized_name = f"FIELD_{i+1}"
                                
                                # Debug: log field name extraction
                                print(f"Field {i}: '{field_name}' -> '{sanitized_name}' (fallback: DOUBLE, size: 8, offset: {i * 8})")
                                
                                field = type('Field', (), {
                                    'name': sanitized_name,
                                    'type': 'DOUBLE',  # Fallback to DOUBLE
                                    'size': 8,         # Fallback size
                                    'offset': i * 8,   # Estimated offset
                                    'array_element_count': 1,
                                    'array_element_size': 8,
                                    'is_enhanced_field': True
                                })()
                                self.fields.append(field)
                                
                                offset += 32  # Move to next field
                                
                    except Exception as e:
                        # If all parsing fails, fall back to the old method
                        print(f"Failed to parse field definitions, using fallback method: {e}")
                        print(f"Creating {limited_field_count} fields using fallback method")
                        for i in range(limited_field_count):
                            # Use actual field name if available, otherwise use generic name
                            if i < len(actual_fields) and actual_fields[i] and len(actual_fields[i].strip()) > 0:
                                field_name = actual_fields[i]
                            else:
                                field_name = f'FIELD_{i+1}'
                            
                            # Sanitize field name for SQL
                            sanitized_name = sanitize_func(field_name)
                            
                            # Ensure unique field names by appending index if needed
                            if sanitized_name == "FIELD_UNKNOWN" or len(sanitized_name.strip()) == 0:
                                sanitized_name = f"FIELD_{i+1}"
                            
                            # Debug: log field name extraction
                            print(f"Field {i}: '{field_name}' -> '{sanitized_name}' (fallback: DOUBLE, size: 8, offset: {i * 8})")
                            
                            field = type('Field', (), {
                                'name': sanitized_name,
                                'type': 'DOUBLE',  # Default to DOUBLE for numeric fields
                                'size': 8,         # Default size for DOUBLE
                                'offset': i * 8,   # Estimate offset
                                'array_element_count': 1,  # Not an array - want individual columns
                                'array_element_size': 8,   # Element size
                                'is_enhanced_field': True  # Mark as enhanced field to prevent grouping
                            })()
                            self.fields.append(field)
                    
                    # The schema mapper will handle creating individual columns
                    # No need for special array handling here
                    
                    # Ensure we have the correct number of fields
                    if len(self.fields) < field_count:
                        print(f"DEBUG: Only created {len(self.fields)} fields, need {field_count}. Creating additional fields.")
                        for i in range(len(self.fields), field_count):
                            field_name = f'FIELD_{i+1}'
                            sanitized_name = sanitize_func(field_name)
                            
                            field = type('Field', (), {
                                'name': sanitized_name,
                                'type': 'DOUBLE',
                                'size': 8,
                                'offset': i * 8,
                                'array_element_count': 1,
                                'array_element_size': 8,
                                'is_enhanced_field': True
                            })()
                            self.fields.append(field)
                            print(f"DEBUG: Created additional field {i}: {sanitized_name}")
                    
                    print(f"DEBUG: Final field count: {len(self.fields)}")
                    
                    # If we still don't have enough fields, create them all from scratch
                    if len(self.fields) < field_count:
                        print(f"DEBUG: Still need more fields. Creating all {field_count} fields from scratch.")
                        self.fields = []  # Clear existing fields
                        for i in range(field_count):
                            field_name = f'FIELD_{i+1}'
                            sanitized_name = sanitize_func(field_name)
                            
                            field = type('Field', (), {
                                'name': sanitized_name,
                                'type': 'DOUBLE',
                                'size': 8,
                                'offset': i * 8,
                                'array_element_count': 1,
                                'array_element_size': 8,
                                'is_enhanced_field': True
                            })()
                            self.fields.append(field)
                            print(f"DEBUG: Created field {i}: {sanitized_name}")
                    
                    print(f"DEBUG: Final field count after fallback: {len(self.fields)}")
            
            return EnhancedTableDef(table_name, field_count, memo_count, index_count, record_size, actual_fields, self._sanitize_field_name_for_sql)
            
        except Exception as e:
            self.logger.warning(f"Enhanced table definition creation failed: {e}")
            return None
    
    def _create_multidimensional_table_definition(self, table_name, definition_bytes):
        """
        Create an enhanced table definition for multidimensional tables with multiple definition portions
        
        Args:
            table_name: Name of the table
            definition_bytes: Raw definition bytes from the table (dict of portions)
            
        Returns:
            Enhanced table definition or None if analysis fails
        """
        try:
            print(f"DEBUG: Starting multidimensional table definition for {table_name}")
            print(f"DEBUG: Number of portions: {len(definition_bytes)}")
            
            # For now, use the enhanced table definition approach but with better field extraction
            # This is a simplified version that should work
            combined_bytes = b''
            for key in sorted(definition_bytes.keys()):
                combined_bytes += definition_bytes[key]
            
            # Try to extract basic information from the header
            if len(combined_bytes) < 10:
                return None
            
            # Parse the header (first 10 bytes)
            import struct
            min_version_driver = struct.unpack('<H', combined_bytes[0:2])[0]
            record_size = struct.unpack('<H', combined_bytes[2:4])[0]
            field_count = struct.unpack('<H', combined_bytes[4:6])[0]
            memo_count = struct.unpack('<H', combined_bytes[6:8])[0]
            index_count = struct.unpack('<H', combined_bytes[8:10])[0]
            
            print(f"DEBUG: Multidimensional parsing for {table_name}: {field_count} fields, {memo_count} memos, {index_count} indexes, record_size={record_size}")
            
            # Try to extract actual field names from the raw definition bytes
            actual_fields = self._extract_field_names_from_bytes(combined_bytes, field_count)
            
            # Create enhanced table definition
            class EnhancedTableDef:
                def __init__(self, name, field_names, sanitize_func):
                    self.name = name
                    self.fields = []
                    self.memo_count = memo_count
                    self.index_count = index_count
                    self.record_size = record_size
                    
                    for i, field_name in enumerate(field_names):
                        # Sanitize field name for SQL
                        sanitized_name = sanitize_func(field_name)
                        
                        # Ensure unique field names
                        if sanitized_name == "FIELD_UNKNOWN" or len(sanitized_name.strip()) == 0:
                            sanitized_name = f"FIELD_{i+1}"
                        
                        field = type('Field', (), {
                            'name': sanitized_name,
                            'type': 'DOUBLE',  # Default type for now
                            'size': 8,         # Default size
                            'offset': i * 8,   # Estimated offset
                            'array_element_count': 1,
                            'array_element_size': None,
                            'is_enhanced_field': True  # Mark as enhanced field to prevent grouping
                        })()
                        self.fields.append(field)
            
            return EnhancedTableDef(table_name, actual_fields, self._sanitize_field_name_for_sql)
            
        except Exception as e:
            print(f"DEBUG: Multidimensional table definition creation failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _extract_field_names_using_pytopspeed_structure(self, definition_bytes, field_count):
        """
        Try to extract field names using the same structure that pytopspeed uses
        """
        field_names = []
        
        try:
            # Import the same structures that pytopspeed uses
            from construct import Struct, Int16ul, CString, If, Byte, Enum, BitsInteger
            
            # Field type structure (from pytopspeed)
            FIELD_TYPE_STRUCT = Enum(Byte,
                BYTE=1, SHORT=2, DATE=3, TIME=4, LONG=5, STRING=6, DECIMAL=7, MEMO=8, BLOB=9,
                CSTRING=10, PSTRING=11, PICTURE=12, DOUBLE=9, _default_='STRING'
            )
            
            # Table definition field structure (from pytopspeed)
            TABLE_DEFINITION_FIELD_STRUCT = Struct(
                "type" / FIELD_TYPE_STRUCT,
                "offset" / Int16ul,
                "name" / CString("ascii"),
                "array_element_count" / Int16ul,
                "size" / Int16ul,
                "overlaps" / Int16ul,
                "number" / Int16ul,
                "array_element_size" / If(lambda x: x['type'] in ['STRING', 'CSTRING', 'PSTRING', 'PICTURE'], Int16ul),
                "template" / If(lambda x: x['type'] in ['STRING', 'CSTRING', 'PSTRING', 'PICTURE'], Int16ul),
                "decimal_count" / If(lambda x: x['type'] == 'DECIMAL', Byte),
                "decimal_size" / If(lambda x: x['type'] == 'DECIMAL', Byte),
            )
            
            # Try to parse fields one by one with more flexible approach
            offset = 0
            for i in range(field_count):
                field_name = None
                
                # Strategy 1: Try full structure parsing
                try:
                    field_def = TABLE_DEFINITION_FIELD_STRUCT.parse(definition_bytes[offset:])
                    field_name = field_def.name
                    offset += 32  # Typical field definition size
                except Exception:
                    # Strategy 2: Try to find field name manually with multiple approaches
                    try:
                        # Look for null-terminated string after the type and offset
                        name_start = offset + 3  # Skip type (1 byte) and offset (2 bytes)
                        name_end = name_start
                        while name_end < len(definition_bytes) and definition_bytes[name_end] != 0:
                            name_end += 1
                        
                        if name_end > name_start:
                            field_name = definition_bytes[name_start:name_end].decode('ascii', errors='ignore')
                            if not field_name or not field_name.isprintable():
                                field_name = None
                        
                        # If that didn't work, try different offsets
                        if not field_name:
                            for test_offset in [offset + 1, offset + 2, offset + 4, offset + 5]:
                                if test_offset < len(definition_bytes):
                                    test_end = test_offset
                                    while test_end < len(definition_bytes) and definition_bytes[test_end] != 0:
                                        test_end += 1
                                    
                                    if test_end > test_offset:
                                        test_name = definition_bytes[test_offset:test_end].decode('ascii', errors='ignore')
                                        if test_name and test_name.isprintable() and len(test_name) > 2:
                                            field_name = test_name
                                            break
                        
                        offset += 32  # Move to next field
                    except:
                        offset += 32
                
                field_names.append(field_name)
                        
        except Exception as e:
            self.logger.warning(f"Pytopspeed structure field name extraction failed: {e}")
            field_names = [None] * field_count
        
        return field_names
    
    def _extract_field_names_resilient_parser(self, definition_bytes, field_count):
        """
        Build a more resilient parser that can handle various byte structures and patterns
        """
        field_names = []
        
        try:
            # Strategy 1: Try to find all possible field names using multiple approaches
            all_candidates = []
            
            # Approach 1: Scan for null-terminated strings with various starting positions
            for start_offset in range(0, min(500, len(definition_bytes)), 1):
                for step_size in [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40]:
                    candidates = []
                    offset = start_offset
                    
                    for i in range(field_count):
                        if offset >= len(definition_bytes):
                            break
                            
                        # Try to find a string at this position
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1 or null_pos <= offset:
                            break
                        
                        try:
                            string_data = definition_bytes[offset:null_pos]
                            if len(string_data) >= 2 and len(string_data) <= 50:
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if (field_name and field_name.isprintable() and 
                                        not field_name.isdigit() and
                                        not field_name.lower() in ['null', 'none', 'empty', 'field', 'column'] and
                                        len(field_name) >= 2):
                                        candidates.append(field_name)
                                    else:
                                        candidates.append(None)
                                else:
                                    candidates.append(None)
                            else:
                                candidates.append(None)
                        except:
                            candidates.append(None)
                        
                        offset += step_size
                    
                    # If we got a good number of valid strings, add this approach
                    valid_count = len([c for c in candidates if c is not None])
                    if valid_count >= field_count * 0.3:  # At least 30% valid
                        all_candidates.append((candidates, valid_count, f"offset_{start_offset}_step_{step_size}"))
                    
                    # Don't try too many combinations
                    if len(all_candidates) > 20:
                        break
                
                if len(all_candidates) > 20:
                    break
            
            # Approach 2: Look for field names with common patterns
            pattern_candidates = []
            for i in range(len(definition_bytes) - 10):
                # Look for strings that start with common prefixes
                prefixes = [b'FOR:', b'GRF:', b'FLU:', b'DAT:', b'ACT:', b'COM:', b'ODPV:', b'OCAN:', b'OMSG:', b'OTPL:', b'OSCE:', b'OMVR:', b'OTID:', b'OTPS:', b'OTPF:', b'OSUF:', b'VER:', b'OCURR:', b'OCRT:', b'ODPC:']
                for prefix in prefixes:
                    if definition_bytes[i:i+len(prefix)] == prefix:
                        # Found a prefix, extract the full field name
                        end_pos = i + len(prefix)
                        while end_pos < len(definition_bytes) and definition_bytes[end_pos] != 0:
                            end_pos += 1
                        
                        if end_pos > i + len(prefix):
                            string_data = definition_bytes[i:end_pos]
                            if all(32 <= b <= 126 for b in string_data):
                                field_name = string_data.decode('ascii', errors='ignore')
                                if field_name and field_name.isprintable():
                                    pattern_candidates.append(field_name)
            
            # Approach 3: Look for strings that look like field names (contain underscores, colons, etc.)
            field_like_candidates = []
            offset = 0
            while offset < len(definition_bytes):
                null_pos = definition_bytes.find(0, offset)
                if null_pos == -1:
                    break
                
                if null_pos > offset:
                    try:
                        string_data = definition_bytes[offset:null_pos]
                        if len(string_data) >= 3 and len(string_data) <= 50:
                            if all(32 <= b <= 126 for b in string_data):
                                field_name = string_data.decode('ascii', errors='ignore')
                                if (field_name and field_name.isprintable() and 
                                    (':' in field_name or '_' in field_name or 
                                     field_name.isupper() or 
                                     any(c.isdigit() for c in field_name)) and
                                    not field_name.isdigit() and
                                    not field_name.lower() in ['null', 'none', 'empty', 'field', 'column']):
                                    field_like_candidates.append(field_name)
                    except:
                        pass
                
                offset = null_pos + 1
            
            # Approach 4: Try to find field names in structured positions with different alignments
            structured_candidates = []
            for alignment in [1, 2, 4, 8, 12, 16, 20, 24, 28, 32]:
                for start_pos in range(0, min(200, len(definition_bytes)), alignment):
                    candidates = []
                    offset = start_pos
                    
                    for i in range(field_count):
                        if offset >= len(definition_bytes):
                            break
                            
                        # Try to find a string at this position
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1 or null_pos <= offset:
                            break
                        
                        try:
                            string_data = definition_bytes[offset:null_pos]
                            if len(string_data) >= 2 and len(string_data) <= 50:
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if (field_name and field_name.isprintable() and 
                                        not field_name.isdigit() and
                                        not field_name.lower() in ['null', 'none', 'empty', 'field', 'column'] and
                                        len(field_name) >= 2):
                                        candidates.append(field_name)
                                    else:
                                        candidates.append(None)
                                else:
                                    candidates.append(None)
                            else:
                                candidates.append(None)
                        except:
                            candidates.append(None)
                        
                        offset += alignment
                    
                    # If we got a good number of valid strings, add this approach
                    valid_count = len([c for c in candidates if c is not None])
                    if valid_count >= field_count * 0.4:  # At least 40% valid
                        structured_candidates.append((candidates, valid_count, f"alignment_{alignment}_start_{start_pos}"))
                    
                    # Don't try too many combinations
                    if len(structured_candidates) > 15:
                        break
                
                if len(structured_candidates) > 15:
                    break
            
            # Combine all approaches and find the best result
            all_approaches = []
            
            # Add structured candidates (highest priority)
            all_approaches.extend(structured_candidates)
            
            # Add pattern candidates
            if pattern_candidates:
                all_approaches.append((pattern_candidates[:field_count], len(pattern_candidates), "pattern_based"))
            
            # Add field-like candidates
            if field_like_candidates:
                all_approaches.append((field_like_candidates[:field_count], len(field_like_candidates), "field_like"))
            
            # Add other candidates
            all_approaches.extend(all_candidates)
            
            # Sort by validity and select the best approach
            all_approaches.sort(key=lambda x: x[1], reverse=True)
            
            if all_approaches:
                best_approach = all_approaches[0]
                field_names = best_approach[0]
                self.logger.info(f"Resilient parser using approach '{best_approach[2]}' with {best_approach[1]} valid field names")
                
                # If we still don't have 100% field names, try to fill gaps
                if best_approach[1] < field_count:
                    self.logger.info(f"Only {best_approach[1]}/{field_count} field names extracted, attempting to fill gaps")
                    
                    # Try to fill gaps with names from other approaches
                    combined_names = [None] * field_count
                    
                    # Start with the best approach
                    for i, name in enumerate(best_approach[0]):
                        if i < field_count:
                            combined_names[i] = name
                    
                    # Fill gaps with names from other approaches
                    for approach in all_approaches[1:]:
                        if approach[0]:
                            for i, name in enumerate(approach[0]):
                                if i < field_count and combined_names[i] is None and name and name.strip():
                                    combined_names[i] = name
                    
                    # Update field_names with the combined result
                    field_names = combined_names
                    final_count = len([name for name in field_names if name and name.strip()])
                    self.logger.info(f"After gap filling: {final_count}/{field_count} field names extracted")
            else:
                field_names = [None] * field_count
                self.logger.warning("Resilient parser found no valid field names")
                
        except Exception as e:
            self.logger.warning(f"Resilient parser failed: {e}")
            field_names = [None] * field_count
        
        return field_names
    
    def _extract_field_names_from_bytes(self, definition_bytes, field_count):
        """
        Attempt to extract actual field names from raw table definition bytes
        
        Args:
            definition_bytes: Raw table definition bytes
            field_count: Number of fields expected
            
        Returns:
            List of field names (may be empty or partial if parsing fails)
        """
        field_names = []
        
        try:
            # Method 1: Try the new resilient parser (most comprehensive)
            resilient_names = self._extract_field_names_resilient_parser(definition_bytes, field_count)
            
            # Method 2: Try to use pytopspeed structure (most accurate for standard cases)
            pytopspeed_names = self._extract_field_names_using_pytopspeed_structure(definition_bytes, field_count)
            
            # Method 3: Find all null-terminated strings in the definition
            all_strings = self._find_all_strings_in_bytes(definition_bytes)
            
            # Method 4: Try to parse field definitions with known TopSpeed structure
            structured_names = self._parse_structured_field_names(definition_bytes, field_count)
            
            # Method 5: Look for common field name patterns
            pattern_names = self._find_field_name_patterns(definition_bytes, field_count)
            
            # Method 6: Advanced parsing with multiple strategies
            advanced_names = self._parse_advanced_field_names(definition_bytes, field_count)
            
            # Method 7: Try different field definition structures
            alternative_names = self._parse_alternative_structures(definition_bytes, field_count)
            
            # Method 8: Comprehensive byte scanning for all possible field names
            comprehensive_names = self._comprehensive_field_name_scan(definition_bytes, field_count)
            
            # Combine all results and find the best match
            all_methods = [
                ("resilient", resilient_names),
                ("pytopspeed", pytopspeed_names),
                ("comprehensive", comprehensive_names),
                ("structured", structured_names),
                ("advanced", advanced_names),
                ("alternative", alternative_names),
                ("pattern", pattern_names),
                ("all_strings", all_strings[:field_count])
            ]
            
            # Find the method that gives us the most field names
            best_method = None
            best_count = 0
            best_method_name = None
            for method_name, names in all_methods:
                valid_names = [n for n in names if n is not None]
                if len(valid_names) > best_count:
                    best_count = len(valid_names)
                    best_method = names
                    best_method_name = method_name
            
            if best_method:
                field_names = best_method
                print(f"Using method '{best_method_name}' with {best_count} valid field names")
                
                # If we still don't have 100% field names, try to fill in the gaps
                if best_count < field_count:
                    print(f"Only {best_count}/{field_count} field names extracted, attempting to fill gaps")
                    
                    # Try to find missing field names by combining results from all methods
                    combined_names = [None] * field_count
                    
                    # Start with the best method
                    for i, name in enumerate(best_method):
                        if i < field_count:
                            combined_names[i] = name
                    
                    # Fill gaps with names from other methods
                    for method_name, method_result in all_methods:
                        if method_result and method_name != best_method_name:
                            for i, name in enumerate(method_result):
                                if i < field_count and combined_names[i] is None and name and name.strip():
                                    combined_names[i] = name
                    
                    # Update field_names with the combined result
                    field_names = combined_names
                    final_count = len([name for name in field_names if name and name.strip()])
                    print(f"After gap filling: {final_count}/{field_count} field names extracted")
            else:
                field_names = all_strings[:field_count]
                print(f"Using fallback method with {len(field_names)} field names")
            
            # Debug: print extracted field names
            print(f"Extracted field names: {field_names[:10]}...")  # Show first 10
            
            # Pad with None if we don't have enough names
            while len(field_names) < field_count:
                field_names.append(None)
            
            self.logger.info(f"Extracted {len([n for n in field_names if n])} field names from raw definition")
            
        except Exception as e:
            self.logger.warning(f"Failed to extract field names from raw definition: {e}")
        
        return field_names
    
    def _find_all_strings_in_bytes(self, definition_bytes):
        """Find all null-terminated strings in the definition bytes"""
        strings = []
        offset = 0
        
        while offset < len(definition_bytes):
            # Find next null terminator
            null_pos = definition_bytes.find(0, offset)
            if null_pos == -1:
                break
            
            # Extract string before null
            if null_pos > offset:
                try:
                    string_data = definition_bytes[offset:null_pos]
                    if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                        field_name = string_data.decode('ascii', errors='ignore')
                        if field_name and len(field_name) > 0:
                            strings.append(field_name)
                except:
                    pass
            
            offset = null_pos + 1
        
        return strings
    
    def _parse_structured_field_names(self, definition_bytes, field_count):
        """Try to parse field names using TopSpeed table definition structure"""
        field_names = []
        
        try:
            # Skip header (first 10 bytes)
            offset = 10
            
            # TopSpeed field definition structure (approximate):
            # Each field definition is typically 16-32 bytes
            # Field name is usually at the beginning, null-terminated
            # Followed by type, size, offset, and other metadata
            
            for i in range(field_count):
                if offset >= len(definition_bytes):
                    break
                
                # Look for field name (null-terminated string)
                name_start = offset
                name_end = offset
                
                # Find null terminator
                while name_end < len(definition_bytes) and definition_bytes[name_end] != 0:
                    name_end += 1
                
                if name_end > name_start and name_end < len(definition_bytes):
                    try:
                        field_name = definition_bytes[name_start:name_end].decode('ascii', errors='ignore')
                        if field_name and field_name.isprintable() and len(field_name) > 0:
                            field_names.append(field_name)
                            # Skip to next field definition (estimate 16-32 bytes per field)
                            offset = name_end + 1
                            # Align to next field boundary
                            while offset < len(definition_bytes) and offset % 16 != 0:
                                offset += 1
                        else:
                            field_names.append(None)
                            offset += 16  # Skip estimated field size
                    except:
                        field_names.append(None)
                        offset += 16
                else:
                    field_names.append(None)
                    offset += 16
                    
        except Exception as e:
            self.logger.warning(f"Structured field name parsing failed: {e}")
        
        return field_names
    
    def _find_field_name_patterns(self, definition_bytes, field_count):
        """Look for common field name patterns in the definition bytes"""
        field_names = []
        
        try:
            # Look for common field name patterns
            common_patterns = [
                b'LSE_ID', b'ARCSEQ', b'PRODUCTCODE', b'FIELD_', b'PROD', b'PRE',
                b'ID', b'NAME', b'TYPE', b'SIZE', b'VALUE', b'DATA', b'INFO'
            ]
            
            found_names = []
            for pattern in common_patterns:
                pos = 0
                while True:
                    pos = definition_bytes.find(pattern, pos)
                    if pos == -1:
                        break
                    
                    # Try to extract the full field name
                    name_start = pos
                    name_end = pos
                    
                    # Look backwards for start of field name
                    while name_start > 0 and definition_bytes[name_start - 1] != 0:
                        name_start -= 1
                    
                    # Look forwards for end of field name
                    while name_end < len(definition_bytes) and definition_bytes[name_end] != 0:
                        name_end += 1
                    
                    if name_end > name_start:
                        try:
                            field_name = definition_bytes[name_start:name_end].decode('ascii', errors='ignore')
                            if field_name and field_name.isprintable():
                                found_names.append(field_name)
                        except:
                            pass
                    
                    pos += len(pattern)
            
            # Remove duplicates and limit to field_count
            unique_names = list(dict.fromkeys(found_names))  # Preserve order, remove duplicates
            field_names = unique_names[:field_count]
            
            # Pad with None if needed
            while len(field_names) < field_count:
                field_names.append(None)
                
        except Exception as e:
            self.logger.warning(f"Pattern-based field name extraction failed: {e}")
        
        return field_names
    
    def _parse_advanced_field_names(self, definition_bytes, field_count):
        """Advanced parsing with multiple strategies to extract field names"""
        field_names = []
        
        try:
            # Strategy 1: Look for field names with different encodings
            encoding_names = self._parse_with_different_encodings(definition_bytes, field_count)
            
            # Strategy 2: Look for field names with different string terminators
            terminator_names = self._parse_with_different_terminators(definition_bytes, field_count)
            
            # Strategy 3: Look for field names with length prefixes
            length_prefix_names = self._parse_with_length_prefixes(definition_bytes, field_count)
            
            # Strategy 4: Look for field names in different byte positions
            position_names = self._parse_with_different_positions(definition_bytes, field_count)
            
            # Combine all strategies and find the best result
            all_strategies = [encoding_names, terminator_names, length_prefix_names, position_names]
            best_strategy = None
            best_count = 0
            
            for strategy in all_strategies:
                valid_names = [n for n in strategy if n is not None]
                if len(valid_names) > best_count:
                    best_count = len(valid_names)
                    best_strategy = strategy
            
            if best_strategy:
                field_names = best_strategy
            else:
                field_names = [None] * field_count
                
        except Exception as e:
            self.logger.warning(f"Advanced field name parsing failed: {e}")
            field_names = [None] * field_count
        
        return field_names
    
    def _parse_alternative_structures(self, definition_bytes, field_count):
        """Try different field definition structures"""
        field_names = []
        
        try:
            # Structure 1: Field names stored as fixed-length strings
            fixed_length_names = self._parse_fixed_length_strings(definition_bytes, field_count)
            
            # Structure 2: Field names stored with different alignment
            aligned_names = self._parse_aligned_strings(definition_bytes, field_count)
            
            # Structure 3: Field names stored in reverse order
            reverse_names = self._parse_reverse_order(definition_bytes, field_count)
            
            # Structure 4: Field names stored with different byte orders
            byte_order_names = self._parse_different_byte_orders(definition_bytes, field_count)
            
            # Combine all structures and find the best result
            all_structures = [fixed_length_names, aligned_names, reverse_names, byte_order_names]
            best_structure = None
            best_count = 0
            
            for structure in all_structures:
                valid_names = [n for n in structure if n is not None]
                if len(valid_names) > best_count:
                    best_count = len(valid_names)
                    best_structure = structure
            
            if best_structure:
                field_names = best_structure
            else:
                field_names = [None] * field_count
                
        except Exception as e:
            self.logger.warning(f"Alternative structure parsing failed: {e}")
            field_names = [None] * field_count
        
        return field_names
    
    def _parse_with_different_encodings(self, definition_bytes, field_count):
        """Try parsing with different character encodings"""
        field_names = []
        
        try:
            # Try different encodings
            encodings = ['ascii', 'latin1', 'cp1252', 'utf-8', 'utf-16', 'utf-16le', 'utf-16be']
            
            for encoding in encodings:
                try:
                    # Find all null-terminated strings with this encoding
                    strings = []
                    offset = 0
                    
                    while offset < len(definition_bytes):
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1:
                            break
                        
                        if null_pos > offset:
                            try:
                                string_data = definition_bytes[offset:null_pos]
                                if len(string_data) > 0:
                                    field_name = string_data.decode(encoding, errors='ignore')
                                    if field_name and field_name.isprintable() and len(field_name) > 0:
                                        strings.append(field_name)
                            except:
                                pass
                        
                        offset = null_pos + 1
                    
                    if len(strings) >= field_count * 0.5:  # If we got at least 50% of fields
                        field_names = strings[:field_count]
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Different encoding parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_with_different_terminators(self, definition_bytes, field_count):
        """Try parsing with different string terminators"""
        field_names = []
        
        try:
            # Try different terminators
            terminators = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 32, 255]
            
            for terminator in terminators:
                try:
                    strings = []
                    offset = 0
                    
                    while offset < len(definition_bytes):
                        term_pos = definition_bytes.find(terminator, offset)
                        if term_pos == -1:
                            break
                        
                        if term_pos > offset:
                            try:
                                string_data = definition_bytes[offset:term_pos]
                                if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable() and len(field_name) > 0:
                                        strings.append(field_name)
                            except:
                                pass
                        
                        offset = term_pos + 1
                    
                    if len(strings) >= field_count * 0.5:  # If we got at least 50% of fields
                        field_names = strings[:field_count]
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Different terminator parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_with_length_prefixes(self, definition_bytes, field_count):
        """Try parsing field names with length prefixes"""
        import struct
        field_names = []
        
        try:
            # Look for strings with length prefixes (1 or 2 bytes)
            offset = 10  # Skip header
            
            for i in range(field_count):
                if offset >= len(definition_bytes):
                    break
                
                # Try 1-byte length prefix
                if offset + 1 < len(definition_bytes):
                    length = definition_bytes[offset]
                    if 1 <= length <= 50:  # Reasonable field name length
                        if offset + 1 + length < len(definition_bytes):
                            try:
                                string_data = definition_bytes[offset + 1:offset + 1 + length]
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable():
                                        field_names.append(field_name)
                                        offset += 1 + length
                                        continue
                            except:
                                pass
                
                # Try 2-byte length prefix
                if offset + 2 < len(definition_bytes):
                    length = struct.unpack('<H', definition_bytes[offset:offset + 2])[0]
                    if 1 <= length <= 50:  # Reasonable field name length
                        if offset + 2 + length < len(definition_bytes):
                            try:
                                string_data = definition_bytes[offset + 2:offset + 2 + length]
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable():
                                        field_names.append(field_name)
                                        offset += 2 + length
                                        continue
                            except:
                                pass
                
                # Fallback: skip to next potential field
                field_names.append(None)
                offset += 16  # Skip estimated field size
                
        except Exception as e:
            self.logger.warning(f"Length prefix parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_with_different_positions(self, definition_bytes, field_count):
        """Try parsing field names from different byte positions"""
        field_names = []
        
        try:
            # Try different starting positions
            start_positions = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
            
            for start_pos in start_positions:
                if start_pos >= len(definition_bytes):
                    break
                    
                try:
                    strings = []
                    offset = start_pos
                    
                    while offset < len(definition_bytes) and len(strings) < field_count:
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1:
                            break
                        
                        if null_pos > offset:
                            try:
                                string_data = definition_bytes[offset:null_pos]
                                if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable() and len(field_name) > 0:
                                        strings.append(field_name)
                            except:
                                pass
                        
                        offset = null_pos + 1
                    
                    if len(strings) >= field_count * 0.5:  # If we got at least 50% of fields
                        field_names = strings[:field_count]
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Different position parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_fixed_length_strings(self, definition_bytes, field_count):
        """Try parsing field names as fixed-length strings"""
        field_names = []
        
        try:
            # Try different fixed lengths
            fixed_lengths = [8, 10, 12, 16, 20, 24, 32, 40, 48, 64]
            
            for length in fixed_lengths:
                try:
                    strings = []
                    offset = 10  # Skip header
                    
                    for i in range(field_count):
                        if offset + length > len(definition_bytes):
                            break
                        
                        try:
                            string_data = definition_bytes[offset:offset + length]
                            # Find null terminator within the fixed length
                            null_pos = string_data.find(0)
                            if null_pos != -1:
                                string_data = string_data[:null_pos]
                            
                            if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                                field_name = string_data.decode('ascii', errors='ignore')
                                if field_name and field_name.isprintable():
                                    strings.append(field_name)
                                else:
                                    strings.append(None)
                            else:
                                strings.append(None)
                        except:
                            strings.append(None)
                        
                        offset += length
                    
                    if len([s for s in strings if s is not None]) >= field_count * 0.5:
                        field_names = strings
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Fixed length string parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_aligned_strings(self, definition_bytes, field_count):
        """Try parsing field names with different alignment"""
        field_names = []
        
        try:
            # Try different alignments
            alignments = [1, 2, 4, 8, 16, 32]
            
            for alignment in alignments:
                try:
                    strings = []
                    offset = 10  # Skip header
                    
                    # Align to the specified boundary
                    offset = ((offset + alignment - 1) // alignment) * alignment
                    
                    for i in range(field_count):
                        if offset >= len(definition_bytes):
                            break
                        
                        # Look for null-terminated string
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1:
                            break
                        
                        if null_pos > offset:
                            try:
                                string_data = definition_bytes[offset:null_pos]
                                if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable():
                                        strings.append(field_name)
                                    else:
                                        strings.append(None)
                                else:
                                    strings.append(None)
                            except:
                                strings.append(None)
                        else:
                            strings.append(None)
                        
                        # Move to next aligned position
                        offset = ((null_pos + alignment) // alignment) * alignment
                    
                    if len([s for s in strings if s is not None]) >= field_count * 0.5:
                        field_names = strings
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Aligned string parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_reverse_order(self, definition_bytes, field_count):
        """Try parsing field names in reverse order"""
        field_names = []
        
        try:
            # Reverse the bytes and try parsing
            reversed_bytes = definition_bytes[::-1]
            
            strings = []
            offset = 0
            
            while offset < len(reversed_bytes):
                null_pos = reversed_bytes.find(0, offset)
                if null_pos == -1:
                    break
                
                if null_pos > offset:
                    try:
                        string_data = reversed_bytes[offset:null_pos]
                        if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                            field_name = string_data.decode('ascii', errors='ignore')
                            if field_name and field_name.isprintable():
                                strings.append(field_name)
                    except:
                        pass
                
                offset = null_pos + 1
            
            # Reverse the strings back to original order
            field_names = strings[::-1][:field_count]
            
        except Exception as e:
            self.logger.warning(f"Reverse order parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _parse_different_byte_orders(self, definition_bytes, field_count):
        """Try parsing with different byte orders"""
        field_names = []
        
        try:
            # Try different byte orders
            byte_orders = ['little', 'big']
            
            for byte_order in byte_orders:
                try:
                    strings = []
                    offset = 0
                    
                    while offset < len(definition_bytes):
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1:
                            break
                        
                        if null_pos > offset:
                            try:
                                string_data = definition_bytes[offset:null_pos]
                                if len(string_data) > 0 and all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable():
                                        strings.append(field_name)
                            except:
                                pass
                        
                        offset = null_pos + 1
                    
                    if len(strings) >= field_count * 0.5:
                        field_names = strings[:field_count]
                        break
                        
                except:
                    continue
                    
        except Exception as e:
            self.logger.warning(f"Different byte order parsing failed: {e}")
        
        # Pad with None if needed
        while len(field_names) < field_count:
            field_names.append(None)
            
        return field_names
    
    def _comprehensive_field_name_scan(self, definition_bytes, field_count):
        """
        Comprehensive scan of definition bytes to find ALL possible field names
        This method tries multiple strategies to extract field names from every possible location
        """
        field_names = []
        
        try:
            # Strategy 1: Look for longer, more meaningful field names first
            meaningful_strings = []
            offset = 0
            while offset < len(definition_bytes):
                null_pos = definition_bytes.find(0, offset)
                if null_pos == -1:
                    break
                
                if null_pos > offset:
                    try:
                        string_data = definition_bytes[offset:null_pos]
                        if len(string_data) >= 4 and len(string_data) <= 50:  # Prefer longer names
                            if all(32 <= b <= 126 for b in string_data):  # Printable ASCII
                                field_name = string_data.decode('ascii', errors='ignore')
                                if (field_name and field_name.isprintable() and 
                                    not field_name.isdigit() and
                                    not field_name.lower() in ['null', 'none', 'empty', 'field', 'column']):
                                    meaningful_strings.append(field_name)
                    except:
                        pass
                
                offset = null_pos + 1
            
            # Strategy 2: Look for field names with common patterns
            pattern_strings = []
            for offset in range(0, len(definition_bytes) - 10):
                try:
                    # Look for strings that start with common prefixes
                    prefixes = [b'FOR_', b'FIELD_', b'COL_', b'FLD_', b'F_', b'ARC', b'PROD', b'CUM', b'UNIT', b'DECI']
                    for prefix in prefixes:
                        if definition_bytes[offset:offset + len(prefix)] == prefix:
                            # Found a prefix, extract the full field name
                            end_pos = offset + len(prefix)
                            while end_pos < len(definition_bytes) and definition_bytes[end_pos] != 0:
                                end_pos += 1
                            
                            if end_pos > offset + len(prefix):
                                string_data = definition_bytes[offset:end_pos]
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if field_name and field_name.isprintable():
                                        pattern_strings.append(field_name)
                except:
                    pass
            
            # Strategy 3: Try to find field names in structured positions
            structured_strings = []
            # Try different starting positions and step sizes
            for start_pos in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
                if start_pos >= len(definition_bytes):
                    break
                    
                for step_size in [8, 12, 16, 20, 24, 32]:
                    strings = []
                    offset = start_pos
                    
                    for i in range(field_count):
                        if offset >= len(definition_bytes):
                            break
                            
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1 or null_pos <= offset:
                            break
                        
                        try:
                            string_data = definition_bytes[offset:null_pos]
                            if len(string_data) >= 3 and len(string_data) <= 50:
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if (field_name and field_name.isprintable() and 
                                        not field_name.isdigit() and
                                        not field_name.lower() in ['null', 'none', 'empty', 'field', 'column']):
                                        strings.append(field_name)
                                    else:
                                        strings.append(None)
                                else:
                                    strings.append(None)
                            else:
                                strings.append(None)
                        except:
                            strings.append(None)
                        
                        offset += step_size
                    
                    # If we got a good number of valid strings, use this approach
                    valid_count = len([s for s in strings if s is not None])
                    if valid_count >= field_count * 0.3:  # At least 30% valid
                        structured_strings = strings
                        break
                    
                if structured_strings:
                    break
            
            # Strategy 4: Look for field names with underscores (common in database field names)
            underscore_strings = []
            offset = 0
            while offset < len(definition_bytes):
                null_pos = definition_bytes.find(0, offset)
                if null_pos == -1:
                    break
                
                if null_pos > offset:
                    try:
                        string_data = definition_bytes[offset:null_pos]
                        if len(string_data) >= 4 and len(string_data) <= 50:
                            if all(32 <= b <= 126 for b in string_data):  # Printable ASCII
                                field_name = string_data.decode('ascii', errors='ignore')
                                if (field_name and field_name.isprintable() and 
                                    '_' in field_name and  # Contains underscore
                                    not field_name.isdigit() and
                                    not field_name.lower() in ['null', 'none', 'empty', 'field', 'column']):
                                    underscore_strings.append(field_name)
                    except:
                        pass
                
                offset = null_pos + 1
            
            # Strategy 5: Look for field names in different byte positions and structures
            # Try to find field names that might be stored in different formats
            alternative_strings = []
            
            # Try different starting positions and step sizes to find field names
            for start_offset in range(0, min(200, len(definition_bytes)), 4):
                for step_size in [8, 12, 16, 20, 24, 28, 32, 36, 40]:
                    strings = []
                    offset = start_offset
                    
                    for i in range(field_count):
                        if offset >= len(definition_bytes):
                            break
                            
                        # Try to find a string at this position
                        null_pos = definition_bytes.find(0, offset)
                        if null_pos == -1 or null_pos <= offset:
                            break
                        
                        try:
                            string_data = definition_bytes[offset:null_pos]
                            if len(string_data) >= 3 and len(string_data) <= 50:
                                if all(32 <= b <= 126 for b in string_data):
                                    field_name = string_data.decode('ascii', errors='ignore')
                                    if (field_name and field_name.isprintable() and 
                                        not field_name.isdigit() and
                                        not field_name.lower() in ['null', 'none', 'empty', 'field', 'column'] and
                                        len(field_name) >= 3):
                                        strings.append(field_name)
                                    else:
                                        strings.append(None)
                                else:
                                    strings.append(None)
                            else:
                                strings.append(None)
                        except:
                            strings.append(None)
                        
                        offset += step_size
                    
                    # If we got a good number of valid strings, use this approach
                    valid_count = len([s for s in strings if s is not None])
                    if valid_count >= field_count * 0.4:  # At least 40% valid
                        alternative_strings = strings
                        break
                    
                if alternative_strings:
                    break
            
            # Combine all strategies and prioritize longer, more meaningful names
            all_candidates = []
            
            # Add alternative strings first (highest priority - most comprehensive)
            all_candidates.extend(alternative_strings)
            
            # Add underscore strings (high priority)
            all_candidates.extend(underscore_strings)
            
            # Add pattern strings (high priority)
            all_candidates.extend(pattern_strings)
            
            # Add structured strings (medium priority)
            all_candidates.extend(structured_strings)
            
            # Add meaningful strings (lower priority)
            all_candidates.extend(meaningful_strings)
            
            # Remove duplicates while preserving order
            seen = set()
            unique_candidates = []
            for candidate in all_candidates:
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    unique_candidates.append(candidate)
            
            # Take the first field_count candidates
            field_names = unique_candidates[:field_count]
            
            # Pad with None if we don't have enough
            while len(field_names) < field_count:
                field_names.append(None)
                
        except Exception as e:
            self.logger.warning(f"Comprehensive field name scan failed: {e}")
            field_names = [None] * field_count
        
        return field_names
    
    def _sanitize_field_name_for_sql(self, field_name):
        """Sanitize field name for SQL column names"""
        if not field_name:
            return "FIELD_UNKNOWN"
        
        # Remove or replace invalid characters
        import re
        
        # Remove table prefixes (GRF:, FOR:, FLU:, etc.) and field order suffixes (_1, _2, etc.)
        sanitized = str(field_name)
        
        # Remove common table prefixes
        prefixes_to_remove = ['GRF:', 'FOR:', 'FLU:', 'DAT:', 'ACT:', 'COM:', 'ODPV:', 'OCAN:', 'OMSG:', 'OTPL:', 'OSCE:', 'OMVR:', 'OTID:', 'OTPS:', 'OTPF:', 'OSUF:', 'VER:', 'OCURR:', 'OCRT:', 'ODPC:']
        for prefix in prefixes_to_remove:
            if sanitized.startswith(prefix):
                sanitized = sanitized[len(prefix):]
                break
        
        # Remove field order suffixes like _1, _2, _3, etc.
        sanitized = re.sub(r'_\d+$', '', sanitized)
        
        # Also remove any remaining numeric suffixes that might be at the end
        sanitized = re.sub(r'_\d+$', '', sanitized)
        
        # Replace invalid characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', sanitized)
        
        # Ensure it starts with a letter or underscore
        if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
            sanitized = 'FIELD_' + sanitized
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = "FIELD_UNKNOWN"
        
        # Limit length to reasonable SQL column name length
        if len(sanitized) > 50:
            sanitized = sanitized[:50]
        
        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        
        # Ensure it's not empty after sanitization
        if not sanitized:
            sanitized = "FIELD_UNKNOWN"
        
        return sanitized
    
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
                        
                        # Create schema for this file with proper prefixing
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
