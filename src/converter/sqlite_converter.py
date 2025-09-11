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
            
            # Check if this is an enhanced table definition (large array table)
            # Use a lower threshold since FORCAST has 41 fields but is still a large array table
            if hasattr(table_def, 'field_count') and table_def.field_count is not None and table_def.field_count > 30:
                return self._migrate_large_array_table_data(tps, table_name, sanitized_table_name, conn, table_def)
            
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
                    # Analyze the raw bytes to understand the structure
                    enhanced_def = self._create_enhanced_table_definition(table_name, table.definition_bytes)
                    if enhanced_def:
                        self.logger.info(f"Created enhanced table definition for {table_name}")
                        return enhanced_def
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
            
            # Create enhanced table definition
            class EnhancedTableDef:
                def __init__(self, name, field_count, memo_count, index_count, record_size):
                    self.name = name
                    self.fields = []
                    self.memos = []
                    self.indexes = []
                    self.record_size = record_size
                    self.field_count = field_count
                    self.memo_count = memo_count
                    self.index_count = index_count
                    
                    # Create placeholder fields for large arrays
                    # This allows the table to be created with a reasonable structure
                    if field_count is not None and field_count > 30:  # Likely a large array table (lowered threshold for FORCAST)
                        # Create a single JSON field to hold all array data
                        class ArrayField:
                            def __init__(self, name, field_type, size):
                                self.name = name
                                self.type = field_type
                                self.size = size
                                self.offset = 0
                                self.array_element_count = 1
                                self.array_element_size = size
                        
                        # Add a single JSON field for the entire array
                        self.fields.append(ArrayField(f"{name}_ARRAY_DATA", "JSON", record_size))
                    else:
                        # For smaller tables, create individual fields
                        for i in range(min(field_count, 20)):  # Limit to 20 fields max
                            class SimpleField:
                                def __init__(self, name, field_type, size, offset):
                                    self.name = name
                                    self.type = field_type
                                    self.size = size
                                    self.offset = offset
                                    self.array_element_count = 1
                                    self.array_element_size = size
                            
                            field_size = max(8, record_size // field_count) if field_count > 0 else 8
                            self.fields.append(SimpleField(f"{name}_FIELD_{i+1}", "TEXT", field_size, i * field_size))
            
            return EnhancedTableDef(table_name, field_count, memo_count, index_count, record_size)
            
        except Exception as e:
            self.logger.warning(f"Enhanced table definition creation failed: {e}")
            return None
    
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
