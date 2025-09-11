"""
Schema Mapper for converting TopSpeed table definitions to SQLite CREATE TABLE statements
"""

import sqlite3
from typing import Dict, List, Optional, Tuple
from .multidimensional_handler import MultidimensionalHandler


class TopSpeedToSQLiteMapper:
    """Maps TopSpeed data types and table definitions to SQLite schema"""
    
    # TopSpeed to SQLite data type mapping
    TYPE_MAPPING = {
        'BYTE': 'INTEGER',      # 1-byte integer
        'SHORT': 'INTEGER',     # 2-byte integer  
        'USHORT': 'INTEGER',    # 2-byte unsigned integer
        'LONG': 'INTEGER',      # 4-byte integer
        'ULONG': 'INTEGER',     # 4-byte unsigned integer
        'FLOAT': 'REAL',        # 4-byte floating point
        'DOUBLE': 'REAL',       # 8-byte floating point
        'DECIMAL': 'REAL',      # Decimal number
        'STRING': 'TEXT',       # Fixed-length string
        'CSTRING': 'TEXT',      # Null-terminated string
        'PSTRING': 'TEXT',      # Pascal string (length-prefixed)
        'DATE': 'TEXT',         # Date (YYYYMMDD format)
        'TIME': 'TEXT',         # Time (HHMMSSHS format)
        'GROUP': 'BLOB',        # Compound data structure
        'MEMO': 'BLOB',         # Memo/BLOB data
        'BLOB': 'BLOB',         # Binary large object
    }
    
    def __init__(self):
        self.table_definitions = {}
        self.multidimensional_handler = MultidimensionalHandler()
    
    def map_field_type(self, topspeed_type: str, size: int) -> str:
        """
        Map TopSpeed field type to SQLite type
        
        Args:
            topspeed_type: TopSpeed data type
            size: Field size in bytes
            
        Returns:
            SQLite data type
        """
        base_type = self.TYPE_MAPPING.get(topspeed_type, 'BLOB')
        
        # Special handling for specific types
        if topspeed_type == 'STRING' and size > 0:
            # For fixed-length strings, we could add a length constraint
            # but SQLite TEXT can handle any length
            return 'TEXT'
        elif topspeed_type in ['BYTE', 'SHORT', 'USHORT', 'LONG', 'ULONG']:
            return 'INTEGER'
        elif topspeed_type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
            return 'REAL'
        elif topspeed_type in ['DATE', 'TIME']:
            return 'TEXT'  # Store as text, could be converted to proper date/time later
        else:
            return base_type
    
    def sanitize_field_name(self, name: str) -> str:
        """
        Sanitize field name for SQLite compatibility
        
        Args:
            name: Original field name
            
        Returns:
            Sanitized field name
        """
        # Remove table prefix (e.g., "TIT:PROJ_DESCR" -> "PROJ_DESCR")
        if ':' in name:
            sanitized = name.split(':', 1)[1]  # Take everything after the first colon
        else:
            sanitized = name
            
        # Replace problematic characters
        sanitized = sanitized.replace(' ', '_')  # Replace spaces with underscores
        sanitized = sanitized.replace('-', '_')  # Replace hyphens with underscores
        sanitized = sanitized.replace('.', '_')  # Replace dots with underscores
        sanitized = sanitized.replace('/', '_')  # Replace forward slashes with underscores
        sanitized = sanitized.replace('\\', '_')  # Replace backslashes with underscores
        
        # Ensure name starts with letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = '_' + sanitized
            
        # Limit length (SQLite doesn't have strict limits, but 64 chars is reasonable)
        if len(sanitized) > 64:
            sanitized = sanitized[:64]
            
        return sanitized
    
    def sanitize_table_name(self, name: str) -> str:
        """
        Sanitize table name for SQLite compatibility
        
        Args:
            name: Original table name
            
        Returns:
            Sanitized table name
        """
        # Convert to string if it's a Container object
        if hasattr(name, '__str__') and not isinstance(name, str):
            name = str(name)
        
        # Similar to field names but with additional considerations
        sanitized = name.replace(' ', '_')
        sanitized = sanitized.replace('-', '_')
        sanitized = sanitized.replace('.', '_')
        sanitized = sanitized.replace('/', '_')
        sanitized = sanitized.replace('\\', '_')
        
        # Handle SQLite reserved words
        sqlite_reserved_words = {
            'ORDER': 'ORDER_TABLE',
            'GROUP': 'GROUP_TABLE', 
            'SELECT': 'SELECT_TABLE',
            'FROM': 'FROM_TABLE',
            'WHERE': 'WHERE_TABLE',
            'INSERT': 'INSERT_TABLE',
            'UPDATE': 'UPDATE_TABLE',
            'DELETE': 'DELETE_TABLE',
            'CREATE': 'CREATE_TABLE',
            'DROP': 'DROP_TABLE',
            'ALTER': 'ALTER_TABLE',
            'INDEX': 'INDEX_TABLE',
            'TABLE': 'TABLE_TABLE'
        }
        
        if sanitized.upper() in sqlite_reserved_words:
            sanitized = sqlite_reserved_words[sanitized.upper()]
        
        # Ensure name starts with letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = '_' + sanitized
            
        # Limit length
        if len(sanitized) > 64:
            sanitized = sanitized[:64]
            
        return sanitized
    
    def generate_create_table_sql(self, table_name: str, table_def) -> str:
        """
        Generate CREATE TABLE SQL statement for a TopSpeed table
        
        Args:
            table_name: Name of the table
            table_def: TopSpeed table definition object
            
        Returns:
            CREATE TABLE SQL statement
        """
        sanitized_table_name = self.sanitize_table_name(table_name)
        
        # Analyze table structure for multi-dimensional fields
        analysis = self.multidimensional_handler.analyze_table_structure(table_def)
        
        # Use multidimensional handler to create schema if arrays are detected
        if analysis['has_arrays']:
            return self.multidimensional_handler.create_sqlite_schema(sanitized_table_name, analysis)
        
        # Fall back to original logic for regular tables
        # Start building the CREATE TABLE statement
        sql_parts = [f"CREATE TABLE {sanitized_table_name} ("]
        
        # Add fields
        field_definitions = []
        for field in table_def.fields:
            sanitized_field_name = self.sanitize_field_name(field.name)
            sqlite_type = self.map_field_type(field.type, field.size)
            field_definitions.append(f"    {sanitized_field_name} {sqlite_type}")
        
        # Add memo fields as BLOB
        for memo in table_def.memos:
            sanitized_memo_name = self.sanitize_field_name(memo.name)
            field_definitions.append(f"    {sanitized_memo_name} BLOB")
        
        # Join field definitions
        sql_parts.append(",\n".join(field_definitions))
        
        # Close the CREATE TABLE statement
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def generate_create_index_sql(self, table_name: str, index_def, table_def) -> str:
        """
        Generate CREATE INDEX SQL statement for a TopSpeed index
        
        Args:
            table_name: Name of the table
            index_def: TopSpeed index definition object
            table_def: TopSpeed table definition object (to look up field names)
            
        Returns:
            CREATE INDEX SQL statement
        """
        sanitized_table_name = self.sanitize_table_name(table_name)
        sanitized_index_name = self.sanitize_field_name(index_def.name)
        
        # Make index name unique by prefixing with table name
        unique_index_name = f"{sanitized_table_name}_{sanitized_index_name}"
        
        # Get field names for the index by looking up field numbers
        field_names = []
        for index_field in index_def.fields:
            # Look up the field name using the field number
            field_number = index_field.field_number
            if field_number < len(table_def.fields):
                field_name = table_def.fields[field_number].name
                sanitized_field_name = self.sanitize_field_name(field_name)
                field_names.append(sanitized_field_name)
        
        if not field_names:
            return ""
        
        # Create the index
        fields_str = ", ".join(field_names)
        return f"CREATE INDEX {unique_index_name} ON {sanitized_table_name} ({fields_str});"
    
    def map_table_schema(self, table_name: str, table_def) -> Dict[str, str]:
        """
        Map a complete TopSpeed table schema to SQLite
        
        Args:
            table_name: Name of the table
            table_def: TopSpeed table definition object
            
        Returns:
            Dictionary with 'create_table' and 'create_indexes' keys
        """
        # Convert table_name to string if it's a Container object
        table_name_str = str(table_name) if hasattr(table_name, '__str__') else table_name
        
        result = {
            'table_name': self.sanitize_table_name(table_name_str),
            'create_table': self.generate_create_table_sql(table_name_str, table_def),
            'create_indexes': []
        }
        
        # Add index creation statements
        for index in table_def.indexes:
            index_sql = self.generate_create_index_sql(table_name_str, index, table_def)
            if index_sql:
                result['create_indexes'].append(index_sql)
        
        return result
    
    def create_sqlite_schema(self, tps, output_file: str) -> None:
        """
        Create SQLite database with schema from TopSpeed file
        
        Args:
            tps: TopSpeed file object
            output_file: Path to output SQLite file
        """
        # Create SQLite database
        conn = sqlite3.connect(output_file)
        cursor = conn.cursor()
        
        try:
            # Process each table
            for table_number in tps.tables._TpsTablesList__tables:
                table = tps.tables._TpsTablesList__tables[table_number]
                
                if table.name and table.name != '':
                    # Get table definition
                    try:
                        table_def = tps.tables.get_definition(table_number)
                        
                        # Map schema
                        schema = self.map_table_schema(table.name, table_def)
                        
                        # Create table
                        cursor.execute(schema['create_table'])
                        print(f"Created table: {schema['table_name']}")
                        
                        # Create indexes
                        for index_sql in schema['create_indexes']:
                            cursor.execute(index_sql)
                            print(f"Created index for: {schema['table_name']}")
                            
                    except Exception as e:
                        print(f"Error processing table {table.name}: {e}")
                        continue
            
            # Commit changes
            conn.commit()
            print(f"Schema created successfully in {output_file}")
            
        except Exception as e:
            print(f"Error creating schema: {e}")
            conn.rollback()
        finally:
            conn.close()
