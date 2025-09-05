#!/usr/bin/env python3
"""
Reverse Converter - Convert SQLite databases back to TopSpeed files

This module provides functionality to convert SQLite databases back to
TopSpeed .phd and .mod files, reconstructing the binary format.
"""

import os
import sqlite3
import struct
import logging
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path

from construct import (
    Array, Byte, Bytes, Const, Float32l, Float64l, Struct,
    Int16sl, Int32sl, Int32ub, Int8ul, Int16ul, Int32ul,
    CString, PaddedString, Enum, BitsInteger, BitStruct, Flag, Padding, If
)


class ReverseConverter:
    """
    Converter for creating TopSpeed files from SQLite databases
    """
    
    def __init__(self, progress_callback=None):
        """
        Initialize reverse converter
        
        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # TopSpeed file structures
        self._init_construct_structures()
    
    def _init_construct_structures(self):
        """Initialize construct structures for TopSpeed file format"""
        
        # Field types
        self.FIELD_TYPE_STRUCT = Enum(Byte,
            BYTE=1,
            SHORT=2,
            DATE=3,
            TIME=4,
            LONG=5,
            STRING=6,
            DECIMAL=7,
            MEMO=8,
            BLOB=9,
            CSTRING=10,
            PSTRING=11,
            PICTURE=12,
            _default_='STRING'
        )
        
        # Table definition structures
        self.TABLE_DEFINITION_FIELD_STRUCT = Struct(
            "type" / self.FIELD_TYPE_STRUCT,
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
        
        # Index structures
        self.INDEX_TYPE_STRUCT = Enum(BitsInteger(2),
            INDEX=1,
            DYNAMIC_INDEX=2
        )
        
        self.INDEX_FIELD_ORDER_TYPE_STRUCT = Enum(Int16ul,
            ASCENDING=0,
            DESCENDING=1,
            _default_='DESCENDING'
        )
        
        self.TABLE_DEFINITION_INDEX_STRUCT = Struct(
            "external_filename" / CString("ascii"),
            "index_mark" / If(lambda x: len(x['external_filename']) == 0, Const(1, Byte)),
            "name" / CString("ascii"),
            "flags" / BitStruct(
                Padding(1),
                "type" / self.INDEX_TYPE_STRUCT,
                Padding(2),
                "NOCASE" / Flag,
                "OPT" / Flag,
                "DUP" / Flag
            ),
            "field_count" / Int16ul,
            "fields" / Array(lambda x: x['field_count'],
                Struct(
                    "field_number" / Int16ul,
                    "order_type" / self.INDEX_FIELD_ORDER_TYPE_STRUCT
                )
            )
        )
        
        # Memo structures
        self.MEMO_TYPE_STRUCT = Enum(Flag,
            BLOB=1
        )
        
        self.TABLE_DEFINITION_MEMO_STRUCT = Struct(
            "external_filename" / CString("ascii"),
            "memo_mark" / If(lambda x: len(x['external_filename']) == 0, Const(1, Byte)),
            "name" / CString("ascii"),
            "size" / Int16ul,
            "flags" / BitStruct(
                Padding(5),
                "memo_type" / self.MEMO_TYPE_STRUCT,
                "BINARY" / Flag,
                "Flag" / Flag,
                Padding(8)
            )
        )
        
        # Complete table definition
        self.TABLE_DEFINITION_STRUCT = Struct(
            "min_version_driver" / Int16ul,
            "record_size" / Int16ul,
            "field_count" / Int16ul,
            "memo_count" / Int16ul,
            "index_count" / Int16ul,
            "fields" / Array(lambda x: x['field_count'], self.TABLE_DEFINITION_FIELD_STRUCT),
            "memos" / Array(lambda x: x['memo_count'], self.TABLE_DEFINITION_MEMO_STRUCT),
            "indexes" / Array(lambda x: x['index_count'], self.TABLE_DEFINITION_INDEX_STRUCT)
        )
        
        # Record structures
        self.RECORD_TYPE = Enum(Byte,
            NULL=None,
            DATA=0xF3,
            METADATA=0xF6,
            TABLE_DEFINITION=0xFA,
            TABLE_NAME=0xFE,
            MEMO=0xFC,
            _default_='INDEX'
        )
        
        self.DATA_RECORD_DATA = Struct(
            "record_number" / Int32ub,
            "data" / Bytes(lambda ctx: ctx._.data_size - 9)
        )
        
        self.TABLE_DEFINITION_RECORD_DATA = Struct(
            "table_definition_bytes" / Bytes(lambda ctx: ctx._.data_size - 5)
        )
        
        # Page header structure
        self.PAGE_HEADER_STRUCT = Struct(
            "offset" / Int32ul,
            "size" / Int16ul,
            "uncompressed_size" / Int16ul,
            "uncompressed_unabridged_size" / Int16ul,
            "record_count" / Int16ul,
            "hierarchy_level" / Byte
        )
        
        # File header structure
        self.FILE_HEADER_STRUCT = Struct(
            "offset" / Int32ul,
            "size" / Int16ul,
            "file_size" / Int32ul,
            "allocated_file_size" / Int32ul,
            "top_speed_mark" / Const(b"tOpS\x00\x00"),
            "last_issued_row" / Int32ub,
            "change_count" / Int32ul,
            "page_root_ref" / Int32ul,
            "block_start_ref" / Array(lambda ctx: (ctx["size"] - 0x20) // 2 // 4, Int32ul),
            "block_end_ref" / Array(lambda ctx: (ctx["size"] - 0x20) // 2 // 4, Int32ul)
        )
    
    def convert_sqlite_to_topspeed(self, sqlite_file: str, output_dir: str) -> Dict[str, Any]:
        """
        Convert SQLite database back to TopSpeed files
        
        Args:
            sqlite_file: Path to input SQLite file
            output_dir: Directory to write output files
            
        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'files_created': [],
            'tables_processed': 0,
            'records_processed': 0,
            'duration': 0,
            'errors': []
        }
        
        try:
            # Check if input file exists
            if not os.path.exists(sqlite_file):
                error_msg = f"SQLite file not found: {sqlite_file}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            self.logger.info(f"Starting reverse conversion: {sqlite_file} -> {output_dir}")
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Connect to SQLite database
            conn = sqlite3.connect(sqlite_file)
            cursor = conn.cursor()
            
            # Get all tables and categorize by prefix
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            all_tables = [row[0] for row in cursor.fetchall()]
            
            phd_tables = [t for t in all_tables if t.startswith('phd_')]
            mod_tables = [t for t in all_tables if t.startswith('mod_')]
            
            # If no prefixed tables found, treat all tables as PHD tables (single file conversion)
            if not phd_tables and not mod_tables and all_tables:
                phd_tables = all_tables
                self.logger.info(f"No prefixed tables found, treating all {len(all_tables)} tables as PHD tables")
            else:
                self.logger.info(f"Found {len(phd_tables)} PHD tables, {len(mod_tables)} MOD tables")
            
            # Process PHD file if we have PHD tables
            if phd_tables:
                phd_file = os.path.join(output_dir, "TxWells.PHD")
                phd_result = self._create_topspeed_file(
                    conn, phd_tables, phd_file, "PHD"
                )
                if phd_result['success']:
                    results['files_created'].append(phd_file)
                    results['tables_processed'] += phd_result['tables_processed']
                    results['records_processed'] += phd_result['records_processed']
                else:
                    results['errors'].extend(phd_result['errors'])
            
            # Process MOD file if we have MOD tables
            if mod_tables:
                mod_file = os.path.join(output_dir, "TxWells.mod")
                mod_result = self._create_topspeed_file(
                    conn, mod_tables, mod_file, "MOD"
                )
                if mod_result['success']:
                    results['files_created'].append(mod_file)
                    results['tables_processed'] += mod_result['tables_processed']
                    results['records_processed'] += mod_result['records_processed']
                else:
                    results['errors'].extend(mod_result['errors'])
            
            conn.close()
            
            results['success'] = len(results['files_created']) > 0
            self.logger.info(f"Reverse conversion completed: {results['success']}")
            
        except Exception as e:
            self.logger.error(f"Reverse conversion failed: {e}")
            results['errors'].append(str(e))
        
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def _create_topspeed_file(self, conn: sqlite3.Connection, tables: List[str], 
                            output_file: str, file_type: str) -> Dict[str, Any]:
        """
        Create a TopSpeed file from SQLite tables
        
        Args:
            conn: SQLite connection
            tables: List of table names to include
            output_file: Output file path
            file_type: Type of file (PHD or MOD)
            
        Returns:
            Dictionary with conversion results
        """
        results = {
            'success': False,
            'tables_processed': 0,
            'records_processed': 0,
            'errors': []
        }
        
        try:
            self.logger.info(f"Creating {file_type} file: {output_file}")
            
            # Create file with basic structure
            with open(output_file, 'wb') as f:
                # Write file header (simplified)
                header_data = self._create_file_header(len(tables))
                f.write(header_data)
                
                # Write table definitions and data
                for table_name in tables:
                    self.logger.info(f"Processing table: {table_name}")
                    
                    # Remove prefix to get original table name
                    original_name = table_name[4:]  # Remove 'phd_' or 'mod_' prefix
                    
                    # Get table schema from SQLite
                    table_schema = self._get_table_schema(conn, table_name)
                    
                    # Create table definition
                    table_def = self._create_table_definition(original_name, table_schema)
                    
                    # Write table name record
                    self._write_table_name_record(f, original_name)
                    
                    # Write table definition record
                    self._write_table_definition_record(f, table_def)
                    
                    # Write data records
                    record_count = self._write_data_records(conn, f, table_name, table_schema)
                    
                    results['tables_processed'] += 1
                    results['records_processed'] += record_count
                    
                    self.logger.info(f"Processed {record_count} records from {table_name}")
            
            results['success'] = True
            self.logger.info(f"Successfully created {file_type} file with {results['tables_processed']} tables")
            
        except Exception as e:
            self.logger.error(f"Error creating {file_type} file: {e}")
            results['errors'].append(str(e))
        
        return results
    
    def _create_file_header(self, table_count: int) -> bytes:
        """Create TopSpeed file header"""
        # This is a simplified header - in a full implementation,
        # we would need to calculate proper page references and block allocations
        
        header_data = struct.pack('<I', 0x200)  # offset
        header_data += struct.pack('<H', 0x200)  # size
        header_data += struct.pack('<I', 0x10000)  # file_size (placeholder)
        header_data += struct.pack('<I', 0x10000)  # allocated_file_size (placeholder)
        header_data += b"tOpS\x00\x00"  # top_speed_mark
        header_data += struct.pack('>I', 1)  # last_issued_row
        header_data += struct.pack('<I', 1)  # change_count
        header_data += struct.pack('<I', 0)  # page_root_ref (placeholder)
        
        # Block references (simplified)
        block_count = 1
        header_data += struct.pack('<H', block_count)  # block count
        for i in range(block_count):
            header_data += struct.pack('<I', 0)  # block_start_ref
            header_data += struct.pack('<I', 0)  # block_end_ref
        
        # Pad to 0x200 bytes
        header_data += b'\x00' * (0x200 - len(header_data))
        
        return header_data
    
    def _get_table_schema(self, conn: sqlite3.Connection, table_name: str) -> List[Dict]:
        """Get table schema from SQLite"""
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info([{table_name}])")
        columns = cursor.fetchall()
        
        schema = []
        for col in columns:
            schema.append({
                'name': col[1],
                'type': col[2],
                'not_null': bool(col[3]),
                'default_value': col[4],
                'primary_key': bool(col[5])
            })
        
        return schema
    
    def _create_table_definition(self, table_name: str, schema: List[Dict]) -> Dict:
        """Create TopSpeed table definition from SQLite schema"""
        
        # Map SQLite types to TopSpeed types
        type_mapping = {
            'INTEGER': 'LONG',
            'TEXT': 'STRING',
            'REAL': 'DECIMAL',
            'BLOB': 'BLOB',
            'DATE': 'DATE',
            'TIME': 'TIME'
        }
        
        fields = []
        memos = []
        indexes = []
        
        offset = 0
        field_number = 0
        
        for col in schema:
            field_type = type_mapping.get(col['type'], 'STRING')
            
            # Calculate field size
            if field_type == 'LONG':
                size = 4
            elif field_type == 'STRING':
                size = 255  # Default string size
            elif field_type == 'DECIMAL':
                size = 8
            elif field_type == 'BLOB':
                size = 0  # Memo field
            else:
                size = 4
            
            if field_type == 'BLOB':
                # Create memo field
                memos.append({
                    'name': col['name'],
                    'size': 0,
                    'memo_type': 1,  # BLOB
                    'external_filename': '',
                    'flags': 0
                })
            else:
                # Create regular field
                fields.append({
                    'type': field_type,
                    'offset': offset,
                    'name': col['name'],
                    'array_element_count': 1,
                    'size': size,
                    'overlaps': 0,
                    'number': field_number,
                    'array_element_size': size if field_type in ['STRING', 'CSTRING'] else 0,
                    'template': 0
                })
                offset += size
                field_number += 1
        
        return {
            'min_version_driver': 0,
            'record_size': offset,
            'field_count': len(fields),
            'memo_count': len(memos),
            'index_count': len(indexes),
            'fields': fields,
            'memos': memos,
            'indexes': indexes
        }
    
    def _write_table_name_record(self, f, table_name: str):
        """Write TABLE_NAME record to file"""
        # Handle encoding issues by using latin-1 or replacing problematic characters
        try:
            name_bytes = table_name.encode('ascii')
        except UnicodeEncodeError:
            # Replace non-ASCII characters with safe alternatives
            safe_name = table_name.encode('ascii', errors='replace').decode('ascii')
            name_bytes = safe_name.encode('ascii')
        
        data_size = 9 + len(name_bytes)
        
        # Record header
        f.write(struct.pack('<H', data_size))  # data_size
        f.write(struct.pack('<I', 0))  # table_number (placeholder)
        f.write(b'\xFE')  # TABLE_NAME record type
        
        # Record data
        f.write(name_bytes)
        f.write(b'\x00' * (data_size - 9 - len(name_bytes)))  # padding
    
    def _write_table_definition_record(self, f, table_def: Dict):
        """Write TABLE_DEFINITION record to file"""
        # This is a simplified implementation
        # In a full implementation, we would serialize the complete table definition
        
        data_size = 100  # Placeholder size
        f.write(struct.pack('<H', data_size))  # data_size
        f.write(struct.pack('<I', 0))  # table_number (placeholder)
        f.write(b'\xFA')  # TABLE_DEFINITION record type
        
        # Placeholder table definition data
        f.write(b'\x00' * (data_size - 5))
    
    def _write_data_records(self, conn: sqlite3.Connection, f, table_name: str, 
                          schema: List[Dict]) -> int:
        """Write data records to file"""
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM [{table_name}]")
        rows = cursor.fetchall()
        
        record_count = 0
        for row in rows:
            # Convert row data to binary format
            record_data = self._convert_row_to_binary(row, schema)
            
            # Write record header
            data_size = 9 + len(record_data)
            f.write(struct.pack('<H', data_size))  # data_size
            f.write(struct.pack('<I', record_count + 1))  # record_number
            f.write(b'\xF3')  # DATA record type
            
            # Write record data
            f.write(record_data)
            
            record_count += 1
        
        return record_count
    
    def _convert_row_to_binary(self, row: Tuple, schema: List[Dict]) -> bytes:
        """Convert SQLite row to binary format"""
        data = b''
        
        for i, (col, value) in enumerate(zip(schema, row)):
            if value is None:
                # Handle NULL values
                if col['type'] == 'INTEGER':
                    data += struct.pack('<i', 0)
                elif col['type'] == 'TEXT':
                    data += b'\x00' * 255  # Null string
                elif col['type'] == 'REAL':
                    data += struct.pack('<d', 0.0)
                else:
                    data += b'\x00' * 4
            else:
                # Handle non-NULL values
                if col['type'] == 'INTEGER':
                    data += struct.pack('<i', int(value))
                elif col['type'] == 'TEXT':
                    try:
                        text_bytes = str(value).encode('ascii')
                    except UnicodeEncodeError:
                        # Replace non-ASCII characters with safe alternatives
                        safe_text = str(value).encode('ascii', errors='replace').decode('ascii')
                        text_bytes = safe_text.encode('ascii')
                    data += text_bytes
                    data += b'\x00' * (255 - len(text_bytes))  # Pad to 255 bytes
                elif col['type'] == 'REAL':
                    data += struct.pack('<d', float(value))
                elif col['type'] == 'BLOB':
                    # Handle BLOB data
                    if isinstance(value, bytes):
                        data += value
                    else:
                        data += str(value).encode('ascii')
        
        return data
