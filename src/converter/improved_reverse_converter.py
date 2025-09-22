#!/usr/bin/env python3
"""
Improved Reverse Converter - Phase 2.1 Implementation

This module implements the improved reverse converter with proper TopSpeed header format
based on the detailed format specification from Phase 1 analysis.
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


class ImprovedReverseConverter:
    """
    Improved converter for creating TopSpeed files from SQLite databases
    with proper header format implementation
    """
    
    def __init__(self, progress_callback=None):
        """
        Initialize improved reverse converter
        
        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # TopSpeed file structures
        self._init_construct_structures()
    
    def _init_construct_structures(self):
        """Initialize construct structures for TopSpeed file format"""
        
        # Field types (from pytopspeed analysis)
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
        
        # Complete table definition
        self.TABLE_DEFINITION_STRUCT = Struct(
            "min_version_driver" / Int16ul,
            "record_size" / Int16ul,
            "field_count" / Int16ul,
            "memo_count" / Int16ul,
            "index_count" / Int16ul,
            "fields" / Array(lambda x: x['field_count'], self.TABLE_DEFINITION_FIELD_STRUCT),
            "memos" / Array(lambda x: x['memo_count'], Bytes(1)),  # Simplified for now
            "indexes" / Array(lambda x: x['index_count'], Bytes(1))  # Simplified for now
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
        
        # Page header structure (from format specification)
        self.PAGE_HEADER_STRUCT = Struct(
            "offset" / Int32ul,
            "size" / Int16ul,
            "uncompressed_size" / Int16ul,
            "uncompressed_unabridged_size" / Int16ul,
            "record_count" / Int16ul,
            "hierarchy_level" / Byte,
            "padding" / Bytes(7)  # 7 bytes padding to make 16 bytes total
        )
        
        # File header structure (exact format from specification)
        self.FILE_HEADER_STRUCT = Struct(
            "offset" / Int32ul,                    # 0x00: Always 0x00000000
            "size" / Int16ul,                      # 0x04: Header size (0x0200)
            "file_size" / Int32ul,                 # 0x06: Total file size
            "allocated_file_size" / Int32ul,       # 0x0A: Allocated file size
            "top_speed_mark" / Const(b"tOpS\x00\x00"),  # 0x0E: Signature
            "last_issued_row" / Int32ub,           # 0x14: Last issued row (big-endian)
            "change_count" / Int32ul,              # 0x18: Change count (little-endian)
            "page_root_ref" / Int32ul,             # 0x1C: Page root reference
            "block_references" / Bytes(0x1E0)      # 0x20: Block references (480 bytes)
        )
    
    def create_topspeed_file(self, sqlite_file: str, output_file: str, file_type: str = "PHD") -> Dict[str, Any]:
        """
        Create a TopSpeed file from SQLite database with proper header format
        
        Args:
            sqlite_file: Path to input SQLite file
            output_file: Path to output TopSpeed file
            file_type: Type of file (PHD or MOD)
            
        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'file_created': output_file,
            'tables_processed': 0,
            'records_processed': 0,
            'duration': 0,
            'errors': [],
            'file_size': 0,
            'header_info': {}
        }
        
        try:
            self.logger.info(f"Creating {file_type} file: {output_file}")
            
            # Check if input file exists
            if not os.path.exists(sqlite_file):
                error_msg = f"SQLite file not found: {sqlite_file}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            # Connect to SQLite database
            conn = sqlite3.connect(sqlite_file)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            all_tables = [row[0] for row in cursor.fetchall()]
            
            # Filter tables by prefix if present
            if file_type == "PHD":
                tables = [t for t in all_tables if t.startswith('phd_') or not any(t.startswith(p) for p in ['phd_', 'mod_'])]
            else:  # MOD
                tables = [t for t in all_tables if t.startswith('mod_')]
            
            if not tables:
                error_msg = f"No {file_type} tables found in SQLite database"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                conn.close()
                return results
            
            self.logger.info(f"Found {len(tables)} {file_type} tables to process")
            
            # Create file with proper header
            with open(output_file, 'wb') as f:
                # First, estimate file size
                estimated_size = self._estimate_file_size(conn, tables)
                self.logger.info(f"Estimated file size: {estimated_size:,} bytes")
                
                # Create and write proper header
                header_data = self._create_proper_file_header(estimated_size, file_type)
                f.write(header_data)
                results['header_info'] = self._parse_header_info(header_data)
                
                # Write table definitions and data
                for table_name in tables:
                    self.logger.info(f"Processing table: {table_name}")
                    
                    # Remove prefix to get original table name
                    original_name = table_name[4:] if table_name.startswith(('phd_', 'mod_')) else table_name
                    
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
            
            # Get actual file size and update header if needed
            actual_file_size = os.path.getsize(output_file)
            results['file_size'] = actual_file_size
            
            # Update header with actual file size if different
            if actual_file_size != estimated_size:
                self.logger.info(f"Updating header with actual file size: {actual_file_size:,} bytes")
                self._update_file_header_size(output_file, actual_file_size)
            
            conn.close()
            
            results['success'] = True
            self.logger.info(f"Successfully created {file_type} file: {output_file}")
            self.logger.info(f"Final file size: {results['file_size']:,} bytes")
            
        except Exception as e:
            self.logger.error(f"Error creating {file_type} file: {e}")
            results['errors'].append(str(e))
        
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def _estimate_file_size(self, conn: sqlite3.Connection, tables: List[str]) -> int:
        """Estimate file size based on tables and data"""
        base_size = 0x200  # Header size
        
        cursor = conn.cursor()
        total_records = 0
        total_fields = 0
        
        for table_name in tables:
            # Count records
            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
            record_count = cursor.fetchone()[0]
            total_records += record_count
            
            # Count fields
            cursor.execute(f"PRAGMA table_info([{table_name}])")
            field_count = len(cursor.fetchall())
            total_fields += field_count
        
        # Estimate sizes
        # Each table needs: name record + definition record + data records
        table_overhead = len(tables) * 0x200  # Rough estimate for table metadata
        record_overhead = total_records * 0x100  # Rough estimate for record overhead
        data_size = total_records * 0x200  # Rough estimate for actual data
        
        estimated_size = base_size + table_overhead + record_overhead + data_size
        
        # Round up to nearest 64-byte boundary (TopSpeed requirement)
        estimated_size = ((estimated_size + 63) // 64) * 64
        
        return estimated_size
    
    def _create_proper_file_header(self, file_size: int, file_type: str) -> bytes:
        """
        Create proper TopSpeed file header matching exact format specification
        
        Args:
            file_size: Estimated total file size
            file_type: Type of file (PHD or MOD)
            
        Returns:
            Properly formatted header bytes
        """
        # Calculate page root reference (first page after header)
        page_root_ref = 1 if file_type == "MOD" else 2  # Based on analysis
        
        # Calculate last issued row and change count
        last_issued_row = 1
        change_count = 1
        
        # Create header data
        header_data = struct.pack('<I', 0x00000000)  # offset (always 0)
        header_data += struct.pack('<H', 0x0200)     # size (always 512)
        header_data += struct.pack('<I', file_size)  # file_size
        header_data += struct.pack('<I', file_size)  # allocated_file_size (same as file_size)
        header_data += b"tOpS\x00\x00"              # top_speed_mark (signature)
        header_data += struct.pack('>I', last_issued_row)  # last_issued_row (big-endian)
        header_data += struct.pack('<I', change_count)     # change_count (little-endian)
        header_data += struct.pack('<I', page_root_ref)    # page_root_ref (little-endian)
        
        # Create block references
        # Based on analysis: 60 blocks, each with start_ref and end_ref
        block_references = b''
        
        # Calculate block size and number of blocks
        block_size = 0x10000  # 64KB blocks
        num_blocks = 60  # Standard number from analysis
        
        for i in range(num_blocks):
            if i == 0:
                # First block: covers header area
                start_ref = 0x00000000
                end_ref = 0x00000000
            elif i == 1:
                # Second block: covers first data area
                start_ref = 0x0000029C if file_type == "PHD" else 0x00000020
                end_ref = 0x0000065E if file_type == "PHD" else 0x000000B4
            else:
                # Subsequent blocks: calculate based on file size
                start_ref = i * 0x1000  # Rough calculation
                end_ref = min(start_ref + 0x1000 - 1, (file_size // 0x100) - 1)
            
            block_references += struct.pack('<I', start_ref)  # block_start_ref
            block_references += struct.pack('<I', end_ref)    # block_end_ref
        
        # Pad block references to exactly 480 bytes (0x1E0)
        if len(block_references) < 0x1E0:
            block_references += b'\x00' * (0x1E0 - len(block_references))
        elif len(block_references) > 0x1E0:
            block_references = block_references[:0x1E0]
        
        header_data += block_references
        
        # Ensure header is exactly 512 bytes
        if len(header_data) != 0x200:
            self.logger.warning(f"Header size mismatch: {len(header_data)} bytes, expected 512")
            if len(header_data) < 0x200:
                header_data += b'\x00' * (0x200 - len(header_data))
            else:
                header_data = header_data[:0x200]
        
        return header_data
    
    def _parse_header_info(self, header_data: bytes) -> Dict[str, Any]:
        """Parse header information for validation"""
        if len(header_data) < 0x200:
            return {'error': 'Header too small'}
        
        try:
            offset = struct.unpack('<I', header_data[0:4])[0]
            size = struct.unpack('<H', header_data[4:6])[0]
            file_size = struct.unpack('<I', header_data[6:10])[0]
            allocated_file_size = struct.unpack('<I', header_data[10:14])[0]
            top_speed_mark = header_data[14:20]
            last_issued_row = struct.unpack('>I', header_data[20:24])[0]
            change_count = struct.unpack('<I', header_data[24:28])[0]
            page_root_ref = struct.unpack('<I', header_data[28:32])[0]
            
            return {
                'offset': offset,
                'size': size,
                'file_size': file_size,
                'allocated_file_size': allocated_file_size,
                'top_speed_mark': top_speed_mark.hex(),
                'last_issued_row': last_issued_row,
                'change_count': change_count,
                'page_root_ref': page_root_ref,
                'valid_signature': top_speed_mark == b"tOpS\x00\x00"
            }
        except Exception as e:
            return {'error': f'Header parsing failed: {e}'}
    
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
        try:
            name_bytes = table_name.encode('ascii')
        except UnicodeEncodeError:
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
                        safe_text = str(value).encode('ascii', errors='replace').decode('ascii')
                        text_bytes = safe_text.encode('ascii')
                    data += text_bytes
                    data += b'\x00' * (255 - len(text_bytes))  # Pad to 255 bytes
                elif col['type'] == 'REAL':
                    data += struct.pack('<d', float(value))
                elif col['type'] == 'BLOB':
                    if isinstance(value, bytes):
                        data += value
                    else:
                        data += str(value).encode('ascii')
        
        return data
    
    def _update_file_header_size(self, file_path: str, actual_size: int):
        """Update file header with actual file size."""
        try:
            with open(file_path, 'r+b') as f:
                # Update file_size at offset 0x06
                f.seek(0x06)
                f.write(struct.pack('<I', actual_size))
                
                # Update allocated_file_size at offset 0x0A
                f.seek(0x0A)
                f.write(struct.pack('<I', actual_size))
                
        except Exception as e:
            self.logger.warning(f"Failed to update header size: {e}")
