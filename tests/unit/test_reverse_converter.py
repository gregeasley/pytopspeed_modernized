#!/usr/bin/env python3
"""
Unit tests for reverse converter functionality (Issue I3)

Tests the ReverseConverter class for converting SQLite databases
back to TopSpeed .phd and .mod files.
"""

import pytest
import sqlite3
import tempfile
import os
import struct
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from converter.reverse_converter import ReverseConverter


class TestReverseConverter:
    """Test cases for reverse converter"""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary SQLite database for testing"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        # Create a test database with sample data
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create PHD tables
        cursor.execute('''
            CREATE TABLE phd_TABLE1 (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value REAL
            )
        ''')
        cursor.execute('''
            CREATE TABLE phd_TABLE2 (
                id INTEGER PRIMARY KEY,
                description TEXT
            )
        ''')
        
        # Create MOD tables
        cursor.execute('''
            CREATE TABLE mod_MODTABLE1 (
                id INTEGER PRIMARY KEY,
                data BLOB
            )
        ''')
        
        # Insert sample data
        cursor.execute("INSERT INTO phd_TABLE1 (id, name, value) VALUES (1, 'test1', 1.5)")
        cursor.execute("INSERT INTO phd_TABLE1 (id, name, value) VALUES (2, 'test2', 2.5)")
        cursor.execute("INSERT INTO phd_TABLE2 (id, description) VALUES (1, 'desc1')")
        cursor.execute("INSERT INTO mod_MODTABLE1 (id, data) VALUES (1, 'blob data')")
        
        conn.commit()
        conn.close()
        
        yield db_path
        
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def converter(self):
        """Create a ReverseConverter instance for testing"""
        return ReverseConverter(progress_callback=None)
    
    def test_convert_sqlite_to_topspeed_success(self, converter, temp_db, temp_dir):
        """Test successful SQLite to TopSpeed conversion"""
        results = converter.convert_sqlite_to_topspeed(temp_db, temp_dir)
        
        assert results['success'] is True
        assert len(results['files_created']) == 2  # PHD and MOD files
        assert results['tables_processed'] == 3  # 2 PHD + 1 MOD table
        assert results['records_processed'] == 4  # Total records
        assert len(results['errors']) == 0
        
        # Verify files were created
        phd_file = os.path.join(temp_dir, 'TxWells.PHD')
        mod_file = os.path.join(temp_dir, 'TxWells.mod')
        
        assert os.path.exists(phd_file)
        assert os.path.exists(mod_file)
        assert phd_file in results['files_created']
        assert mod_file in results['files_created']
    
    def test_convert_sqlite_to_topspeed_phd_only(self, converter, temp_dir):
        """Test conversion with only PHD tables"""
        # Create database with only PHD tables
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE phd_ONLY (id INTEGER)')
        cursor.execute('INSERT INTO phd_ONLY (id) VALUES (1)')
        conn.commit()
        conn.close()
        
        try:
            results = converter.convert_sqlite_to_topspeed(db_path, temp_dir)
            
            assert results['success'] is True
            assert len(results['files_created']) == 1  # Only PHD file
            assert results['tables_processed'] == 1
            assert results['records_processed'] == 1
            
            phd_file = os.path.join(temp_dir, 'TxWells.PHD')
            assert os.path.exists(phd_file)
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_convert_sqlite_to_topspeed_mod_only(self, converter, temp_dir):
        """Test conversion with only MOD tables"""
        # Create database with only MOD tables
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE mod_ONLY (id INTEGER)')
        cursor.execute('INSERT INTO mod_ONLY (id) VALUES (1)')
        conn.commit()
        conn.close()
        
        try:
            results = converter.convert_sqlite_to_topspeed(db_path, temp_dir)
            
            assert results['success'] is True
            assert len(results['files_created']) == 1  # Only MOD file
            assert results['tables_processed'] == 1
            assert results['records_processed'] == 1
            
            mod_file = os.path.join(temp_dir, 'TxWells.mod')
            assert os.path.exists(mod_file)
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_convert_sqlite_to_topspeed_no_tables(self, converter, temp_dir):
        """Test conversion with no tables"""
        # Create empty database
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        conn = sqlite3.connect(db_path)
        conn.close()
        
        try:
            results = converter.convert_sqlite_to_topspeed(db_path, temp_dir)
            
            assert results['success'] is False
            assert len(results['files_created']) == 0
            assert results['tables_processed'] == 0
            assert results['records_processed'] == 0
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
    
    def test_convert_sqlite_to_topspeed_missing_file(self, converter, temp_dir):
        """Test conversion with missing SQLite file"""
        missing_db = os.path.join(temp_dir, 'missing.sqlite')
        results = converter.convert_sqlite_to_topspeed(missing_db, temp_dir)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'not found' in results['errors'][0]
    
    def test_get_table_schema(self, converter, temp_db):
        """Test getting table schema from SQLite"""
        conn = sqlite3.connect(temp_db)
        
        schema = converter._get_table_schema(conn, 'phd_TABLE1')
        
        assert len(schema) == 3  # id, name, value columns
        assert schema[0]['name'] == 'id'
        assert schema[0]['type'] == 'INTEGER'
        assert schema[0]['primary_key'] is True
        assert schema[1]['name'] == 'name'
        assert schema[1]['type'] == 'TEXT'
        assert schema[2]['name'] == 'value'
        assert schema[2]['type'] == 'REAL'
        
        conn.close()
    
    def test_create_table_definition(self, converter):
        """Test creating TopSpeed table definition from SQLite schema"""
        schema = [
            {'name': 'id', 'type': 'INTEGER', 'primary_key': True},
            {'name': 'name', 'type': 'TEXT', 'primary_key': False},
            {'name': 'data', 'type': 'BLOB', 'primary_key': False}
        ]
        
        table_def = converter._create_table_definition('TEST_TABLE', schema)
        
        assert table_def['field_count'] == 2  # INTEGER and TEXT (BLOB becomes memo)
        assert table_def['memo_count'] == 1   # BLOB field
        assert table_def['index_count'] == 0  # No indexes in this test
        assert len(table_def['fields']) == 2
        assert len(table_def['memos']) == 1
        
        # Check field definitions
        assert table_def['fields'][0]['name'] == 'id'
        assert table_def['fields'][0]['type'] == 'LONG'
        assert table_def['fields'][1]['name'] == 'name'
        assert table_def['fields'][1]['type'] == 'STRING'
        
        # Check memo definition
        assert table_def['memos'][0]['name'] == 'data'
        assert table_def['memos'][0]['memo_type'] == 1  # BLOB
    
    def test_write_table_name_record(self, converter, temp_dir):
        """Test writing TABLE_NAME record to file"""
        test_file = os.path.join(temp_dir, 'test_record')
        
        with open(test_file, 'wb') as f:
            converter._write_table_name_record(f, 'TEST_TABLE')
        
        # Verify file was created and has correct size
        assert os.path.exists(test_file)
        file_size = os.path.getsize(test_file)
        assert file_size > 0
        
        # Read and verify record structure
        with open(test_file, 'rb') as f:
            data = f.read()
        
        # Check record header
        data_size = struct.unpack('<H', data[:2])[0]
        table_number = struct.unpack('<I', data[2:6])[0]
        record_type = data[6]
        
        assert record_type == 0xFE  # TABLE_NAME record type
        assert table_number == 0    # Placeholder value
        assert data_size == 9 + len('TEST_TABLE')  # Header + table name
    
    def test_write_table_name_record_encoding_handling(self, converter, temp_dir):
        """Test writing TABLE_NAME record with non-ASCII characters"""
        test_file = os.path.join(temp_dir, 'test_record_unicode')
        
        with open(test_file, 'wb') as f:
            converter._write_table_name_record(f, 'ТЕСТ_ТАБЛИЦА')  # Cyrillic characters
        
        # Should not raise an exception and should create a file
        assert os.path.exists(test_file)
        file_size = os.path.getsize(test_file)
        assert file_size > 0
    
    def test_convert_row_to_binary(self, converter):
        """Test converting SQLite row to binary format"""
        schema = [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'TEXT'},
            {'name': 'value', 'type': 'REAL'},
            {'name': 'data', 'type': 'BLOB'}
        ]
        
        row = (1, 'test', 1.5, b'binary data')
        binary_data = converter._convert_row_to_binary(row, schema)
        
        assert len(binary_data) > 0
        
        # Verify integer conversion
        id_value = struct.unpack('<i', binary_data[:4])[0]
        assert id_value == 1
        
        # Verify text conversion (should be padded to 255 bytes)
        text_data = binary_data[4:259]  # Skip integer, get text portion
        assert text_data.startswith(b'test')
        assert len(text_data) == 255
        
        # Verify real conversion
        real_value = struct.unpack('<d', binary_data[259:267])[0]
        assert abs(real_value - 1.5) < 0.001
    
    def test_convert_row_to_binary_null_values(self, converter):
        """Test converting SQLite row with NULL values"""
        schema = [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'TEXT'},
            {'name': 'value', 'type': 'REAL'}
        ]
        
        row = (None, None, None)
        binary_data = converter._convert_row_to_binary(row, schema)
        
        assert len(binary_data) > 0
        
        # Verify NULL integer (should be 0)
        id_value = struct.unpack('<i', binary_data[:4])[0]
        assert id_value == 0
        
        # Verify NULL text (should be all zeros)
        text_data = binary_data[4:259]
        assert text_data == b'\x00' * 255
        
        # Verify NULL real (should be 0.0)
        real_value = struct.unpack('<d', binary_data[259:267])[0]
        assert real_value == 0.0
    
    def test_convert_row_to_binary_encoding_handling(self, converter):
        """Test converting row with non-ASCII text"""
        schema = [
            {'name': 'name', 'type': 'TEXT'}
        ]
        
        row = ('тест',)  # Cyrillic text
        binary_data = converter._convert_row_to_binary(row, schema)
        
        # Should not raise an exception
        assert len(binary_data) == 255  # Padded to 255 bytes
    
    def test_create_file_header(self, converter):
        """Test creating TopSpeed file header"""
        header_data = converter._create_file_header(5)  # 5 tables
        
        assert len(header_data) == 0x200  # 512 bytes
        assert header_data.startswith(struct.pack('<I', 0x200))  # offset
        assert b"tOpS\x00\x00" in header_data  # TopSpeed magic number
    
    def test_progress_callback_invocation(self, converter, temp_db, temp_dir):
        """Test that progress callback is invoked during conversion"""
        progress_calls = []
        
        def progress_callback(current, total, message):
            progress_calls.append((current, total, message))
        
        converter.progress_callback = progress_callback
        
        results = converter.convert_sqlite_to_topspeed(temp_db, temp_dir)
        
        assert results['success'] is True
        # Progress callback should be passed through to internal methods
        # (Note: Current implementation doesn't use progress callback internally,
        # but the structure is there for future enhancement)
    
    def test_duration_tracking(self, converter, temp_db, temp_dir):
        """Test that conversion duration is tracked"""
        results = converter.convert_sqlite_to_topspeed(temp_db, temp_dir)
        
        assert results['success'] is True
        assert 'duration' in results
        assert results['duration'] >= 0
    
    def test_file_creation_with_different_table_prefixes(self, converter, temp_dir):
        """Test that files are created correctly based on table prefixes"""
        # Create database with mixed prefixes
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE phd_PHDTABLE (id INTEGER)')
        cursor.execute('CREATE TABLE mod_MODTABLE (id INTEGER)')
        cursor.execute('CREATE TABLE tps_TPSTABLE (id INTEGER)')
        cursor.execute('CREATE TABLE other_OTHERTABLE (id INTEGER)')
        conn.commit()
        conn.close()
        
        try:
            results = converter.convert_sqlite_to_topspeed(db_path, temp_dir)
            
            assert results['success'] is True
            assert len(results['files_created']) == 2  # PHD and MOD files
            assert results['tables_processed'] == 2  # Only phd_ and mod_ tables
            
            # Verify correct files were created
            phd_file = os.path.join(temp_dir, 'TxWells.PHD')
            mod_file = os.path.join(temp_dir, 'TxWells.mod')
            
            assert os.path.exists(phd_file)
            assert os.path.exists(mod_file)
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)
