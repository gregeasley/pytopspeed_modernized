"""
Unit tests for SQLite converter components
"""

import pytest
import sqlite3
import tempfile
import os
import json
from unittest.mock import Mock, MagicMock
from converter.sqlite_converter import SqliteConverter
from converter.multidimensional_handler import ArrayFieldInfo


class TestSqliteConverterInitialization:
    """Test SqliteConverter initialization"""
    
    def test_converter_initialization_default(self):
        """Test converter initialization with default parameters"""
        converter = SqliteConverter()
        
        assert converter is not None
        assert converter.batch_size == 1000
        assert converter.progress_callback is None
        assert converter.schema_mapper is not None
        assert converter.logger is not None
    
    def test_converter_initialization_custom(self):
        """Test converter initialization with custom parameters"""
        def dummy_callback(current, total, message):
            pass
        
        converter = SqliteConverter(batch_size=500, progress_callback=dummy_callback)
        
        assert converter.batch_size == 500
        assert converter.progress_callback == dummy_callback
        assert converter.schema_mapper is not None
        assert converter.logger is not None


class TestFieldValueConversion:
    """Test field value conversion functionality"""
    
    def test_convert_field_value_string(self):
        """Test string field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        # Test STRING type
        field = MockField('STRING')
        assert converter._convert_field_value(field, "test string") == "test string"
        assert converter._convert_field_value(field, b"test bytes") == "test bytes"
        assert converter._convert_field_value(field, None) is None
        
        # Test with null bytes (rstrip only removes from end)
        assert converter._convert_field_value(field, "test\x00string") == "test\x00string"
        assert converter._convert_field_value(field, b"test\x00bytes") == "test\x00bytes"
    
    def test_convert_field_value_numeric(self):
        """Test numeric field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        # Test integer types
        for int_type in ['BYTE', 'SHORT', 'USHORT', 'LONG', 'ULONG']:
            field = MockField(int_type)
            assert converter._convert_field_value(field, 123) == 123
            assert converter._convert_field_value(field, "456") == 456
            assert converter._convert_field_value(field, None) is None
        
        # Test float types
        for float_type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
            field = MockField(float_type)
            assert converter._convert_field_value(field, 123.45) == 123.45
            assert converter._convert_field_value(field, "456.78") == 456.78
            assert converter._convert_field_value(field, None) is None
    
    def test_convert_field_value_date(self):
        """Test date field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        field = MockField('DATE')
        
        # Test valid date (0xYYYYMMDD format)
        # Example: 0x20231225 = 2023-12-25
        date_value = (2023 << 16) | (12 << 8) | 25
        result = converter._convert_field_value(field, date_value)
        assert result == "2023-12-25"
        
        # Test invalid date
        assert converter._convert_field_value(field, 0) is None
        assert converter._convert_field_value(field, None) is None
    
    def test_convert_field_value_time(self):
        """Test time field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        field = MockField('TIME')
        
        # Test valid time (0xHHMMSSHS format)
        # Example: 0x14300000 = 14:30:00
        time_value = (20 << 24) | (30 << 16) | (45 << 8) | 0
        result = converter._convert_field_value(field, time_value)
        assert result == "20:30:45"
        
        # Test invalid time
        assert converter._convert_field_value(field, 0) is None
        assert converter._convert_field_value(field, None) is None
    
    def test_convert_field_value_group(self):
        """Test group (binary) field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        field = MockField('GROUP')
        
        # Test binary data
        binary_data = b"binary data"
        assert converter._convert_field_value(field, binary_data) == binary_data
        assert converter._convert_field_value(field, None) is None
        assert converter._convert_field_value(field, "string") is None
    
    def test_convert_field_value_default(self):
        """Test default field value conversion"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, type):
                self.type = type
        
        field = MockField('UNKNOWN_TYPE')
        
        # Test default conversion to string
        assert converter._convert_field_value(field, 123) == "123"
        assert converter._convert_field_value(field, 123.45) == "123.45"
        assert converter._convert_field_value(field, None) is None


class TestRecordConversion:
    """Test record to tuple conversion"""
    
    def test_convert_record_to_tuple_dict(self):
        """Test converting dict record to tuple"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, name, type):
                self.name = name
                self.type = type
        
        class MockTableDef:
            def __init__(self):
                self.fields = [
                    MockField("FIELD1", "STRING"),
                    MockField("FIELD2", "LONG")
                ]
                self.memos = []
        
        record = {
            "FIELD1": "test value",
            "FIELD2": 123,
            "OTHER_FIELD": "ignored"
        }
        
        table_def = MockTableDef()
        result = converter._convert_record_to_tuple(record, table_def)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == "test value"
        assert result[1] == 123
    
    def test_convert_record_to_tuple_object(self):
        """Test converting object record to tuple"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, name, type):
                self.name = name
                self.type = type
        
        class MockTableDef:
            def __init__(self):
                self.fields = [
                    MockField("FIELD1", "STRING"),
                    MockField("FIELD2", "LONG")
                ]
                self.memos = []
        
        class MockRecord:
            def __init__(self):
                self.FIELD1 = "test value"
                self.FIELD2 = 123
        
        record = MockRecord()
        table_def = MockTableDef()
        result = converter._convert_record_to_tuple(record, table_def)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == "test value"
        assert result[1] == 123
    
    def test_convert_record_to_tuple_with_memos(self):
        """Test converting record with memo fields"""
        converter = SqliteConverter()
        
        class MockField:
            def __init__(self, name, type):
                self.name = name
                self.type = type
        
        class MockMemo:
            def __init__(self, name):
                self.name = name
        
        class MockTableDef:
            def __init__(self):
                self.fields = [MockField("FIELD1", "STRING")]
                self.memos = [MockMemo("MEMO1")]
        
        record = {
            "FIELD1": "test value",
            "MEMO1": "memo content"
        }
        
        table_def = MockTableDef()
        result = converter._convert_record_to_tuple(record, table_def)
        
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == "test value"
        assert result[1] == "memo content"


class TestSchemaCreation:
    """Test SQLite schema creation"""
    
    def test_create_schema_basic(self, sample_tps, temp_sqlite_db):
        """Test basic schema creation"""
        converter = SqliteConverter()
        
        conn = sqlite3.connect(temp_sqlite_db)
        try:
            table_mapping = converter._create_schema(sample_tps, conn)
            
            assert isinstance(table_mapping, dict)
            assert len(table_mapping) > 0
            
            # Check that tables were created
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            assert table_count > 0
            
            # Check that indexes were created
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
            index_count = cursor.fetchone()[0]
            assert index_count > 0
            
        finally:
            conn.close()
    
    def test_create_schema_table_mapping(self, sample_tps, temp_sqlite_db):
        """Test that table mapping is correct"""
        converter = SqliteConverter()
        
        conn = sqlite3.connect(temp_sqlite_db)
        try:
            table_mapping = converter._create_schema(sample_tps, conn)
            
            # Check that mapping contains expected tables
            assert len(table_mapping) > 0
            
            # Check that all mapped tables exist in database
            cursor = conn.cursor()
            for original_name, sanitized_name in table_mapping.items():
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (sanitized_name,))
                result = cursor.fetchone()
                assert result is not None, f"Table {sanitized_name} not found in database"
                
        finally:
            conn.close()


class TestDataMigration:
    """Test data migration functionality"""
    
    def test_migrate_table_data_basic(self, sample_tps, sample_table_name, temp_sqlite_db):
        """Test basic table data migration"""
        converter = SqliteConverter()
        
        # First create schema
        conn = sqlite3.connect(temp_sqlite_db)
        try:
            table_mapping = converter._create_schema(sample_tps, conn)
            
            if sample_table_name in table_mapping:
                sanitized_name = table_mapping[sample_table_name]
                
                # Migrate data
                record_count = converter._migrate_table_data(
                    sample_tps, sample_table_name, sanitized_name, conn
                )
                
                assert record_count >= 0
                
                # Check that data was inserted
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {sanitized_name}")
                db_count = cursor.fetchone()[0]
                assert db_count == record_count
                
        finally:
            conn.close()
    
    def test_migrate_table_data_empty_table(self, sample_tps, temp_sqlite_db):
        """Test migration of empty table"""
        converter = SqliteConverter()
        
        # Find an empty table
        empty_table_name = None
        for table_number in sample_tps.tables._TpsTablesList__tables:
            table = sample_tps.tables._TpsTablesList__tables[table_number]
            if table.name and table.name != '':
                sample_tps.set_current_table(table.name)
                record_count = sum(1 for _ in sample_tps)
                if record_count == 0:
                    empty_table_name = table.name
                    break
        
        if empty_table_name:
            conn = sqlite3.connect(temp_sqlite_db)
            try:
                table_mapping = converter._create_schema(sample_tps, conn)
                
                if empty_table_name in table_mapping:
                    sanitized_name = table_mapping[empty_table_name]
                    
                    # Migrate data
                    record_count = converter._migrate_table_data(
                        sample_tps, empty_table_name, sanitized_name, conn
                    )
                    
                    assert record_count == 0
                    
            finally:
                conn.close()


class TestProgressCallback:
    """Test progress callback functionality"""
    
    def test_progress_callback_invocation(self):
        """Test that progress callback is invoked correctly"""
        callback_invocations = []
        
        def progress_callback(current, total, message):
            callback_invocations.append((current, total, message))
        
        converter = SqliteConverter(progress_callback=progress_callback)
        
        # Test progress update
        converter._update_progress(10, 100, "Test message")
        
        assert len(callback_invocations) == 1
        assert callback_invocations[0] == (10, 100, "Test message")
    
    def test_progress_callback_no_callback(self):
        """Test progress update without callback"""
        converter = SqliteConverter()
        
        # Should not raise exception
        converter._update_progress(10, 100, "Test message")


class TestErrorHandling:
    """Test error handling in converter"""
    
    def test_convert_nonexistent_file(self):
        """Test conversion of nonexistent file"""
        converter = SqliteConverter()
        
        results = converter.convert("nonexistent.phd", "output.sqlite")
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert results['tables_created'] == 0
        assert results['total_records'] == 0
    
    def test_convert_invalid_file(self, temp_sqlite_db):
        """Test conversion of invalid file"""
        converter = SqliteConverter()
        
        # Create a temporary invalid file
        with tempfile.NamedTemporaryFile(suffix='.phd', delete=False) as tmp:
            tmp.write(b"invalid data")
            invalid_file = tmp.name
        
        try:
            results = converter.convert(invalid_file, temp_sqlite_db)
            
            assert results['success'] is False
            assert len(results['errors']) > 0
            
        finally:
            os.unlink(invalid_file)
    
    def test_migrate_table_data_error_handling(self, sample_tps, temp_sqlite_db):
        """Test error handling in table data migration"""
        converter = SqliteConverter()
        
        # Test with invalid table name
        record_count = converter._migrate_table_data(
            sample_tps, "INVALID_TABLE", "INVALID_TABLE", 
            sqlite3.connect(temp_sqlite_db)
        )
        
        assert record_count == 0


class TestMultidimensionalConversion:
    """Test multidimensional array conversion functionality"""
    
    def test_convert_multidimensional_record_to_tuple_single_field_array(self):
        """Test conversion of single-field array records"""
        converter = SqliteConverter()
        
        # Mock array field info
        array_info = ArrayFieldInfo(
            base_name="TEST:BOOLPARAM",
            element_type="BYTE",
            element_size=1,
            array_size=3,
            start_offset=0,
            element_offsets=[0, 1, 2],
            is_single_field_array=True
        )
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        # Mock record object with raw data (how raw TPS records are represented)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x01\x00\x01'  # Raw bytes: True, False, True
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        assert result[0] == json.dumps([True, False, True])
    
    def test_convert_multidimensional_record_to_tuple_mixed_fields(self):
        """Test conversion of records with both arrays and regular fields"""
        converter = SqliteConverter()
        
        # Mock array field info
        array_info = ArrayFieldInfo(
            base_name="TEST:BOOLPARAM",
            element_type="BYTE",
            element_size=1,
            array_size=2,
            start_offset=2,
            element_offsets=[2, 3],
            is_single_field_array=True
        )
        
        # Mock regular field
        regular_field = Mock()
        regular_field.name = "TEST:ID"
        regular_field.type = "SHORT"
        regular_field.offset = 0
        regular_field.size = 2
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': [regular_field]
        }
        
        # Mock record object with raw data (SHORT ID + 2 BYTE array)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x01\x00\x01\x00'  # ID=1, BOOLPARAM=[True, False]
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 2
        assert result[0] == 1  # ID field
        assert result[1] == json.dumps([True, False])  # BOOLPARAM array
    
    def test_convert_multidimensional_record_to_tuple_boolean_parsing(self):
        """Test boolean parsing in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock array field info for BYTE array
        array_info = ArrayFieldInfo(
            base_name="TEST:BOOLPARAM",
            element_type="BYTE",
            element_size=1,
            array_size=4,
            start_offset=0,
            element_offsets=[0, 1, 2, 3],
            is_single_field_array=True
        )
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock record object with raw data (4 bytes: True, False, True, False)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x01\x00\x01\x00'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        assert result[0] == json.dumps([True, False, True, False])
    
    def test_convert_multidimensional_record_to_tuple_numeric_arrays(self):
        """Test numeric array parsing in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock array field info for DOUBLE array
        array_info = ArrayFieldInfo(
            base_name="TEST:REALPARAM",
            element_type="DOUBLE",
            element_size=8,
            array_size=2,
            start_offset=0,
            element_offsets=[0, 8],
            is_single_field_array=True
        )
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock record object with raw data (2 doubles: 1.0, 2.0)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x00\x00\x00\x00\x00\x00\xf0\x3f' + b'\x00\x00\x00\x00\x00\x00\x00\x40'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        parsed_array = json.loads(result[0])
        assert len(parsed_array) == 2
        assert abs(parsed_array[0] - 1.0) < 0.0001
        assert abs(parsed_array[1] - 2.0) < 0.0001
    
    def test_convert_multidimensional_record_to_tuple_string_arrays(self):
        """Test string array parsing in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock array field info for STRING array
        array_info = ArrayFieldInfo(
            base_name="TEST:NAMES",
            element_type="STRING",
            element_size=10,
            array_size=2,
            start_offset=0,
            element_offsets=[0, 10],
            is_single_field_array=True
        )
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock record object with raw data (2 strings: "Hello", "World")
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'Hello\x00\x00\x00\x00\x00' + b'World\x00\x00\x00\x00\x00'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        assert result[0] == json.dumps(["Hello", "World"])
    
    def test_convert_multidimensional_record_to_tuple_insufficient_data(self):
        """Test handling of insufficient data in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock array field info
        array_info = ArrayFieldInfo(
            base_name="TEST:BOOLPARAM",
            element_type="BYTE",
            element_size=1,
            array_size=3,
            start_offset=0,
            element_offsets=[0, 1, 2],
            is_single_field_array=True
        )
        
        # Mock analysis
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock insufficient raw record data (only 2 bytes for 3-byte array)
        # Mock record object with raw data (only 2 bytes for 3-byte array)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x01\x00'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        # Should handle insufficient data gracefully
        parsed_array = json.loads(result[0])
        assert len(parsed_array) == 3  # All 3 elements, but last one is None due to insufficient data
        assert parsed_array == [True, False, None]
    
    def test_convert_multidimensional_record_to_tuple_no_arrays(self):
        """Test conversion when no arrays are present"""
        converter = SqliteConverter()
        
        # Mock analysis with no arrays
        analysis = {
            'has_arrays': False,
            'array_fields': [],
            'regular_fields': []
        }
        
        # Mock record object with raw data
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'test data'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert result == ()  # Returns empty tuple when no arrays or regular fields
    
    def test_convert_multidimensional_record_to_tuple_regular_field_boolean(self):
        """Test boolean parsing for regular fields in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock regular field
        regular_field = Mock()
        regular_field.name = "TEST:FLAG"
        regular_field.type = "BYTE"
        regular_field.offset = 0
        regular_field.size = 1
        
        # Mock analysis
        analysis = {
            'has_arrays': False,
            'array_fields': [],
            'regular_fields': [regular_field]
        }
        
        # Mock record object with raw data (1 byte: True)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'\x01'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        assert result[0] is True  # Should be converted to boolean
    
    def test_convert_multidimensional_record_to_tuple_regular_field_string(self):
        """Test string parsing for regular fields in multidimensional records"""
        converter = SqliteConverter()
        
        # Mock regular field
        regular_field = Mock()
        regular_field.name = "TEST:NAME"
        regular_field.type = "STRING"
        regular_field.offset = 0
        regular_field.size = 10
        
        # Mock analysis
        analysis = {
            'has_arrays': False,
            'array_fields': [],
            'regular_fields': [regular_field]
        }
        
        # Mock record object with raw data (string with null terminator)
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = b'Hello\x00\x00\x00\x00\x00'
        
        # Mock table definition
        mock_table_def = Mock()
        mock_table_def.fields = []
        mock_table_def.memos = []
        
        result = converter._convert_multidimensional_record_to_tuple(
            mock_record, mock_table_def, analysis
        )
        
        assert len(result) == 1
        assert result[0] == "Hello"  # Should be converted to string with null stripping
