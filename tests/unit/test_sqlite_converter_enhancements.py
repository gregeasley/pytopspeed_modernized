"""
Unit tests for enhanced SQLite converter methods

Tests the resilience enhancements added to the SQLite converter including
adaptive batch sizing, memory management, and enhanced table definition parsing.
"""

import pytest
import json
import base64
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from converter.resilience_enhancements import ResilienceEnhancer
from converter.sqlite_converter import SqliteConverter


class TestSqliteConverterEnhancements:
    """Test cases for enhanced SQLite converter methods"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.enhancer = ResilienceEnhancer()
        self.converter = SqliteConverter()
    
    def test_get_adaptive_batch_size_small_records(self):
        """Test adaptive batch sizing for small records"""
        # Mock table definition with small records
        table_def = Mock()
        table_def.record_size = 50
        table_def.field_count = 10
        
        batch_size = self.enhancer.get_adaptive_batch_size(table_def)
        
        # Small records should get larger batch sizes
        assert batch_size > 100
        assert batch_size == 400  # 100 * 4
    
    def test_get_adaptive_batch_size_large_records(self):
        """Test adaptive batch sizing for large records"""
        # Mock table definition with large records
        table_def = Mock()
        table_def.record_size = 6000
        table_def.field_count = 10
        
        batch_size = self.enhancer.get_adaptive_batch_size(table_def)
        
        # Large records should get smaller batch sizes
        assert batch_size < 100
        assert batch_size == 10  # max(10, 100 // 10)
    
    def test_get_adaptive_batch_size_very_large_records(self):
        """Test adaptive batch sizing for very large records"""
        # Mock table definition with very large records
        table_def = Mock()
        table_def.record_size = 12000
        table_def.field_count = 10
        
        batch_size = self.enhancer.get_adaptive_batch_size(table_def)
        
        # Very large records should get very small batch sizes
        assert batch_size == 5  # max(5, 100 // 20)
    
    def test_get_adaptive_batch_size_complex_tables(self):
        """Test adaptive batch sizing for complex tables"""
        # Mock table definition with many fields
        table_def = Mock()
        table_def.record_size = 500
        table_def.field_count = 150
        
        batch_size = self.enhancer.get_adaptive_batch_size(table_def)
        
        # Complex tables should get smaller batch sizes
        assert batch_size < 100
        assert batch_size == 10  # max(10, 100 // 10) - field_count=150 triggers field_count > 100 condition
    
    def test_get_adaptive_batch_size_no_attributes(self):
        """Test adaptive batch sizing with missing attributes"""
        # Mock table definition without size attributes
        table_def = Mock()
        # Don't set record_size or field_count
        table_def.record_size = None
        table_def.field_count = None
        
        batch_size = self.enhancer.get_adaptive_batch_size(table_def)
        
        # Should return default batch size
        assert batch_size == 100
    
    @patch('psutil.Process')
    def test_check_memory_usage_under_limit(self, mock_process):
        """Test memory usage check when under limit"""
        # Mock process with low memory usage
        mock_process.return_value.memory_info.return_value.rss = 200 * 1024 * 1024  # 200MB
        
        result = self.enhancer.check_memory_usage()
        
        assert result == False  # Not over limit
    
    @patch('psutil.Process')
    def test_check_memory_usage_over_limit(self, mock_process):
        """Test memory usage check when over limit"""
        # Mock process with high memory usage
        mock_process.return_value.memory_info.return_value.rss = 600 * 1024 * 1024  # 600MB
        
        with patch.object(self.enhancer.logger, 'warning') as mock_warning:
            result = self.enhancer.check_memory_usage()
            
            assert result == True  # Over limit
            mock_warning.assert_called_once()
    
    @patch('psutil.Process')
    def test_check_memory_usage_psutil_not_available(self, mock_process):
        """Test memory usage check when psutil is not available"""
        # Mock ImportError
        mock_process.side_effect = ImportError("psutil not available")
        
        result = self.enhancer.check_memory_usage()
        
        assert result == False  # Should return False when psutil unavailable
    
    @patch('psutil.Process')
    def test_check_memory_usage_exception(self, mock_process):
        """Test memory usage check when exception occurs"""
        # Mock exception
        mock_process.side_effect = Exception("Process error")
        
        with patch.object(self.enhancer.logger, 'debug') as mock_debug:
            result = self.enhancer.check_memory_usage()
            
            assert result == False  # Should return False on exception
            mock_debug.assert_called_once()
    
    def test_extract_raw_data_safe_success(self):
        """Test successful raw data extraction"""
        # Mock record with data
        record = Mock()
        record.data.data.data = b"test data"
        
        result = self.enhancer.extract_raw_data_safe(record)
        
        assert result == b"test data"
    
    def test_extract_raw_data_safe_fallback_methods(self):
        """Test raw data extraction with fallback methods"""
        # Mock record with data in different location
        record = Mock()
        record.data.data.data = None  # First method fails
        record.data.data = b"fallback data"
        
        result = self.enhancer.extract_raw_data_safe(record)
        
        assert result == b"fallback data"
    
    def test_extract_raw_data_safe_no_data(self):
        """Test raw data extraction when no data available"""
        # Mock record with no data
        record = Mock()
        record.data.data.data = None
        record.data.data = None
        record.data = None
        
        result = self.enhancer.extract_raw_data_safe(record)
        
        assert result is None
    
    def test_extract_raw_data_safe_exception(self):
        """Test raw data extraction with exception"""
        # Create a simple object that doesn't have the expected attributes
        class ExceptionRecord:
            def __init__(self):
                pass  # No 'data' attribute
        
        record = ExceptionRecord()
        
        result = self.enhancer.extract_raw_data_safe(record)
        
        assert result is None
    
    def test_create_compact_json_success(self):
        """Test successful compact JSON creation"""
        raw_data = b"test binary data"
        table_name = "TEST_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        # Parse the JSON to verify structure
        parsed = json.loads(result)
        
        assert parsed['data_size'] == len(raw_data)
        assert parsed['table'] == table_name
        assert 'raw_data' in parsed
        assert base64.b64decode(parsed['raw_data']) == raw_data
    
    def test_create_compact_json_large_data(self):
        """Test compact JSON creation for large data"""
        # Create large binary data (> 1000 bytes)
        raw_data = b"x" * 1500
        table_name = "LARGE_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        parsed = json.loads(result)
        
        assert parsed['data_size'] == 1500
        assert 'first_8_bytes' in parsed
        assert 'last_8_bytes' in parsed
    
    def test_create_compact_json_medium_data(self):
        """Test compact JSON creation for medium data"""
        # Create medium binary data (< 1000 bytes)
        raw_data = b"x" * 500
        table_name = "MEDIUM_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        parsed = json.loads(result)
        
        assert parsed['data_size'] == 500
        assert 'first_4_bytes' in parsed
        assert 'first_8_bytes' not in parsed
        assert 'last_8_bytes' not in parsed
    
    def test_create_compact_json_exception(self):
        """Test compact JSON creation with exception"""
        # Mock base64 to raise exception
        with patch('base64.b64encode', side_effect=Exception("Base64 error")):
            raw_data = b"test data"
            table_name = "ERROR_TABLE"
            
            with patch.object(self.enhancer.logger, 'warning') as mock_warning:
                result = self.enhancer.create_compact_json(raw_data, table_name)
                
                parsed = json.loads(result)
                assert parsed['error'] == 'data_processing_failed'
                assert parsed['size'] == len(raw_data)
                mock_warning.assert_called_once()
    
    def test_estimate_table_size_no_table(self):
        """Test table size estimation when table not found"""
        # Mock TPS with no matching table
        tps = Mock()
        tps.tables._TpsTablesList__tables = {1: Mock(name="OTHER_TABLE")}
        
        result = self.enhancer.estimate_table_size(tps, "NONEXISTENT_TABLE")
        
        assert result['estimated_records'] == 0
        assert result['estimated_size_mb'] == 0
    
    def test_estimate_table_size_success(self):
        """Test successful table size estimation"""
        # Mock TPS with matching table
        tps = Mock()
        table_mock = Mock()
        table_mock.name = "TEST_TABLE"
        tps.tables._TpsTablesList__tables = {1: table_mock}
        
        # Mock pages
        page1 = Mock()
        page2 = Mock()
        page1.hierarchy_level = 0
        page2.hierarchy_level = 0
        tps.pages.list.return_value = [1, 2]
        tps.pages.__getitem__ = Mock(side_effect=lambda x: page1 if x == 1 else page2)
        
        # Mock records
        record1 = Mock()
        record1.type = 'DATA'
        record1.data.table_number = 1
        
        record2 = Mock()
        record2.type = 'DATA'
        record2.data.table_number = 1
        
        with patch('pytopspeed.tpsrecord.TpsRecordsList') as mock_records_list:
            mock_records_list.return_value = [record1, record2]
            
            result = self.enhancer.estimate_table_size(tps, "TEST_TABLE")
            
            assert result['estimated_records'] > 0
            assert result['estimated_size_mb'] > 0
            assert 'sample_pages' in result
            assert 'total_pages' in result
    
    def test_estimate_table_size_exception(self):
        """Test table size estimation with exception"""
        # Mock TPS that raises exception
        tps = Mock()
        tps.tables._TpsTablesList__tables = Mock(side_effect=Exception("TPS error"))
        
        with patch.object(self.enhancer.logger, 'debug') as mock_debug:
            result = self.enhancer.estimate_table_size(tps, "ERROR_TABLE")
            
            assert result['estimated_records'] == 0
            assert result['estimated_size_mb'] == 0
            mock_debug.assert_called_once()
    
    def test_create_enhanced_table_definition_success(self):
        """Test successful enhanced table definition creation"""
        table_name = "TEST_TABLE"
        definition_bytes = {
            0: b'\x01\x00\x00\x10\x29\x00\x00\x00\x00\x00',  # 10 bytes with field_count=41
            1: b'\x00' * 500,  # 500 bytes
            2: b'\x00' * 15   # 15 bytes
        }
        
        result = self.converter._create_enhanced_table_definition(table_name, definition_bytes)
        
        assert result is not None
        assert result.name == table_name
        assert result.field_count == 41
        assert result.record_size == 4096  # 0x1000
        assert result.memo_count == 0
        assert result.index_count == 0
        assert len(result.fields) > 0
    
    def test_create_enhanced_table_definition_large_array(self):
        """Test enhanced table definition for large array table"""
        table_name = "LARGE_ARRAY_TABLE"
        definition_bytes = {
            0: b'\x01\x00\x00\x10\x65\x00\x00\x00\x00\x00',  # field_count=101 (>50)
            1: b'\x00' * 500,
            2: b'\x00' * 15
        }
        
        result = self.converter._create_enhanced_table_definition(table_name, definition_bytes)
        
        assert result is not None
        assert result.field_count == 101
        # Should create the correct number of fields (101) for large arrays
        assert len(result.fields) == 101
        # All fields should be created with proper names
        for i, field in enumerate(result.fields):
            assert field.name is not None
            assert field.name != ""
            assert hasattr(field, 'type')
            assert hasattr(field, 'size')
            assert hasattr(field, 'offset')
    
    def test_create_enhanced_table_definition_insufficient_data(self):
        """Test enhanced table definition with insufficient data"""
        table_name = "INSUFFICIENT_TABLE"
        definition_bytes = {
            0: b'\x01\x00'  # Only 2 bytes, insufficient
        }
        
        result = self.converter._create_enhanced_table_definition(table_name, definition_bytes)
        
        assert result is None
    
    def test_create_enhanced_table_definition_exception(self):
        """Test enhanced table definition with exception"""
        table_name = "ERROR_TABLE"
        definition_bytes = {
            0: b'\x01\x00\x00\x10\x29\x00\x00\x00\x00\x00',
            1: b'\x00' * 500,
            2: b'\x00' * 15
        }
        
        # Mock struct.unpack to raise exception
        with patch('struct.unpack', side_effect=Exception("Struct error")):
            with patch.object(self.converter.logger, 'warning') as mock_warning:
                result = self.converter._create_enhanced_table_definition(table_name, definition_bytes)
                
                assert result is None
                mock_warning.assert_called_once()
    
    def test_get_table_definition_robust_success(self):
        """Test robust table definition retrieval with success"""
        tps = Mock()
        table_number = 1
        table_name = "SUCCESS_TABLE"
        
        # Mock successful table definition
        mock_table_def = Mock()
        mock_table_def.fields = [Mock(), Mock()]
        tps.tables.get_definition.return_value = mock_table_def
        
        result = self.converter._get_table_definition_robust(tps, table_number, table_name)
        
        assert result == mock_table_def
        tps.tables.get_definition.assert_called_once_with(table_number)
    
    def test_get_table_definition_robust_enhanced_fallback(self):
        """Test robust table definition with enhanced fallback"""
        tps = Mock()
        table_number = 1
        table_name = "ENHANCED_TABLE"
        
        # Mock failed table definition
        tps.tables.get_definition.side_effect = Exception("Parse error")
        
        # Mock table with definition bytes
        mock_table = Mock()
        mock_table.definition_bytes = {
            0: b'\x01\x00\x00\x10\x29\x00\x00\x00\x00\x00',
            1: b'\x00' * 500,
            2: b'\x00' * 15
        }
        tps.tables._TpsTablesList__tables = {table_number: mock_table}
        
        # Mock enhanced table definition creation
        mock_enhanced_def = Mock()
        mock_enhanced_def.fields = [Mock()]
        
        with patch.object(self.converter, '_create_enhanced_table_definition', return_value=mock_enhanced_def):
            with patch.object(self.converter.logger, 'warning') as mock_warning:
                with patch.object(self.converter.logger, 'info') as mock_info:
                    result = self.converter._get_table_definition_robust(tps, table_number, table_name)
                    
                    assert result == mock_enhanced_def
                    mock_warning.assert_called_once()
                    mock_info.assert_called()
    
    def test_get_table_definition_robust_minimal_fallback(self):
        """Test robust table definition with minimal fallback"""
        tps = Mock()
        table_number = 1
        table_name = "MINIMAL_TABLE"
        
        # Mock failed table definition
        tps.tables.get_definition.side_effect = Exception("Parse error")
        
        # Mock table without definition bytes
        mock_table = Mock()
        mock_table.definition_bytes = {}
        tps.tables._TpsTablesList__tables = {table_number: mock_table}
        
        # Mock failed enhanced table definition creation
        with patch.object(self.converter, '_create_enhanced_table_definition', return_value=None):
            with patch.object(self.converter.logger, 'warning') as mock_warning:
                with patch.object(self.converter.logger, 'info') as mock_info:
                    result = self.converter._get_table_definition_robust(tps, table_number, table_name)
                    
                    assert result is not None
                    assert result.name == table_name
                    assert result.fields == []
                    assert result.memos == []
                    assert result.indexes == []
                    assert result.record_size == 0
                    assert result.field_count == 0
                    assert result.memo_count == 0
                    assert result.index_count == 0
                    mock_warning.assert_called()
                    mock_info.assert_called()
    
    def test_get_table_definition_robust_enhanced_exception(self):
        """Test robust table definition with enhanced creation exception"""
        tps = Mock()
        table_number = 1
        table_name = "EXCEPTION_TABLE"
        
        # Mock failed table definition
        tps.tables.get_definition.side_effect = Exception("Parse error")
        
        # Mock table with definition bytes
        mock_table = Mock()
        mock_table.definition_bytes = {
            0: b'\x01\x00\x00\x10\x29\x00\x00\x00\x00\x00',
            1: b'\x00' * 500,
            2: b'\x00' * 15
        }
        tps.tables._TpsTablesList__tables = {table_number: mock_table}
        
        # Mock enhanced table definition creation exception
        with patch.object(self.converter, '_create_enhanced_table_definition', side_effect=Exception("Enhanced error")):
            with patch.object(self.converter.logger, 'warning') as mock_warning:
                with patch.object(self.converter.logger, 'info') as mock_info:
                    result = self.converter._get_table_definition_robust(tps, table_number, table_name)
                    
                    assert result is not None
                    assert result.name == table_name
                    assert result.fields == []
                    mock_warning.assert_called()
                    mock_info.assert_called()
