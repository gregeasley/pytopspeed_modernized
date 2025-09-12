"""
Unit tests for ResilienceEnhancer class

Tests the resilience enhancement features including memory management,
adaptive batch sizing, safe data extraction, and size estimation.
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


class TestResilienceEnhancer:
    """Test cases for ResilienceEnhancer class"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.enhancer = ResilienceEnhancer(max_memory_mb=500, enable_progress_tracking=True)
    
    def test_initialization(self):
        """Test ResilienceEnhancer initialization"""
        enhancer = ResilienceEnhancer(max_memory_mb=1000, enable_progress_tracking=False)
        
        assert enhancer.max_memory_mb == 1000
        assert enhancer.enable_progress_tracking == False
        assert enhancer.logger is not None
    
    def test_initialization_defaults(self):
        """Test ResilienceEnhancer initialization with defaults"""
        enhancer = ResilienceEnhancer()
        
        assert enhancer.max_memory_mb == 500
        assert enhancer.enable_progress_tracking == True
    
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
    
    @patch('gc.collect')
    def test_force_memory_cleanup(self, mock_gc_collect):
        """Test forced memory cleanup"""
        with patch.object(self.enhancer.logger, 'debug') as mock_debug:
            self.enhancer.force_memory_cleanup()
            
            mock_gc_collect.assert_called_once()
            mock_debug.assert_called_once_with("Forced memory cleanup completed")
    
    @patch('gc.collect')
    def test_force_memory_cleanup_exception(self, mock_gc_collect):
        """Test forced memory cleanup with exception"""
        # Mock exception
        mock_gc_collect.side_effect = Exception("GC error")
        
        with patch.object(self.enhancer.logger, 'debug') as mock_debug:
            self.enhancer.force_memory_cleanup()
            
            mock_debug.assert_called_once()
            # Should not raise exception
    
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
        # Create large binary data (> 2000 bytes)
        raw_data = b"x" * 2500
        table_name = "LARGE_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        parsed = json.loads(result)
        
        assert parsed['data_size'] == 2500
        assert 'first_16_bytes' in parsed
        assert 'last_16_bytes' in parsed
        assert 'checksum' in parsed
    
    def test_create_compact_json_medium_data(self):
        """Test compact JSON creation for medium data"""
        # Create medium binary data (> 1000 bytes)
        raw_data = b"x" * 1500
        table_name = "MEDIUM_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        parsed = json.loads(result)
        
        assert parsed['data_size'] == 1500
        assert 'first_8_bytes' in parsed
        assert 'last_8_bytes' in parsed
        assert 'checksum' not in parsed
    
    def test_create_compact_json_small_data(self):
        """Test compact JSON creation for small data"""
        # Create small binary data (< 1000 bytes)
        raw_data = b"small data"
        table_name = "SMALL_TABLE"
        
        result = self.enhancer.create_compact_json(raw_data, table_name)
        
        parsed = json.loads(result)
        
        assert parsed['data_size'] == 10
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
                assert parsed['table'] == table_name
                mock_warning.assert_called_once()
    
    def test_estimate_table_size_no_table(self):
        """Test table size estimation when table not found"""
        # Mock TPS with no matching table
        tps = Mock()
        tps.tables._TpsTablesList__tables = {1: Mock(name="OTHER_TABLE")}
        
        result = self.enhancer.estimate_table_size(tps, "NONEXISTENT_TABLE")
        
        assert result['estimated_records'] == 0
        assert result['estimated_size_mb'] == 0
        assert result['recommendation'] == 'skip'
    
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
            assert 'recommendation' in result
            assert 'optimal_batch_size' in result
    
    def test_estimate_table_size_exception(self):
        """Test table size estimation with exception"""
        # Mock TPS that raises exception
        tps = Mock()
        tps.tables._TpsTablesList__tables = Mock(side_effect=Exception("TPS error"))
        
        with patch.object(self.enhancer.logger, 'debug') as mock_debug:
            result = self.enhancer.estimate_table_size(tps, "ERROR_TABLE")
            
            assert result['estimated_records'] == 0
            assert result['estimated_size_mb'] == 0
            assert result['recommendation'] == 'skip'
            mock_debug.assert_called_once()
    
    def test_generate_recommendation_skip(self):
        """Test recommendation generation for empty tables"""
        result = self.enhancer._generate_recommendation(0, 0)
        assert result == 'skip'
    
    def test_generate_recommendation_streaming_high_memory(self):
        """Test recommendation generation for very large tables"""
        result = self.enhancer._generate_recommendation(200000, 2000)
        assert result == 'streaming_high_memory'
    
    def test_generate_recommendation_streaming_medium_memory(self):
        """Test recommendation generation for large tables"""
        result = self.enhancer._generate_recommendation(75000, 750)
        assert result == 'streaming_medium_memory'
    
    def test_generate_recommendation_streaming_low_memory(self):
        """Test recommendation generation for medium tables"""
        result = self.enhancer._generate_recommendation(15000, 150)
        assert result == 'streaming_low_memory'
    
    def test_generate_recommendation_normal(self):
        """Test recommendation generation for small tables"""
        result = self.enhancer._generate_recommendation(5000, 50)
        assert result == 'normal'
    
    def test_get_optimal_batch_size_enterprise(self):
        """Test optimal batch size calculation for enterprise tables"""
        result = self.enhancer._get_optimal_batch_size(200000, 2000)
        assert result == 10
    
    def test_get_optimal_batch_size_large(self):
        """Test optimal batch size calculation for large tables"""
        result = self.enhancer._get_optimal_batch_size(75000, 750)
        assert result == 25
    
    def test_get_optimal_batch_size_medium(self):
        """Test optimal batch size calculation for medium tables"""
        result = self.enhancer._get_optimal_batch_size(15000, 150)
        assert result == 50
    
    def test_get_optimal_batch_size_small(self):
        """Test optimal batch size calculation for small tables"""
        result = self.enhancer._get_optimal_batch_size(5000, 50)
        assert result == 100
    
    def test_log_progress_enabled(self):
        """Test progress logging when enabled"""
        with patch.object(self.enhancer.logger, 'info') as mock_info:
            self.enhancer.log_progress(50, 100, "TEST_TABLE", "processing")
            
            mock_info.assert_called_once_with("Processing TEST_TABLE: 50/100 (50.0%)")
    
    def test_log_progress_disabled(self):
        """Test progress logging when disabled"""
        enhancer = ResilienceEnhancer(enable_progress_tracking=False)
        
        with patch.object(enhancer.logger, 'info') as mock_info:
            enhancer.log_progress(50, 100, "TEST_TABLE", "processing")
            
            mock_info.assert_not_called()
    
    def test_log_progress_no_total(self):
        """Test progress logging without total"""
        with patch.object(self.enhancer.logger, 'info') as mock_info:
            self.enhancer.log_progress(50, 0, "TEST_TABLE", "processing")
            
            mock_info.assert_called_once_with("Processing TEST_TABLE: 50 items")
    
    def test_should_use_streaming_large_records(self):
        """Test streaming decision for large records"""
        table_def = Mock()
        table_def.record_size = 3000
        table_def.field_count = 10
        
        estimated_size = {'estimated_records': 5000}
        
        result = self.enhancer.should_use_streaming(table_def, estimated_size)
        
        assert result == True
    
    def test_should_use_streaming_many_records(self):
        """Test streaming decision for many records"""
        table_def = Mock()
        table_def.record_size = 500
        table_def.field_count = 10
        
        estimated_size = {'estimated_records': 15000}
        
        result = self.enhancer.should_use_streaming(table_def, estimated_size)
        
        assert result == True
    
    def test_should_use_streaming_complex_table(self):
        """Test streaming decision for complex table"""
        table_def = Mock()
        table_def.record_size = 500
        table_def.field_count = 150
        
        estimated_size = {'estimated_records': 5000}
        
        result = self.enhancer.should_use_streaming(table_def, estimated_size)
        
        assert result == True
    
    def test_should_use_streaming_small_table(self):
        """Test streaming decision for small table"""
        table_def = Mock()
        table_def.record_size = 100
        table_def.field_count = 5
        
        estimated_size = {'estimated_records': 5000}
        
        result = self.enhancer.should_use_streaming(table_def, estimated_size)
        
        assert result == False
    
    def test_should_use_streaming_missing_attributes(self):
        """Test streaming decision with missing attributes"""
        table_def = Mock()
        # Don't set record_size or field_count
        table_def.record_size = None
        table_def.field_count = None
        
        estimated_size = {'estimated_records': 5000}
        
        result = self.enhancer.should_use_streaming(table_def, estimated_size)
        
        assert result == False
