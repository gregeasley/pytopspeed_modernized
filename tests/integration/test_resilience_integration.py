"""
Integration tests for resilience features

Tests the integration of resilience enhancements with the actual conversion
process, including end-to-end scenarios with various database sizes.
"""

import pytest
import tempfile
import os
import sqlite3
import json
import base64
from unittest.mock import Mock, patch, MagicMock
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from converter.phz_converter import PhzConverter
from converter.resilience_config import get_resilience_config
from converter.resilience_enhancements import ResilienceEnhancer


class TestResilienceIntegration:
    """Integration tests for resilience features"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.converter = PhzConverter()
        self.enhancer = ResilienceEnhancer(max_memory_mb=100, enable_progress_tracking=True)
    
    def test_small_database_conversion(self):
        """Test conversion of small database with small configuration"""
        # This test would require an actual small PHZ file
        # For now, we'll test the configuration selection
        
        config = get_resilience_config('small')
        
        assert config.max_memory_mb == 200
        assert config.default_batch_size == 200
        assert config.enable_streaming == False
        assert config.enable_parallel_processing == False
    
    def test_medium_database_conversion(self):
        """Test conversion of medium database with medium configuration"""
        config = get_resilience_config('medium')
        
        assert config.max_memory_mb == 500
        assert config.default_batch_size == 100
        assert config.enable_streaming == True
        assert config.streaming_threshold_records == 5000
    
    def test_large_database_conversion(self):
        """Test conversion of large database with large configuration"""
        config = get_resilience_config('large')
        
        assert config.max_memory_mb == 1000
        assert config.default_batch_size == 50
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == True
        assert config.enable_checkpointing == True
    
    def test_enterprise_database_conversion(self):
        """Test conversion of enterprise database with enterprise configuration"""
        config = get_resilience_config('enterprise')
        
        assert config.max_memory_mb == 2000
        assert config.default_batch_size == 25
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == True
        assert config.enable_checkpointing == True
        assert config.enable_resume_capability == True
    
    def test_adaptive_batch_sizing_integration(self):
        """Test adaptive batch sizing integration with different table types"""
        # Test different table characteristics
        test_cases = [
            # (record_size, field_count, expected_batch_size_range)
            (50, 10, (200, 400)),      # Small records
            (500, 25, (50, 100)),      # Medium records
            (2000, 50, (10, 50)),      # Large records
            (8000, 150, (5, 25)),      # Very large records
        ]
        
        for record_size, field_count, expected_range in test_cases:
            table_def = Mock()
            table_def.record_size = record_size
            table_def.field_count = field_count
            
            batch_size = self.enhancer.get_adaptive_batch_size(table_def)
            
            assert expected_range[0] <= batch_size <= expected_range[1], \
                f"Batch size {batch_size} not in expected range {expected_range} for record_size={record_size}, field_count={field_count}"
    
    def test_memory_monitoring_integration(self):
        """Test memory monitoring integration"""
        # Test memory monitoring with different limits
        test_limits = [100, 500, 1000, 2000]
        
        for limit in test_limits:
            enhancer = ResilienceEnhancer(max_memory_mb=limit)
            
            # Mock memory usage
            with patch('psutil.Process') as mock_process:
                # Test under limit
                mock_process.return_value.memory_info.return_value.rss = (limit - 50) * 1024 * 1024
                assert enhancer.check_memory_usage() == False
                
                # Test over limit
                mock_process.return_value.memory_info.return_value.rss = (limit + 50) * 1024 * 1024
                assert enhancer.check_memory_usage() == True
    
    def test_size_estimation_integration(self):
        """Test size estimation integration with mock TPS data"""
        # Mock TPS with realistic table structure
        tps = Mock()
        
        # Mock table
        table_mock = Mock()
        table_mock.name = "INTEGRATION_TEST_TABLE"
        tps.tables._TpsTablesList__tables = {1: table_mock}
        
        # Mock pages
        pages = []
        for i in range(20):  # 20 pages
            page = Mock()
            page.hierarchy_level = 0
            pages.append((i, page))
        
        tps.pages.list.return_value = [p[0] for p in pages]
        # Set up __getitem__ method for pages
        tps.pages.__getitem__ = Mock(side_effect=lambda x: next(p[1] for p in pages if p[0] == x))
        
        # Mock records (5 records per page)
        with patch('pytopspeed.tpsrecord.TpsRecordsList') as mock_records_list:
            mock_records = []
            for i in range(5):
                record = Mock()
                record.type = 'DATA'
                record.data.table_number = 1
                mock_records.append(record)
            
            mock_records_list.return_value = mock_records
            
            result = self.enhancer.estimate_table_size(tps, "INTEGRATION_TEST_TABLE")
            
            # Should estimate 100 records (5 per page * 20 pages)
            assert result['estimated_records'] == 100
            assert result['estimated_size_mb'] > 0
            assert result['sample_pages'] == 20  # All 20 pages sampled (up to max of 20)
            assert result['total_pages'] == 20
            assert 'recommendation' in result
            assert 'optimal_batch_size' in result
    
    def test_compact_json_creation_integration(self):
        """Test compact JSON creation with various data sizes"""
        test_cases = [
            # (data_size, expected_fields)
            (50, ['raw_data', 'data_size', 'table']),                    # Small data
            (500, ['raw_data', 'data_size', 'table', 'first_4_bytes']),  # Medium data
            (1500, ['raw_data', 'data_size', 'table', 'first_8_bytes', 'last_8_bytes']),  # Large data
            (2500, ['raw_data', 'data_size', 'table', 'first_16_bytes', 'last_16_bytes', 'checksum']),  # Very large data
        ]
        
        for data_size, expected_fields in test_cases:
            raw_data = b"x" * data_size
            table_name = f"TEST_TABLE_{data_size}"
            
            result = self.enhancer.create_compact_json(raw_data, table_name)
            parsed = json.loads(result)
            
            # Check that all expected fields are present
            for field in expected_fields:
                assert field in parsed, f"Field {field} missing for data size {data_size}"
            
            # Check data integrity
            assert parsed['data_size'] == data_size
            assert parsed['table'] == table_name
            assert base64.b64decode(parsed['raw_data']) == raw_data
    
    def test_streaming_decision_integration(self):
        """Test streaming decision integration with various scenarios"""
        test_cases = [
            # (table_def_attrs, estimated_size, expected_streaming)
            ({'record_size': 100, 'field_count': 5}, {'estimated_records': 5000}, False),      # Small table
            ({'record_size': 100, 'field_count': 5}, {'estimated_records': 15000}, True),     # Many records
            ({'record_size': 3000, 'field_count': 10}, {'estimated_records': 5000}, True),    # Large records
            ({'record_size': 500, 'field_count': 150}, {'estimated_records': 5000}, True),    # Complex table
            ({'record_size': 500, 'field_count': 150}, {'estimated_records': 5000}, True),    # Complex table
        ]
        
        for table_def_attrs, estimated_size, expected_streaming in test_cases:
            table_def = Mock()
            for attr, value in table_def_attrs.items():
                setattr(table_def, attr, value)
            
            result = self.enhancer.should_use_streaming(table_def, estimated_size)
            
            assert result == expected_streaming, \
                f"Streaming decision incorrect for {table_def_attrs}, {estimated_size}"
    
    def test_progress_logging_integration(self):
        """Test progress logging integration"""
        # Test with progress tracking enabled
        enhancer_enabled = ResilienceEnhancer(enable_progress_tracking=True)
        
        with patch.object(enhancer_enabled.logger, 'info') as mock_info:
            enhancer_enabled.log_progress(50, 100, "TEST_TABLE", "processing")
            mock_info.assert_called_once_with("Processing TEST_TABLE: 50/100 (50.0%)")
        
        # Test with progress tracking disabled
        enhancer_disabled = ResilienceEnhancer(enable_progress_tracking=False)
        
        with patch.object(enhancer_disabled.logger, 'info') as mock_info:
            enhancer_disabled.log_progress(50, 100, "TEST_TABLE", "processing")
            mock_info.assert_not_called()
    
    def test_error_recovery_integration(self):
        """Test error recovery integration with various failure scenarios"""
        # Test raw data extraction with various failure modes
        # Create exception case - use a simple object that doesn't have the 'data' attribute
        class ExceptionRecord:
            def __init__(self):
                pass  # No 'data' attribute
        
        exception_mock = ExceptionRecord()
        
        test_cases = [
            # (record_mock, expected_result)
            (Mock(data=Mock(data=Mock(data=b"success"))), b"success"),  # Success case
            (Mock(data=Mock(data=b"fallback")), b"fallback"),           # Fallback case
            (Mock(data=b"direct"), b"direct"),                         # Direct case
            (exception_mock, None),  # Exception case
        ]
        
        for record_mock, expected_result in test_cases:
            result = self.enhancer.extract_raw_data_safe(record_mock)
            assert result == expected_result
    
    def test_configuration_consistency_integration(self):
        """Test that configurations are consistent and logical"""
        configs = ['small', 'medium', 'large', 'enterprise']
        
        for i, config_name in enumerate(configs):
            config = get_resilience_config(config_name)
            
            # Memory limits should increase with database size
            if i > 0:
                prev_config = get_resilience_config(configs[i-1])
                assert config.max_memory_mb >= prev_config.max_memory_mb
            
            # Batch sizes should generally decrease with database size
            if i > 0:
                prev_config = get_resilience_config(configs[i-1])
                assert config.default_batch_size <= prev_config.default_batch_size
            
            # Streaming should be enabled for larger databases
            if config_name in ['medium', 'large', 'enterprise']:
                assert config.enable_streaming == True
            
            # Parallel processing should be enabled for larger databases
            if config_name in ['large', 'enterprise']:
                assert config.enable_parallel_processing == True
            
            # Checkpointing should be enabled for larger databases
            if config_name in ['large', 'enterprise']:
                assert config.enable_checkpointing == True
    
    def test_database_size_category_estimation_integration(self):
        """Test database size category estimation integration"""
        from converter.resilience_config import estimate_database_size_category
        
        test_cases = [
            # (size_mb, records, expected_category)
            (50, 5000, 'small'),
            (500, 50000, 'medium'),
            (5000, 500000, 'large'),
            (15000, 1500000, 'enterprise'),
            (101, 10001, 'medium'),  # Boundary case (just above thresholds)
            (1001, 100001, 'large'),  # Boundary case (just above thresholds)
            (10001, 1000001, 'enterprise'),  # Boundary case (just above thresholds)
        ]
        
        for size_mb, records, expected_category in test_cases:
            category = estimate_database_size_category(size_mb, records)
            assert category == expected_category, \
                f"Category estimation incorrect for {size_mb}MB, {records} records"
    
    def test_memory_cleanup_integration(self):
        """Test memory cleanup integration"""
        with patch('gc.collect') as mock_gc_collect:
            self.enhancer.force_memory_cleanup()
            mock_gc_collect.assert_called_once()
    
    def test_recommendation_generation_integration(self):
        """Test recommendation generation integration"""
        test_cases = [
            # (records, size_mb, expected_recommendation)
            (0, 0, 'skip'),
            (5000, 50, 'normal'),
            (15000, 150, 'streaming_low_memory'),
            (75000, 750, 'streaming_medium_memory'),
            (200000, 2000, 'streaming_high_memory'),
        ]
        
        for records, size_mb, expected_recommendation in test_cases:
            recommendation = self.enhancer._generate_recommendation(records, size_mb)
            assert recommendation == expected_recommendation, \
                f"Recommendation incorrect for {records} records, {size_mb}MB"
    
    def test_optimal_batch_size_calculation_integration(self):
        """Test optimal batch size calculation integration"""
        test_cases = [
            # (records, size_mb, expected_batch_size)
            (5000, 50, 100),
            (15000, 150, 50),
            (75000, 750, 25),
            (200000, 2000, 10),
        ]
        
        for records, size_mb, expected_batch_size in test_cases:
            batch_size = self.enhancer._get_optimal_batch_size(records, size_mb)
            assert batch_size == expected_batch_size, \
                f"Optimal batch size incorrect for {records} records, {size_mb}MB"
