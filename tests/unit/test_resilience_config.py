"""
Unit tests for ResilienceConfig class

Tests the configuration management for resilience features including
predefined configurations, size estimation, and configuration validation.
"""

import pytest
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from converter.resilience_config import (
    ResilienceConfig, 
    RESILIENCE_CONFIGS, 
    get_resilience_config, 
    estimate_database_size_category
)


class TestResilienceConfig:
    """Test cases for ResilienceConfig class"""
    
    def test_default_initialization(self):
        """Test ResilienceConfig initialization with defaults"""
        config = ResilienceConfig()
        
        # Test memory management defaults
        assert config.max_memory_mb == 500
        assert config.memory_cleanup_interval == 1000
        assert config.enable_memory_monitoring == True
        
        # Test batch processing defaults
        assert config.default_batch_size == 100
        assert config.adaptive_batch_sizing == True
        assert config.max_batch_size == 1000
        assert config.min_batch_size == 5
        
        # Test progress tracking defaults
        assert config.enable_progress_tracking == True
        assert config.progress_log_interval == 100
        assert config.detailed_progress_logging == False
        
        # Test error handling defaults
        assert config.max_consecutive_errors == 100
        assert config.enable_partial_conversion == True
        assert config.skip_problematic_tables == False
        
        # Test performance optimization defaults
        assert config.enable_streaming == True
        assert config.streaming_threshold_records == 10000
        assert config.enable_parallel_processing == False
        assert config.max_worker_threads == 4
        
        # Test database optimization defaults
        assert config.sqlite_journal_mode == "WAL"
        assert config.sqlite_synchronous == "NORMAL"
        assert config.sqlite_cache_size == -2000
        assert config.sqlite_temp_store == "MEMORY"
        
        # Test large table handling defaults
        assert config.large_table_threshold_records == 50000
        assert config.large_table_threshold_mb == 500.0
        assert config.enable_table_size_estimation == True
        
        # Test recovery and resumption defaults
        assert config.enable_checkpointing == False
        assert config.checkpoint_interval == 10000
        assert config.enable_resume_capability == False
    
    def test_custom_initialization(self):
        """Test ResilienceConfig initialization with custom values"""
        config = ResilienceConfig(
            max_memory_mb=1000,
            default_batch_size=50,
            enable_streaming=False,
            sqlite_journal_mode="DELETE"
        )
        
        assert config.max_memory_mb == 1000
        assert config.default_batch_size == 50
        assert config.enable_streaming == False
        assert config.sqlite_journal_mode == "DELETE"
        
        # Other values should remain default
        assert config.memory_cleanup_interval == 1000
        assert config.enable_parallel_processing == False
    
    def test_to_dict(self):
        """Test conversion to dictionary"""
        config = ResilienceConfig(
            max_memory_mb=750,
            default_batch_size=75,
            enable_streaming=True
        )
        
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert config_dict['max_memory_mb'] == 750
        assert config_dict['default_batch_size'] == 75
        assert config_dict['enable_streaming'] == True
        assert config_dict['memory_cleanup_interval'] == 1000  # Default value
    
    def test_from_dict(self):
        """Test creation from dictionary"""
        config_dict = {
            'max_memory_mb': 800,
            'default_batch_size': 60,
            'enable_streaming': False,
            'sqlite_journal_mode': 'TRUNCATE'
        }
        
        config = ResilienceConfig.from_dict(config_dict)
        
        assert config.max_memory_mb == 800
        assert config.default_batch_size == 60
        assert config.enable_streaming == False
        assert config.sqlite_journal_mode == 'TRUNCATE'
        
        # Other values should be default
        assert config.memory_cleanup_interval == 1000
        assert config.enable_parallel_processing == False
    
    def test_for_small_databases(self):
        """Test small database configuration"""
        config = ResilienceConfig.for_small_databases()
        
        assert config.max_memory_mb == 200
        assert config.default_batch_size == 200
        assert config.enable_streaming == False
        assert config.enable_parallel_processing == False
        assert config.detailed_progress_logging == False
    
    def test_for_medium_databases(self):
        """Test medium database configuration"""
        config = ResilienceConfig.for_medium_databases()
        
        assert config.max_memory_mb == 500
        assert config.default_batch_size == 100
        assert config.enable_streaming == True
        assert config.streaming_threshold_records == 5000
        assert config.enable_parallel_processing == False
    
    def test_for_large_databases(self):
        """Test large database configuration"""
        config = ResilienceConfig.for_large_databases()
        
        assert config.max_memory_mb == 1000
        assert config.default_batch_size == 50
        assert config.adaptive_batch_sizing == True
        assert config.enable_streaming == True
        assert config.streaming_threshold_records == 1000
        assert config.enable_parallel_processing == True
        assert config.max_worker_threads == 2
        assert config.enable_checkpointing == True
        assert config.checkpoint_interval == 5000
        assert config.detailed_progress_logging == True
    
    def test_for_enterprise_databases(self):
        """Test enterprise database configuration"""
        config = ResilienceConfig.for_enterprise_databases()
        
        assert config.max_memory_mb == 2000
        assert config.default_batch_size == 25
        assert config.adaptive_batch_sizing == True
        assert config.enable_streaming == True
        assert config.streaming_threshold_records == 500
        assert config.enable_parallel_processing == True
        assert config.max_worker_threads == 4
        assert config.enable_checkpointing == True
        assert config.checkpoint_interval == 1000
        assert config.enable_resume_capability == True
        assert config.detailed_progress_logging == True
        assert config.sqlite_cache_size == -10000
        assert config.large_table_threshold_records == 100000
        assert config.large_table_threshold_mb == 1000.0


class TestResilienceConfigs:
    """Test cases for predefined configurations"""
    
    def test_resilience_configs_contains_all_categories(self):
        """Test that all configuration categories are available"""
        expected_categories = ['small', 'medium', 'large', 'enterprise', 'default']
        
        for category in expected_categories:
            assert category in RESILIENCE_CONFIGS
            assert isinstance(RESILIENCE_CONFIGS[category], ResilienceConfig)
    
    def test_small_config_characteristics(self):
        """Test small configuration characteristics"""
        config = RESILIENCE_CONFIGS['small']
        
        # Should be optimized for small databases
        assert config.max_memory_mb <= 500
        assert config.default_batch_size >= 100
        assert config.enable_streaming == False
        assert config.enable_parallel_processing == False
    
    def test_medium_config_characteristics(self):
        """Test medium configuration characteristics"""
        config = RESILIENCE_CONFIGS['medium']
        
        # Should be balanced for medium databases
        assert config.max_memory_mb >= 200
        assert config.max_memory_mb <= 1000
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == False
    
    def test_large_config_characteristics(self):
        """Test large configuration characteristics"""
        config = RESILIENCE_CONFIGS['large']
        
        # Should be optimized for large databases
        assert config.max_memory_mb >= 500
        assert config.default_batch_size <= 100
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == True
        assert config.enable_checkpointing == True
    
    def test_enterprise_config_characteristics(self):
        """Test enterprise configuration characteristics"""
        config = RESILIENCE_CONFIGS['enterprise']
        
        # Should be optimized for enterprise databases
        assert config.max_memory_mb >= 1000
        assert config.default_batch_size <= 50
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == True
        assert config.enable_checkpointing == True
        assert config.enable_resume_capability == True
        assert config.sqlite_cache_size <= -5000  # Large cache
    
    def test_default_config_characteristics(self):
        """Test default configuration characteristics"""
        config = RESILIENCE_CONFIGS['default']
        
        # Should be balanced defaults
        assert config.max_memory_mb == 500
        assert config.default_batch_size == 100
        assert config.enable_streaming == True
        assert config.enable_parallel_processing == False


class TestGetResilienceConfig:
    """Test cases for get_resilience_config function"""
    
    def test_get_valid_config(self):
        """Test getting valid configuration"""
        config = get_resilience_config('small')
        
        assert isinstance(config, ResilienceConfig)
        assert config.max_memory_mb == 200
    
    def test_get_all_valid_configs(self):
        """Test getting all valid configurations"""
        valid_configs = ['small', 'medium', 'large', 'enterprise', 'default']
        
        for config_name in valid_configs:
            config = get_resilience_config(config_name)
            assert isinstance(config, ResilienceConfig)
    
    def test_get_invalid_config(self):
        """Test getting invalid configuration"""
        with pytest.raises(ValueError) as exc_info:
            get_resilience_config('invalid')
        
        assert "Unknown configuration" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)
    
    def test_get_config_case_sensitive(self):
        """Test that configuration names are case sensitive"""
        with pytest.raises(ValueError):
            get_resilience_config('SMALL')  # Should be lowercase
    
    def test_get_config_empty_string(self):
        """Test getting configuration with empty string"""
        with pytest.raises(ValueError):
            get_resilience_config('')


class TestEstimateDatabaseSizeCategory:
    """Test cases for estimate_database_size_category function"""
    
    def test_estimate_small_database(self):
        """Test estimation for small databases"""
        # Small size and record count
        category = estimate_database_size_category(50.0, 5000)
        assert category == 'small'
        
        # Very small size
        category = estimate_database_size_category(5.0, 10000)
        assert category == 'small'
        
        # Very small record count
        category = estimate_database_size_category(50.0, 1000)
        assert category == 'small'
    
    def test_estimate_medium_database(self):
        """Test estimation for medium databases"""
        # Medium size and record count
        category = estimate_database_size_category(500.0, 50000)
        assert category == 'medium'
        
        # Medium size, small record count
        category = estimate_database_size_category(500.0, 5000)
        assert category == 'medium'
        
        # Small size, medium record count
        category = estimate_database_size_category(50.0, 50000)
        assert category == 'medium'
    
    def test_estimate_large_database(self):
        """Test estimation for large databases"""
        # Large size and record count
        category = estimate_database_size_category(5000.0, 500000)
        assert category == 'large'
        
        # Large size, medium record count
        category = estimate_database_size_category(5000.0, 50000)
        assert category == 'large'
        
        # Medium size, large record count
        category = estimate_database_size_category(500.0, 500000)
        assert category == 'large'
    
    def test_estimate_enterprise_database(self):
        """Test estimation for enterprise databases"""
        # Enterprise size and record count
        category = estimate_database_size_category(15000.0, 1500000)
        assert category == 'enterprise'
        
        # Enterprise size, large record count
        category = estimate_database_size_category(15000.0, 500000)
        assert category == 'enterprise'
        
        # Large size, enterprise record count
        category = estimate_database_size_category(5000.0, 1500000)
        assert category == 'enterprise'
    
    def test_estimate_boundary_cases(self):
        """Test estimation for boundary cases"""
        # Exactly at boundaries (using > not >=)
        category = estimate_database_size_category(100.0, 10000)
        assert category == 'small'  # 100 is not > 100, so small
        
        category = estimate_database_size_category(1000.0, 100000)
        assert category == 'medium'  # 1000 is not > 1000, and 100000 is not > 100000, so medium
        
        category = estimate_database_size_category(10000.0, 1000000)
        assert category == 'large'  # 10000 > 1000, so large
    
    def test_estimate_zero_values(self):
        """Test estimation with zero values"""
        category = estimate_database_size_category(0.0, 0)
        assert category == 'small'
    
    def test_estimate_negative_values(self):
        """Test estimation with negative values"""
        category = estimate_database_size_category(-100.0, -1000)
        assert category == 'small'  # Should handle gracefully
    
    def test_estimate_very_large_values(self):
        """Test estimation with very large values"""
        category = estimate_database_size_category(100000.0, 10000000)
        assert category == 'enterprise'
    
    def test_estimate_float_precision(self):
        """Test estimation with float precision"""
        # Test with precise float values
        category = estimate_database_size_category(99.999, 9999)
        assert category == 'small'
        
        category = estimate_database_size_category(100.001, 10001)
        assert category == 'medium'


class TestConfigurationValidation:
    """Test cases for configuration validation and edge cases"""
    
    def test_config_with_extreme_values(self):
        """Test configuration with extreme values"""
        config = ResilienceConfig(
            max_memory_mb=0,  # Very low memory
            default_batch_size=1,  # Very small batch
            max_batch_size=1000000,  # Very large max batch
            min_batch_size=0  # Very small min batch
        )
        
        assert config.max_memory_mb == 0
        assert config.default_batch_size == 1
        assert config.max_batch_size == 1000000
        assert config.min_batch_size == 0
    
    def test_config_with_negative_values(self):
        """Test configuration with negative values"""
        config = ResilienceConfig(
            max_memory_mb=-100,
            default_batch_size=-50,
            memory_cleanup_interval=-1000
        )
        
        # Should accept negative values (validation would be in usage)
        assert config.max_memory_mb == -100
        assert config.default_batch_size == -50
        assert config.memory_cleanup_interval == -1000
    
    def test_config_sqlite_settings(self):
        """Test SQLite-specific configuration settings"""
        config = ResilienceConfig(
            sqlite_journal_mode="OFF",
            sqlite_synchronous="FULL",
            sqlite_cache_size=5000,
            sqlite_temp_store="FILE"
        )
        
        assert config.sqlite_journal_mode == "OFF"
        assert config.sqlite_synchronous == "FULL"
        assert config.sqlite_cache_size == 5000
        assert config.sqlite_temp_store == "FILE"
    
    def test_config_boolean_settings(self):
        """Test boolean configuration settings"""
        config = ResilienceConfig(
            enable_memory_monitoring=False,
            adaptive_batch_sizing=False,
            enable_progress_tracking=False,
            detailed_progress_logging=True,
            enable_partial_conversion=False,
            skip_problematic_tables=True,
            enable_streaming=False,
            enable_parallel_processing=True,
            enable_table_size_estimation=False,
            enable_checkpointing=True,
            enable_resume_capability=True
        )
        
        assert config.enable_memory_monitoring == False
        assert config.adaptive_batch_sizing == False
        assert config.enable_progress_tracking == False
        assert config.detailed_progress_logging == True
        assert config.enable_partial_conversion == False
        assert config.skip_problematic_tables == True
        assert config.enable_streaming == False
        assert config.enable_parallel_processing == True
        assert config.enable_table_size_estimation == False
        assert config.enable_checkpointing == True
        assert config.enable_resume_capability == True
