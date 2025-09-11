"""
Configuration settings for resilience features

This module provides configuration options for handling large databases
with various resilience and performance optimizations.
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class ResilienceConfig:
    """
    Configuration for resilience features during conversion
    """
    
    # Memory management
    max_memory_mb: int = 500
    memory_cleanup_interval: int = 1000  # Cleanup every N records
    enable_memory_monitoring: bool = True
    
    # Batch processing
    default_batch_size: int = 100
    adaptive_batch_sizing: bool = True
    max_batch_size: int = 1000
    min_batch_size: int = 5
    
    # Progress tracking
    enable_progress_tracking: bool = True
    progress_log_interval: int = 100  # Log progress every N records
    detailed_progress_logging: bool = False
    
    # Error handling
    max_consecutive_errors: int = 100
    enable_partial_conversion: bool = True
    skip_problematic_tables: bool = False
    
    # Performance optimization
    enable_streaming: bool = True
    streaming_threshold_records: int = 10000
    enable_parallel_processing: bool = False
    max_worker_threads: int = 4
    
    # Database optimization
    sqlite_journal_mode: str = "WAL"  # WAL, DELETE, TRUNCATE, PERSIST, MEMORY, OFF
    sqlite_synchronous: str = "NORMAL"  # OFF, NORMAL, FULL
    sqlite_cache_size: int = -2000  # Negative value = KB, positive = pages
    sqlite_temp_store: str = "MEMORY"  # DEFAULT, FILE, MEMORY
    
    # Large table handling
    large_table_threshold_records: int = 50000
    large_table_threshold_mb: float = 500.0
    enable_table_size_estimation: bool = True
    
    # Recovery and resumption
    enable_checkpointing: bool = False
    checkpoint_interval: int = 10000  # Create checkpoint every N records
    enable_resume_capability: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'max_memory_mb': self.max_memory_mb,
            'memory_cleanup_interval': self.memory_cleanup_interval,
            'enable_memory_monitoring': self.enable_memory_monitoring,
            'default_batch_size': self.default_batch_size,
            'adaptive_batch_sizing': self.adaptive_batch_sizing,
            'max_batch_size': self.max_batch_size,
            'min_batch_size': self.min_batch_size,
            'enable_progress_tracking': self.enable_progress_tracking,
            'progress_log_interval': self.progress_log_interval,
            'detailed_progress_logging': self.detailed_progress_logging,
            'max_consecutive_errors': self.max_consecutive_errors,
            'enable_partial_conversion': self.enable_partial_conversion,
            'skip_problematic_tables': self.skip_problematic_tables,
            'enable_streaming': self.enable_streaming,
            'streaming_threshold_records': self.streaming_threshold_records,
            'enable_parallel_processing': self.enable_parallel_processing,
            'max_worker_threads': self.max_worker_threads,
            'sqlite_journal_mode': self.sqlite_journal_mode,
            'sqlite_synchronous': self.sqlite_synchronous,
            'sqlite_cache_size': self.sqlite_cache_size,
            'sqlite_temp_store': self.sqlite_temp_store,
            'large_table_threshold_records': self.large_table_threshold_records,
            'large_table_threshold_mb': self.large_table_threshold_mb,
            'enable_table_size_estimation': self.enable_table_size_estimation,
            'enable_checkpointing': self.enable_checkpointing,
            'checkpoint_interval': self.checkpoint_interval,
            'enable_resume_capability': self.enable_resume_capability
        }
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'ResilienceConfig':
        """Create configuration from dictionary"""
        return cls(**config_dict)
    
    @classmethod
    def for_small_databases(cls) -> 'ResilienceConfig':
        """Configuration optimized for small databases (< 10MB)"""
        return cls(
            max_memory_mb=200,
            default_batch_size=200,
            enable_streaming=False,
            enable_parallel_processing=False,
            detailed_progress_logging=False
        )
    
    @classmethod
    def for_medium_databases(cls) -> 'ResilienceConfig':
        """Configuration optimized for medium databases (10MB - 1GB)"""
        return cls(
            max_memory_mb=500,
            default_batch_size=100,
            enable_streaming=True,
            streaming_threshold_records=5000,
            enable_parallel_processing=False
        )
    
    @classmethod
    def for_large_databases(cls) -> 'ResilienceConfig':
        """Configuration optimized for large databases (> 1GB)"""
        return cls(
            max_memory_mb=1000,
            default_batch_size=50,
            adaptive_batch_sizing=True,
            enable_streaming=True,
            streaming_threshold_records=1000,
            enable_parallel_processing=True,
            max_worker_threads=2,
            enable_checkpointing=True,
            checkpoint_interval=5000,
            detailed_progress_logging=True
        )
    
    @classmethod
    def for_enterprise_databases(cls) -> 'ResilienceConfig':
        """Configuration optimized for enterprise-scale databases (> 10GB)"""
        return cls(
            max_memory_mb=2000,
            default_batch_size=25,
            adaptive_batch_sizing=True,
            enable_streaming=True,
            streaming_threshold_records=500,
            enable_parallel_processing=True,
            max_worker_threads=4,
            enable_checkpointing=True,
            checkpoint_interval=1000,
            enable_resume_capability=True,
            detailed_progress_logging=True,
            sqlite_cache_size=-10000,  # 10MB cache
            large_table_threshold_records=100000,
            large_table_threshold_mb=1000.0
        )


# Predefined configurations for common use cases
RESILIENCE_CONFIGS = {
    'small': ResilienceConfig.for_small_databases(),
    'medium': ResilienceConfig.for_medium_databases(),
    'large': ResilienceConfig.for_large_databases(),
    'enterprise': ResilienceConfig.for_enterprise_databases(),
    'default': ResilienceConfig()  # Default configuration
}


def get_resilience_config(config_name: str = 'default') -> ResilienceConfig:
    """
    Get a predefined resilience configuration
    
    Args:
        config_name: Name of the configuration ('small', 'medium', 'large', 'enterprise', 'default')
        
    Returns:
        ResilienceConfig object
        
    Raises:
        ValueError: If config_name is not recognized
    """
    if config_name not in RESILIENCE_CONFIGS:
        raise ValueError(f"Unknown configuration: {config_name}. Available: {list(RESILIENCE_CONFIGS.keys())}")
    
    return RESILIENCE_CONFIGS[config_name]


def estimate_database_size_category(estimated_size_mb: float, estimated_records: int) -> str:
    """
    Estimate the appropriate configuration category based on database size
    
    Args:
        estimated_size_mb: Estimated database size in MB
        estimated_records: Estimated total number of records
        
    Returns:
        Configuration category name
    """
    if estimated_size_mb > 10000 or estimated_records > 1000000:
        return 'enterprise'
    elif estimated_size_mb > 1000 or estimated_records > 100000:
        return 'large'
    elif estimated_size_mb > 100 or estimated_records > 10000:
        return 'medium'
    else:
        return 'small'
