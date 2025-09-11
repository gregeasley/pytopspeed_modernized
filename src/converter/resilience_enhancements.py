"""
Resilience enhancements for large database conversions

This module provides additional resilience features for handling very large
TopSpeed databases that may exceed normal memory and processing limits.
"""

import json
import base64
import gc
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ResilienceEnhancer:
    """
    Provides resilience enhancements for large database conversions
    """
    
    def __init__(self, max_memory_mb: int = 500, enable_progress_tracking: bool = True):
        """
        Initialize resilience enhancer
        
        Args:
            max_memory_mb: Maximum memory usage in MB before cleanup
            enable_progress_tracking: Whether to enable detailed progress tracking
        """
        self.max_memory_mb = max_memory_mb
        self.enable_progress_tracking = enable_progress_tracking
        self.logger = logger
    
    def get_adaptive_batch_size(self, table_def) -> int:
        """
        Calculate adaptive batch size based on table characteristics
        
        Args:
            table_def: Table definition object
            
        Returns:
            Optimal batch size for this table
        """
        # Base batch size
        base_batch_size = 100
        
        # Adjust based on record size
        if hasattr(table_def, 'record_size') and table_def.record_size is not None:
            record_size = table_def.record_size
            if record_size > 10000:  # Very large records (>10KB)
                return max(5, base_batch_size // 20)
            elif record_size > 5000:  # Large records (>5KB)
                return max(10, base_batch_size // 10)
            elif record_size > 1000:  # Medium-large records (>1KB)
                return max(25, base_batch_size // 4)
            elif record_size < 100:  # Small records (<100B)
                return base_batch_size * 4
        
        # Adjust based on field count
        if hasattr(table_def, 'field_count') and table_def.field_count is not None:
            field_count = table_def.field_count
            if field_count > 200:  # Extremely complex tables
                return max(5, base_batch_size // 20)
            elif field_count > 100:  # Very complex tables
                return max(10, base_batch_size // 10)
            elif field_count > 50:  # Complex tables
                return max(25, base_batch_size // 2)
        
        return base_batch_size
    
    def check_memory_usage(self) -> bool:
        """
        Check if memory usage exceeds the specified limit
        
        Returns:
            True if memory limit exceeded, False otherwise
        """
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > self.max_memory_mb:
                self.logger.warning(f"Memory usage {memory_mb:.1f}MB exceeds limit {self.max_memory_mb}MB")
                return True
            return False
        except ImportError:
            # psutil not available, skip memory checking
            return False
        except Exception as e:
            self.logger.debug(f"Memory check failed: {e}")
            return False
    
    def force_memory_cleanup(self):
        """Force garbage collection to free memory"""
        try:
            gc.collect()
            self.logger.debug("Forced memory cleanup completed")
        except Exception as e:
            self.logger.debug(f"Memory cleanup failed: {e}")
    
    def extract_raw_data_safe(self, record) -> Optional[bytes]:
        """
        Safely extract raw data from a record with multiple fallback methods
        
        Args:
            record: TPS record object
            
        Returns:
            Raw data bytes or None if extraction fails
        """
        try:
            # Try multiple extraction methods in order of preference
            if hasattr(record.data.data, 'data'):
                return record.data.data.data
            elif hasattr(record.data, 'data'):
                return record.data.data
            elif hasattr(record, 'data'):
                return record.data
            else:
                return None
        except Exception as e:
            self.logger.debug(f"Raw data extraction failed: {e}")
            return None
    
    def create_compact_json(self, raw_data: bytes, table_name: str) -> str:
        """
        Create a compact JSON representation of raw data
        
        Args:
            raw_data: Raw binary data
            table_name: Name of the table for context
            
        Returns:
            JSON string representation
        """
        try:
            parsed_data = {
                'raw_data': base64.b64encode(raw_data).decode('ascii'),
                'data_size': len(raw_data),
                'table': table_name
            }
            
            # Add metadata for very large records
            if len(raw_data) > 2000:  # Very large records
                parsed_data['first_16_bytes'] = raw_data[:16].hex()
                parsed_data['last_16_bytes'] = raw_data[-16:].hex()
                parsed_data['checksum'] = hash(raw_data) & 0xFFFFFFFF  # Simple checksum
            elif len(raw_data) > 1000:  # Large records
                parsed_data['first_8_bytes'] = raw_data[:8].hex()
                parsed_data['last_8_bytes'] = raw_data[-8:].hex()
            elif len(raw_data) >= 4:  # Medium records
                parsed_data['first_4_bytes'] = raw_data[:4].hex()
            
            return json.dumps(parsed_data, separators=(',', ':'))  # Compact JSON
        except Exception as e:
            self.logger.warning(f"JSON creation failed: {e}")
            # Fallback to simple representation
            return json.dumps({
                'error': 'data_processing_failed', 
                'size': len(raw_data),
                'table': table_name
            })
    
    def estimate_table_size(self, tps, table_name: str) -> Dict[str, Any]:
        """
        Estimate table size and characteristics for optimization
        
        Args:
            tps: TopSpeed file object
            table_name: Name of the table
            
        Returns:
            Dictionary with size estimates and recommendations
        """
        try:
            # Get table number
            table_number = None
            for num, table in tps.tables._TpsTablesList__tables.items():
                if table.name == table_name:
                    table_number = num
                    break
            
            if table_number is None:
                return {
                    'estimated_records': 0, 
                    'estimated_size_mb': 0,
                    'recommendation': 'skip'
                }
            
            # Count records in first few pages to estimate total
            sample_pages = 0
            sample_records = 0
            total_pages = len([p for p in tps.pages.list() if tps.pages[p].hierarchy_level == 0])
            
            # Sample up to 20 pages for better estimation
            max_sample_pages = min(20, total_pages)
            
            for page_ref in tps.pages.list():
                if tps.pages[page_ref].hierarchy_level == 0:
                    sample_pages += 1
                    if sample_pages > max_sample_pages:
                        break
                    
                    try:
                        from pytopspeed.tpsrecord import TpsRecordsList
                        records = TpsRecordsList(tps, tps.pages[page_ref], encoding='cp1251', check=True)
                        
                        for record in records:
                            if record.type == 'DATA':
                                record_table_number = None
                                if hasattr(record.data, 'table_number'):
                                    record_table_number = record.data.table_number
                                elif hasattr(record, 'table_number'):
                                    record_table_number = record.table_number
                                
                                if record_table_number == table_number:
                                    sample_records += 1
                    except Exception:
                        continue
            
            # Estimate total records
            if sample_pages > 0 and sample_records > 0:
                records_per_page = sample_records / sample_pages
                estimated_records = int(records_per_page * total_pages)
            else:
                estimated_records = 0
            
            # Estimate size (rough calculation based on typical record sizes)
            estimated_size_mb = (estimated_records * 3) / 1024  # Assume ~3KB per record average
            
            # Generate recommendations
            recommendation = self._generate_recommendation(estimated_records, estimated_size_mb)
            
            return {
                'estimated_records': estimated_records,
                'estimated_size_mb': estimated_size_mb,
                'sample_pages': sample_pages,
                'total_pages': total_pages,
                'recommendation': recommendation,
                'optimal_batch_size': self._get_optimal_batch_size(estimated_records, estimated_size_mb)
            }
            
        except Exception as e:
            self.logger.debug(f"Table size estimation failed for {table_name}: {e}")
            return {
                'estimated_records': 0, 
                'estimated_size_mb': 0,
                'recommendation': 'skip'
            }
    
    def _generate_recommendation(self, estimated_records: int, estimated_size_mb: float) -> str:
        """
        Generate processing recommendation based on table size
        
        Args:
            estimated_records: Estimated number of records
            estimated_size_mb: Estimated size in MB
            
        Returns:
            Recommendation string
        """
        if estimated_records == 0:
            return 'skip'
        elif estimated_records > 100000 or estimated_size_mb > 1000:
            return 'streaming_high_memory'
        elif estimated_records > 50000 or estimated_size_mb > 500:
            return 'streaming_medium_memory'
        elif estimated_records > 10000 or estimated_size_mb > 100:
            return 'streaming_low_memory'
        else:
            return 'normal'
    
    def _get_optimal_batch_size(self, estimated_records: int, estimated_size_mb: float) -> int:
        """
        Calculate optimal batch size based on estimates
        
        Args:
            estimated_records: Estimated number of records
            estimated_size_mb: Estimated size in MB
            
        Returns:
            Optimal batch size
        """
        if estimated_records > 100000 or estimated_size_mb > 1000:
            return 10  # Very small batches for huge tables
        elif estimated_records > 50000 or estimated_size_mb > 500:
            return 25  # Small batches for large tables
        elif estimated_records > 10000 or estimated_size_mb > 100:
            return 50  # Medium batches for medium tables
        else:
            return 100  # Normal batches for small tables
    
    def log_progress(self, current: int, total: int, table_name: str, operation: str = "processing"):
        """
        Log progress information for large operations
        
        Args:
            current: Current progress
            total: Total items to process
            table_name: Name of the table being processed
            operation: Type of operation being performed
        """
        if not self.enable_progress_tracking:
            return
        
        if total > 0:
            percentage = (current / total) * 100
            self.logger.info(f"{operation.capitalize()} {table_name}: {current}/{total} ({percentage:.1f}%)")
        else:
            self.logger.info(f"{operation.capitalize()} {table_name}: {current} items")
    
    def should_use_streaming(self, table_def, estimated_size: Dict[str, Any]) -> bool:
        """
        Determine if streaming processing should be used for this table
        
        Args:
            table_def: Table definition object
            estimated_size: Size estimation results
            
        Returns:
            True if streaming should be used
        """
        # Use streaming for large tables
        if estimated_size['estimated_records'] > 10000:
            return True
        
        # Use streaming for tables with large record sizes
        if hasattr(table_def, 'record_size') and table_def.record_size is not None and table_def.record_size > 2000:
            return True
        
        # Use streaming for complex tables
        if hasattr(table_def, 'field_count') and table_def.field_count is not None and table_def.field_count > 100:
            return True
        
        return False
