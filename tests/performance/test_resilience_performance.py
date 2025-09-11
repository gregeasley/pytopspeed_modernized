"""
Performance tests for resilience features

Tests the performance characteristics of resilience enhancements including
memory usage, processing speed, and scalability under various conditions.
"""

import pytest
import time
import gc
import psutil
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from converter.resilience_enhancements import ResilienceEnhancer
from converter.resilience_config import get_resilience_config


class TestResiliencePerformance:
    """Performance tests for resilience features"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.enhancer = ResilienceEnhancer(max_memory_mb=100, enable_progress_tracking=False)
    
    def test_memory_usage_small_batches(self):
        """Test memory usage with small batch sizes"""
        # Create large amount of test data
        test_data = []
        for i in range(1000):
            test_data.append(b"x" * 1000)  # 1KB per item
        
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Process in small batches
        batch_size = 10
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            # Simulate processing
            _ = [self.enhancer.create_compact_json(data, f"TEST_TABLE_{i}") for data in batch]
            
            # Force cleanup every 10 batches
            if i % (batch_size * 10) == 0:
                self.enhancer.force_memory_cleanup()
        
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 50MB for this test)
        assert memory_increase < 50, f"Memory increase too high: {memory_increase}MB"
    
    def test_memory_usage_large_batches(self):
        """Test memory usage with large batch sizes"""
        # Create large amount of test data
        test_data = []
        for i in range(1000):
            test_data.append(b"x" * 1000)  # 1KB per item
        
        initial_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Process in large batches
        batch_size = 100
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            # Simulate processing
            _ = [self.enhancer.create_compact_json(data, f"TEST_TABLE_{i}") for data in batch]
        
        final_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable
        assert memory_increase < 100, f"Memory increase too high: {memory_increase}MB"
    
    def test_adaptive_batch_sizing_performance(self):
        """Test performance of adaptive batch sizing"""
        # Test with different table characteristics
        test_cases = [
            # (record_size, field_count, expected_performance_range)
            (50, 10, (0.001, 0.01)),      # Small records - should be fast
            (500, 25, (0.01, 0.05)),      # Medium records
            (2000, 50, (0.05, 0.2)),      # Large records - should be slower
            (8000, 150, (0.1, 0.5)),      # Very large records - should be slowest
        ]
        
        for record_size, field_count, expected_range in test_cases:
            table_def = Mock()
            table_def.record_size = record_size
            table_def.field_count = field_count
            
            # Measure time to calculate batch size
            start_time = time.time()
            batch_size = self.enhancer.get_adaptive_batch_size(table_def)
            end_time = time.time()
            
            calculation_time = end_time - start_time
            
            assert expected_range[0] <= calculation_time <= expected_range[1], \
                f"Batch size calculation too slow: {calculation_time}s for record_size={record_size}, field_count={field_count}"
            
            # Verify batch size is reasonable
            assert 5 <= batch_size <= 400, \
                f"Batch size {batch_size} out of reasonable range for record_size={record_size}, field_count={field_count}"
    
    def test_compact_json_creation_performance(self):
        """Test performance of compact JSON creation"""
        # Test with different data sizes
        test_cases = [
            # (data_size, expected_time_range)
            (100, (0.001, 0.01)),      # Small data
            (1000, (0.01, 0.05)),      # Medium data
            (10000, (0.05, 0.2)),      # Large data
            (100000, (0.2, 1.0)),      # Very large data
        ]
        
        for data_size, expected_range in test_cases:
            raw_data = b"x" * data_size
            table_name = f"PERF_TEST_TABLE_{data_size}"
            
            # Measure time to create compact JSON
            start_time = time.time()
            result = self.enhancer.create_compact_json(raw_data, table_name)
            end_time = time.time()
            
            creation_time = end_time - start_time
            
            assert expected_range[0] <= creation_time <= expected_range[1], \
                f"JSON creation too slow: {creation_time}s for data_size={data_size}"
            
            # Verify result is valid JSON
            import json
            parsed = json.loads(result)
            assert parsed['data_size'] == data_size
            assert parsed['table'] == table_name
    
    def test_memory_monitoring_performance(self):
        """Test performance of memory monitoring"""
        # Test memory monitoring overhead
        iterations = 1000
        
        start_time = time.time()
        for _ in range(iterations):
            self.enhancer.check_memory_usage()
        end_time = time.time()
        
        total_time = end_time - start_time
        avg_time_per_check = total_time / iterations
        
        # Memory check should be very fast (less than 1ms per check)
        assert avg_time_per_check < 0.001, \
            f"Memory monitoring too slow: {avg_time_per_check}s per check"
    
    def test_size_estimation_performance(self):
        """Test performance of size estimation"""
        # Mock TPS with realistic structure
        tps = Mock()
        
        # Create mock table
        table_mock = Mock(name="PERF_TEST_TABLE")
        tps.tables._TpsTablesList__tables = {1: table_mock}
        
        # Create many pages (simulate large database)
        pages = []
        for i in range(100):  # 100 pages
            page = Mock()
            page.hierarchy_level = 0
            pages.append((i, page))
        
        tps.pages.list.return_value = [p[0] for p in pages]
        tps.pages.__getitem__.side_effect = lambda x: next(p[1] for p in pages if p[0] == x)
        
        # Mock records
        with patch('pytopspeed.tpsrecord.TpsRecordsList') as mock_records_list:
            mock_records = []
            for i in range(10):  # 10 records per page
                record = Mock()
                record.type = 'DATA'
                record.data.table_number = 1
                mock_records.append(record)
            
            mock_records_list.return_value = mock_records
            
            # Measure time to estimate size
            start_time = time.time()
            result = self.enhancer.estimate_table_size(tps, "PERF_TEST_TABLE")
            end_time = time.time()
            
            estimation_time = end_time - start_time
            
            # Size estimation should be reasonably fast (less than 1 second)
            assert estimation_time < 1.0, \
                f"Size estimation too slow: {estimation_time}s"
            
            # Verify result is reasonable
            assert result['estimated_records'] > 0
            assert result['estimated_size_mb'] > 0
    
    def test_configuration_performance(self):
        """Test performance of configuration operations"""
        configs = ['small', 'medium', 'large', 'enterprise', 'default']
        
        # Test configuration retrieval performance
        start_time = time.time()
        for _ in range(1000):  # 1000 iterations
            for config_name in configs:
                config = get_resilience_config(config_name)
                _ = config.to_dict()
        end_time = time.time()
        
        total_time = end_time - start_time
        avg_time_per_config = total_time / (1000 * len(configs))
        
        # Configuration operations should be very fast
        assert avg_time_per_config < 0.001, \
            f"Configuration operations too slow: {avg_time_per_config}s per config"
    
    def test_streaming_decision_performance(self):
        """Test performance of streaming decision logic"""
        # Create test cases
        test_cases = []
        for record_size in [100, 1000, 5000, 10000]:
            for field_count in [10, 50, 100, 200]:
                for estimated_records in [1000, 10000, 100000, 1000000]:
                    table_def = Mock()
                    table_def.record_size = record_size
                    table_def.field_count = field_count
                    estimated_size = {'estimated_records': estimated_records}
                    test_cases.append((table_def, estimated_size))
        
        # Measure time to make streaming decisions
        start_time = time.time()
        for table_def, estimated_size in test_cases:
            _ = self.enhancer.should_use_streaming(table_def, estimated_size)
        end_time = time.time()
        
        total_time = end_time - start_time
        avg_time_per_decision = total_time / len(test_cases)
        
        # Streaming decision should be very fast
        assert avg_time_per_decision < 0.001, \
            f"Streaming decision too slow: {avg_time_per_decision}s per decision"
    
    def test_memory_cleanup_performance(self):
        """Test performance of memory cleanup operations"""
        # Create some memory pressure
        test_data = []
        for i in range(1000):
            test_data.append(b"x" * 10000)  # 10KB per item
        
        # Measure cleanup performance
        start_time = time.time()
        for _ in range(100):  # 100 cleanup operations
            self.enhancer.force_memory_cleanup()
        end_time = time.time()
        
        total_time = end_time - start_time
        avg_time_per_cleanup = total_time / 100
        
        # Memory cleanup should be reasonably fast
        assert avg_time_per_cleanup < 0.01, \
            f"Memory cleanup too slow: {avg_time_per_cleanup}s per cleanup"
    
    def test_large_data_processing_performance(self):
        """Test performance with large data processing"""
        # Test with very large data
        large_data = b"x" * 1000000  # 1MB of data
        table_name = "LARGE_DATA_TABLE"
        
        # Measure processing time
        start_time = time.time()
        result = self.enhancer.create_compact_json(large_data, table_name)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Large data processing should be reasonably fast
        assert processing_time < 2.0, \
            f"Large data processing too slow: {processing_time}s for 1MB data"
        
        # Verify result integrity
        import json
        parsed = json.loads(result)
        assert parsed['data_size'] == 1000000
        assert parsed['table'] == table_name
    
    def test_concurrent_operations_performance(self):
        """Test performance under simulated concurrent operations"""
        import threading
        import queue
        
        # Create work queue
        work_queue = queue.Queue()
        results = []
        
        # Add work items
        for i in range(1000):
            work_queue.put((b"x" * 1000, f"CONCURRENT_TABLE_{i}"))
        
        def worker():
            while True:
                try:
                    raw_data, table_name = work_queue.get(timeout=1)
                    result = self.enhancer.create_compact_json(raw_data, table_name)
                    results.append(result)
                    work_queue.task_done()
                except queue.Empty:
                    break
        
        # Create and start worker threads
        threads = []
        for _ in range(4):  # 4 worker threads
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)
        
        # Measure total processing time
        start_time = time.time()
        
        # Wait for all work to complete
        work_queue.join()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join()
        
        end_time = time.time()
        
        total_time = end_time - start_time
        
        # Concurrent processing should be reasonably fast
        assert total_time < 5.0, \
            f"Concurrent processing too slow: {total_time}s for 1000 items with 4 threads"
        
        # Verify all results were processed
        assert len(results) == 1000
    
    def test_memory_limit_enforcement_performance(self):
        """Test performance of memory limit enforcement"""
        # Test with different memory limits
        memory_limits = [50, 100, 200, 500]  # MB
        
        for limit in memory_limits:
            enhancer = ResilienceEnhancer(max_memory_mb=limit)
            
            # Simulate memory pressure
            test_data = []
            for i in range(100):
                test_data.append(b"x" * 10000)  # 10KB per item
            
            # Process data and check memory
            start_time = time.time()
            for data in test_data:
                _ = enhancer.create_compact_json(data, f"MEMORY_TEST_{i}")
                # Check memory usage
                _ = enhancer.check_memory_usage()
            end_time = time.time()
            
            processing_time = end_time - start_time
            
            # Memory checking should not significantly slow down processing
            assert processing_time < 1.0, \
                f"Memory limit enforcement too slow: {processing_time}s for limit {limit}MB"
    
    def test_scalability_with_data_size(self):
        """Test scalability with increasing data sizes"""
        data_sizes = [1000, 10000, 100000, 1000000]  # 1KB to 1MB
        processing_times = []
        
        for size in data_sizes:
            raw_data = b"x" * size
            table_name = f"SCALABILITY_TEST_{size}"
            
            # Measure processing time
            start_time = time.time()
            result = self.enhancer.create_compact_json(raw_data, table_name)
            end_time = time.time()
            
            processing_time = end_time - start_time
            processing_times.append(processing_time)
        
        # Processing time should scale reasonably with data size
        # (not necessarily linearly, but should not be exponential)
        for i in range(1, len(processing_times)):
            time_ratio = processing_times[i] / processing_times[i-1]
            size_ratio = data_sizes[i] / data_sizes[i-1]
            
            # Time ratio should be less than size ratio squared (avoid exponential scaling)
            assert time_ratio < size_ratio * size_ratio, \
                f"Processing time scaling poorly: {time_ratio} vs {size_ratio} for size {data_sizes[i]}"
    
    def test_batch_size_optimization_performance(self):
        """Test performance impact of different batch sizes"""
        # Test data
        test_data = [b"x" * 1000 for _ in range(1000)]  # 1000 items of 1KB each
        
        batch_sizes = [10, 25, 50, 100, 200]
        processing_times = []
        
        for batch_size in batch_sizes:
            start_time = time.time()
            
            # Process in batches
            for i in range(0, len(test_data), batch_size):
                batch = test_data[i:i + batch_size]
                for j, data in enumerate(batch):
                    _ = self.enhancer.create_compact_json(data, f"BATCH_TEST_{i}_{j}")
                
                # Simulate batch processing overhead
                if i % (batch_size * 10) == 0:
                    self.enhancer.force_memory_cleanup()
            
            end_time = time.time()
            processing_time = end_time - start_time
            processing_times.append(processing_time)
        
        # All batch sizes should complete in reasonable time
        for i, time_taken in enumerate(processing_times):
            assert time_taken < 5.0, \
                f"Batch size {batch_sizes[i]} too slow: {time_taken}s"
        
        # Larger batch sizes should generally be faster (less overhead)
        # But this is not always true due to memory pressure
        # So we just verify they're all reasonable
