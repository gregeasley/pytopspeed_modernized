#!/usr/bin/env python3
"""
Unit tests for PerformanceOptimizer
"""

import pytest
import tempfile
import os
import sqlite3
from unittest.mock import Mock, patch, MagicMock

from converter.performance_optimizer import PerformanceOptimizer


class TestPerformanceOptimizer:
    """Test cases for PerformanceOptimizer"""
    
    def test_initialization(self):
        """Test PerformanceOptimizer initialization"""
        optimizer = PerformanceOptimizer(
            max_workers=4,
            memory_limit_mb=512,
            cache_size=500,
            progress_callback=lambda x, y, z: None
        )
        
        assert optimizer.max_workers == 4
        assert optimizer.memory_limit_mb == 512
        assert optimizer.cache_size == 500
        assert optimizer.progress_callback is not None
        assert optimizer.performance_metrics['start_time'] is None
        assert optimizer.performance_metrics['end_time'] is None
        assert optimizer.cache == {}
        assert optimizer.cache_hits == 0
        assert optimizer.cache_misses == 0
    
    def test_configure_optimization_memory(self):
        """Test optimization configuration for memory strategy"""
        optimizer = PerformanceOptimizer()
        
        config = optimizer._configure_optimization(
            strategy='memory',
            enable_parallel=True,
            enable_streaming=True,
            enable_caching=True
        )
        
        assert config['batch_size'] == 500
        assert config['memory_buffer_size'] == 32 * 1024
        assert config['prefetch_size'] == 50
        assert config['compression'] is True
        assert config['parallel_processing'] is True
        assert config['streaming'] is True
        assert config['caching'] is True
    
    def test_configure_optimization_speed(self):
        """Test optimization configuration for speed strategy"""
        optimizer = PerformanceOptimizer()
        
        config = optimizer._configure_optimization(
            strategy='speed',
            enable_parallel=True,
            enable_streaming=True,
            enable_caching=True
        )
        
        assert config['batch_size'] == 2000
        assert config['memory_buffer_size'] == 128 * 1024
        assert config['prefetch_size'] == 200
        assert config['compression'] is False
        assert config['parallel_processing'] is True
        assert config['streaming'] is True
        assert config['caching'] is True
    
    def test_configure_optimization_balanced(self):
        """Test optimization configuration for balanced strategy"""
        optimizer = PerformanceOptimizer()
        
        config = optimizer._configure_optimization(
            strategy='balanced',
            enable_parallel=True,
            enable_streaming=True,
            enable_caching=True
        )
        
        assert config['batch_size'] == 1000  # Default
        assert config['memory_buffer_size'] == 64 * 1024  # Default
        assert config['prefetch_size'] == 100  # Default
        assert config['compression'] is False  # Default
        assert config['parallel_processing'] is True
        assert config['streaming'] is True
        assert config['caching'] is True
    
    def test_optimize_database_connection(self):
        """Test database connection optimization"""
        optimizer = PerformanceOptimizer()
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_file = f.name
        
        try:
            conn = sqlite3.connect(db_file)
            config = {'batch_size': 1000}
            
            optimizer._optimize_database_connection(conn, config)
            
            # Verify optimizations were applied
            cursor = conn.execute("PRAGMA journal_mode")
            assert cursor.fetchone()[0] == "wal"
            
            cursor = conn.execute("PRAGMA synchronous")
            assert cursor.fetchone()[0] == 1  # NORMAL
            
            cursor = conn.execute("PRAGMA cache_size")
            assert cursor.fetchone()[0] == 10000
            
            conn.close()
            
        finally:
            if os.path.exists(db_file):
                os.unlink(db_file)
    
    def test_convert_field_value_string(self):
        """Test field value conversion for string types"""
        optimizer = PerformanceOptimizer()
        
        # Mock field
        mock_field = Mock()
        mock_field.type = "STRING"
        
        # Test string conversion
        result = optimizer._convert_field_value(mock_field, "test\x00")
        assert result == "test"
        
        # Test None value
        result = optimizer._convert_field_value(mock_field, None)
        assert result is None
        
        # Test non-string value
        result = optimizer._convert_field_value(mock_field, 123)
        assert result == "123"
    
    def test_convert_field_value_integer(self):
        """Test field value conversion for integer types"""
        optimizer = PerformanceOptimizer()
        
        # Mock field
        mock_field = Mock()
        mock_field.type = "LONG"
        
        # Test integer conversion
        result = optimizer._convert_field_value(mock_field, 123)
        assert result == 123
        
        # Test None value
        result = optimizer._convert_field_value(mock_field, None)
        assert result is None
        
        # Test invalid value
        result = optimizer._convert_field_value(mock_field, "invalid")
        assert result is None
    
    def test_convert_field_value_float(self):
        """Test field value conversion for float types"""
        optimizer = PerformanceOptimizer()
        
        # Mock field
        mock_field = Mock()
        mock_field.type = "DECIMAL"
        
        # Test float conversion
        result = optimizer._convert_field_value(mock_field, 123.45)
        assert result == 123.45
        
        # Test None value
        result = optimizer._convert_field_value(mock_field, None)
        assert result is None
        
        # Test invalid value
        result = optimizer._convert_field_value(mock_field, "invalid")
        assert result is None
    
    def test_convert_topspeed_date(self):
        """Test TopSpeed date conversion"""
        optimizer = PerformanceOptimizer()
        
        # Test valid date
        result = optimizer._convert_topspeed_date(20231225)
        assert result == "2023-12-25"
        
        # Test invalid date
        result = optimizer._convert_topspeed_date(123)
        assert result is None
        
        # Test None value
        result = optimizer._convert_topspeed_date(None)
        assert result is None
    
    def test_convert_topspeed_time(self):
        """Test TopSpeed time conversion"""
        optimizer = PerformanceOptimizer()
        
        # Test valid time
        result = optimizer._convert_topspeed_time(143022)
        assert result == "14:30:22"
        
        # Test time with leading zeros
        result = optimizer._convert_topspeed_time(902)
        assert result == "00:09:02"
        
        # Test short time (should be padded)
        result = optimizer._convert_topspeed_time(123)
        assert result == "00:01:23"
        
        # Test None value
        result = optimizer._convert_topspeed_time(None)
        assert result is None
    
    def test_convert_record_to_tuple(self):
        """Test record to tuple conversion"""
        optimizer = PerformanceOptimizer()
        
        # Mock record
        mock_record = Mock()
        mock_record.data = Mock()
        mock_record.data.data = Mock()
        mock_record.data.data.field1 = "value1"
        mock_record.data.data.field2 = 123
        mock_record.record_number = 1
        mock_record._get_memo_data = Mock(return_value=b"memo_data")

        # Mock table definition
        mock_table_def = Mock()
        mock_field1 = Mock()
        mock_field1.name = "field1"
        mock_field1.type = "STRING"
        mock_field2 = Mock()
        mock_field2.name = "field2"
        mock_field2.type = "LONG"
        mock_table_def.fields = [mock_field1, mock_field2]

        mock_memo = Mock()
        mock_memo.name = "memo1"
        mock_table_def.memos = [mock_memo]
        
        result = optimizer._convert_record_to_tuple(mock_record, mock_table_def)
        
        assert len(result) == 3  # 2 fields + 1 memo
        assert result[0] == "value1"
        assert result[1] == 123
        assert result[2] == b"memo_data"
    
    def test_copy_table_optimized(self):
        """Test optimized table copying"""
        optimizer = PerformanceOptimizer()
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f1:
            source_db = f1.name
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f2:
            dest_db = f2.name
        
        try:
            # Create source database
            conn_source = sqlite3.connect(source_db)
            conn_source.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")
            conn_source.execute("INSERT INTO test_table VALUES (1, 'test1')")
            conn_source.execute("INSERT INTO test_table VALUES (2, 'test2')")
            conn_source.commit()
            conn_source.close()
            
            # Create destination database
            conn_dest = sqlite3.connect(dest_db)
            conn_dest.close()
            
            # Copy table
            conn_source = sqlite3.connect(source_db)
            conn_dest = sqlite3.connect(dest_db)
            
            optimizer._copy_table_optimized(conn_source, conn_dest, "test_table")
            
            # Verify copy
            cursor = conn_dest.execute("SELECT * FROM test_table ORDER BY id")
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, 'test1')
            assert rows[1] == (2, 'test2')
            
            conn_source.close()
            conn_dest.close()
            
        finally:
            for db_file in [source_db, dest_db]:
                if os.path.exists(db_file):
                    os.unlink(db_file)
    
    def test_check_memory_limit(self):
        """Test memory limit checking"""
        optimizer = PerformanceOptimizer(memory_limit_mb=100)
        
        with patch('psutil.virtual_memory') as mock_memory:
            # Test within limit (100MB used, limit is 100MB, so should be False)
            mock_memory.return_value = Mock(
                total=1024*1024*1024,  # 1GB
                available=924*1024*1024  # 924MB available (100MB used)
            )
            assert optimizer._check_memory_limit() is False
            
            # Test over limit
            mock_memory.return_value = Mock(
                total=1024*1024*1024,  # 1GB
                available=50*1024*1024  # 50MB available (950MB used)
            )
            assert optimizer._check_memory_limit() is True
    
    def test_calculate_performance_metrics(self):
        """Test performance metrics calculation"""
        optimizer = PerformanceOptimizer()
        
        # Set up some test data
        from datetime import datetime, timedelta
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)
        optimizer.performance_metrics['start_time'] = start_time
        optimizer.performance_metrics['end_time'] = end_time
        optimizer.performance_metrics['memory_usage'] = [50, 60, 70]
        optimizer.performance_metrics['cpu_usage'] = [30, 40, 50]
        optimizer.cache_hits = 8
        optimizer.cache_misses = 2
        
        metrics = optimizer._calculate_performance_metrics()
        
        assert 'avg_memory_usage' in metrics
        assert 'avg_cpu_usage' in metrics
        assert 'cache_hit_rate' in metrics
        assert metrics['avg_memory_usage'] == 60.0
        assert metrics['avg_cpu_usage'] == 40.0
        assert metrics['cache_hit_rate'] == 0.8
    
    def test_get_performance_report(self):
        """Test performance report generation"""
        optimizer = PerformanceOptimizer()
        
        # Set up test metrics
        from datetime import datetime, timedelta
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=5)
        optimizer.performance_metrics['start_time'] = start_time
        optimizer.performance_metrics['end_time'] = end_time
        optimizer.performance_metrics['memory_usage'] = [50, 60, 70]
        optimizer.performance_metrics['cpu_usage'] = [30, 40, 50]
        optimizer.cache_hits = 8
        optimizer.cache_misses = 2
        
        report = optimizer.get_performance_report()
        
        assert "PERFORMANCE OPTIMIZATION REPORT" in report
        assert "PERFORMANCE METRICS" in report
        assert "OPTIMIZATION SETTINGS" in report
        assert "Max Workers: 8" in report  # Default value
        assert "Memory Limit: 1024 MB" in report  # Default value
        assert "Cache Size: 1000" in report  # Default value
    
    def test_optimize_conversion_sequential(self):
        """Test optimized conversion in sequential mode"""
        optimizer = PerformanceOptimizer(max_workers=1)
        
        with tempfile.NamedTemporaryFile(suffix='.phd', delete=False) as f1:
            input_file = f1.name
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f2:
            output_file = f2.name
        
        try:
            # Mock TPS object
            with patch('converter.performance_optimizer.TPS') as mock_tps_class:
                mock_table_def = Mock()
                mock_table_def.fields = [Mock(name="field1", type="STRING")]
                mock_table_def.indexes = []
                mock_table_def.memos = []
                
                mock_tps = Mock()
                mock_tables = Mock()
                mock_tables.__iter__ = Mock(return_value=iter(["test_table"]))
                mock_tables.get_definition = Mock(return_value=mock_table_def)
                mock_tps.tables = mock_tables
                mock_tps.set_current_table = Mock()
                mock_tps.__iter__ = Mock(return_value=iter([Mock()]))
                mock_tps_class.return_value = mock_tps
                
                results = optimizer.optimize_conversion(
                    input_files=[input_file],
                    output_file=output_file,
                    optimization_strategy='balanced',
                    enable_parallel=False
                )
                
                assert 'success' in results
                assert 'files_processed' in results
                assert 'tables_created' in results
                assert 'total_records' in results
                assert 'performance_metrics' in results
                
        finally:
            for file_path in [input_file, output_file]:
                if os.path.exists(file_path):
                    os.unlink(file_path)
    
    def test_optimize_conversion_parallel(self):
        """Test optimized conversion in parallel mode"""
        optimizer = PerformanceOptimizer(max_workers=2)
        
        with tempfile.NamedTemporaryFile(suffix='.phd', delete=False) as f1:
            input_file1 = f1.name
        
        with tempfile.NamedTemporaryFile(suffix='.phd', delete=False) as f2:
            input_file2 = f2.name
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f3:
            output_file = f3.name
        
        try:
            # Mock file processing
            with patch.object(optimizer, '_process_file_optimized') as mock_process:
                mock_process.return_value = {
                    'success': True,
                    'tables_created': 5,
                    'total_records': 100
                }
                
                # Test that parallel processing is attempted
                results = optimizer.optimize_conversion(
                    input_files=[input_file1, input_file2],
                    output_file=output_file,
                    optimization_strategy='balanced',
                    enable_parallel=True
                )
                
                assert 'success' in results
                assert 'files_processed' in results
                assert 'performance_metrics' in results
                    
        finally:
            for file_path in [input_file1, input_file2, output_file]:
                if os.path.exists(file_path):
                    os.unlink(file_path)
    
    def test_optimize_conversion_error_handling(self):
        """Test optimization conversion error handling"""
        optimizer = PerformanceOptimizer()
        
        # Test with non-existent files
        results = optimizer.optimize_conversion(
            input_files=["nonexistent.phd"],
            output_file="output.sqlite"
        )
        
        assert results['success'] is False
        assert len(results['errors']) > 0
    
    def test_migrate_data_streaming(self):
        """Test streaming data migration"""
        optimizer = PerformanceOptimizer()
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_file = f.name
        
        try:
            # Create test database
            conn = sqlite3.connect(db_file)
            conn.execute("CREATE TABLE test_table (field1 TEXT)")
            conn.commit()
            conn.close()
            
            # Mock TPS object
            mock_tps = Mock()
            mock_tps.set_current_table = Mock()
            
            # Mock record
            mock_record = Mock()
            mock_record.data = Mock()
            mock_record.data.data = Mock()
            mock_record.data.data.field1 = "test_value"
            mock_record.record_number = 1
            mock_record._get_memo_data = Mock(return_value=None)
            
            mock_tps.__iter__ = Mock(return_value=iter([mock_record]))
            
            # Mock table definition
            mock_table_def = Mock()
            mock_field = Mock()
            mock_field.name = "field1"
            mock_field.type = "STRING"
            mock_table_def.fields = [mock_field]
            mock_table_def.memos = []
            
            config = {'batch_size': 1000}
            
            conn = sqlite3.connect(db_file)
            record_count = optimizer._migrate_data_streaming(
                mock_tps, conn, "test_table", mock_table_def, config
            )
            conn.commit()
            conn.close()
            
            assert record_count == 1
            
            # Verify data was inserted
            conn = sqlite3.connect(db_file)
            cursor = conn.execute("SELECT * FROM test_table")
            rows = cursor.fetchall()
            conn.close()
            
            assert len(rows) == 1
            assert rows[0][0] == "test_value"
            
        finally:
            if os.path.exists(db_file):
                os.unlink(db_file)
    
    def test_migrate_data_batch(self):
        """Test batch data migration"""
        optimizer = PerformanceOptimizer()
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_file = f.name
        
        try:
            # Create test database
            conn = sqlite3.connect(db_file)
            conn.execute("CREATE TABLE test_table (field1 TEXT)")
            conn.commit()
            conn.close()
            
            # Mock TPS object
            mock_tps = Mock()
            mock_tps.set_current_table = Mock()
            
            # Mock records
            mock_records = []
            for i in range(5):
                mock_record = Mock()
                mock_record.data = Mock()
                mock_record.data.data = Mock()
                mock_record.data.data.field1 = f"test_value_{i}"
                mock_record.record_number = i
                mock_record._get_memo_data = Mock(return_value=None)
                mock_records.append(mock_record)
            
            mock_tps.__iter__ = Mock(return_value=iter(mock_records))
            
            # Mock table definition
            mock_table_def = Mock()
            mock_field = Mock()
            mock_field.name = "field1"
            mock_field.type = "STRING"
            mock_table_def.fields = [mock_field]
            mock_table_def.memos = []
            
            config = {'batch_size': 2}  # Small batch size for testing
            
            conn = sqlite3.connect(db_file)
            record_count = optimizer._migrate_data_batch(
                mock_tps, conn, "test_table", mock_table_def, config
            )
            conn.commit()
            conn.close()
            
            assert record_count == 5
            
            # Verify data was inserted
            conn = sqlite3.connect(db_file)
            cursor = conn.execute("SELECT * FROM test_table ORDER BY field1")
            rows = cursor.fetchall()
            conn.close()
            
            assert len(rows) == 5
            for i, row in enumerate(rows):
                assert row[0] == f"test_value_{i}"
            
        finally:
            if os.path.exists(db_file):
                os.unlink(db_file)
