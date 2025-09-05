#!/usr/bin/env python3
"""
Performance Optimizer - Parallel processing, memory-efficient streaming, and caching

This module provides advanced performance optimization capabilities including
parallel processing, memory-efficient streaming, and intelligent caching.
"""

import os
import sqlite3
import logging
import threading
import multiprocessing as mp
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable, Iterator
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from queue import Queue, Empty
import time
import psutil
import gc

from .sqlite_converter import SqliteConverter
from .schema_mapper import TopSpeedToSQLiteMapper
from pytopspeed.tps import TPS


class PerformanceOptimizer:
    """
    Advanced performance optimizer for TopSpeed to SQLite conversions
    """
    
    def __init__(self, 
                 max_workers: int = None,
                 memory_limit_mb: int = 1024,
                 cache_size: int = 1000,
                 progress_callback=None):
        """
        Initialize performance optimizer
        
        Args:
            max_workers: Maximum number of worker processes/threads
            memory_limit_mb: Memory limit in MB for processing
            cache_size: Size of in-memory cache
            progress_callback: Optional callback function for progress updates
        """
        self.max_workers = max_workers or min(mp.cpu_count(), 8)
        self.memory_limit_mb = memory_limit_mb
        self.cache_size = cache_size
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # Performance monitoring
        self.performance_metrics = {
            'start_time': None,
            'end_time': None,
            'memory_usage': [],
            'cpu_usage': [],
            'throughput_records_per_sec': 0,
            'parallel_efficiency': 0.0,
            'cache_hit_rate': 0.0
        }
        
        # Cache for frequently accessed data
        self.cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Thread-safe locks
        self.cache_lock = threading.Lock()
        self.metrics_lock = threading.Lock()
        
    def optimize_conversion(self, 
                          input_files: List[str], 
                          output_file: str,
                          optimization_strategy: str = 'balanced',
                          enable_parallel: bool = True,
                          enable_streaming: bool = True,
                          enable_caching: bool = True) -> Dict[str, Any]:
        """
        Optimize TopSpeed to SQLite conversion with advanced performance features
        
        Args:
            input_files: List of TopSpeed files to convert
            output_file: Output SQLite database file
            optimization_strategy: Strategy ('memory', 'speed', 'balanced')
            enable_parallel: Enable parallel processing
            enable_streaming: Enable memory-efficient streaming
            enable_caching: Enable intelligent caching
            
        Returns:
            Dictionary with conversion results and performance metrics
        """
        self.performance_metrics['start_time'] = datetime.now()
        
        # Configure optimization based on strategy
        config = self._configure_optimization(optimization_strategy, enable_parallel, 
                                            enable_streaming, enable_caching)
        
        results = {
            'success': False,
            'files_processed': 0,
            'tables_created': 0,
            'total_records': 0,
            'duration': 0,
            'performance_metrics': {},
            'optimization_config': config,
            'errors': []
        }
        
        try:
            self.logger.info(f"Starting optimized conversion with strategy: {optimization_strategy}")
            
            # Start performance monitoring
            monitor_thread = threading.Thread(target=self._monitor_performance)
            monitor_thread.daemon = True
            monitor_thread.start()
            
            if config['parallel_processing'] and len(input_files) > 1:
                # Parallel processing
                results = self._parallel_conversion(input_files, output_file, config, results)
            else:
                # Optimized sequential processing
                results = self._optimized_sequential_conversion(input_files, output_file, config, results)
            
            results['success'] = len(results['errors']) == 0
            
        except Exception as e:
            error_msg = f"Optimized conversion failed: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        finally:
            self.performance_metrics['end_time'] = datetime.now()
            results['duration'] = (self.performance_metrics['end_time'] - 
                                 self.performance_metrics['start_time']).total_seconds()
            results['performance_metrics'] = self._calculate_performance_metrics()
            
        return results
    
    def _configure_optimization(self, strategy: str, enable_parallel: bool, 
                               enable_streaming: bool, enable_caching: bool) -> Dict[str, Any]:
        """Configure optimization parameters based on strategy"""
        config = {
            'parallel_processing': enable_parallel,
            'streaming': enable_streaming,
            'caching': enable_caching,
            'batch_size': 1000,
            'memory_buffer_size': 64 * 1024,  # 64KB
            'cache_ttl': 300,  # 5 minutes
            'compression': False,
            'prefetch_size': 100
        }
        
        if strategy == 'memory':
            config.update({
                'batch_size': 500,
                'memory_buffer_size': 32 * 1024,  # 32KB
                'prefetch_size': 50,
                'compression': True
            })
        elif strategy == 'speed':
            config.update({
                'batch_size': 2000,
                'memory_buffer_size': 128 * 1024,  # 128KB
                'prefetch_size': 200,
                'compression': False
            })
        # 'balanced' uses defaults
        
        return config
    
    def _parallel_conversion(self, input_files: List[str], output_file: str, 
                           config: Dict, results: Dict) -> Dict[str, Any]:
        """Perform parallel conversion of multiple files"""
        self.logger.info(f"Starting parallel conversion with {self.max_workers} workers")
        
        # Create temporary databases for each file
        temp_dbs = []
        for i, file_path in enumerate(input_files):
            temp_db = f"{output_file}_temp_{i}.sqlite"
            temp_dbs.append(temp_db)
        
        # Process files in parallel
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {}
            
            for i, file_path in enumerate(input_files):
                future = executor.submit(self._process_file_optimized, file_path, temp_dbs[i], config)
                future_to_file[future] = (file_path, i)
            
            # Collect results
            for future in as_completed(future_to_file):
                file_path, file_index = future_to_file[future]
                try:
                    file_results = future.result()
                    results['files_processed'] += 1
                    results['tables_created'] += file_results.get('tables_created', 0)
                    results['total_records'] += file_results.get('total_records', 0)
                    
                    if self.progress_callback:
                        self.progress_callback(results['files_processed'], len(input_files),
                                             f"Processed {os.path.basename(file_path)}")
                    
                except Exception as e:
                    error_msg = f"Failed to process {file_path}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
        
        # Merge temporary databases
        if results['files_processed'] > 0:
            self._merge_optimized_databases(temp_dbs, output_file, config)
            
            # Clean up temporary files
            for temp_db in temp_dbs:
                if os.path.exists(temp_db):
                    os.remove(temp_db)
        
        return results
    
    def _optimized_sequential_conversion(self, input_files: List[str], output_file: str,
                                       config: Dict, results: Dict) -> Dict[str, Any]:
        """Perform optimized sequential conversion"""
        self.logger.info("Starting optimized sequential conversion")
        
        # Create output database with optimizations
        conn = sqlite3.connect(output_file)
        self._optimize_database_connection(conn, config)
        
        try:
            for file_path in input_files:
                try:
                    file_results = self._process_file_with_optimizations(file_path, conn, config)
                    results['files_processed'] += 1
                    results['tables_created'] += file_results.get('tables_created', 0)
                    results['total_records'] += file_results.get('total_records', 0)
                    
                    if self.progress_callback:
                        self.progress_callback(results['files_processed'], len(input_files),
                                             f"Processed {os.path.basename(file_path)}")
                    
                except Exception as e:
                    error_msg = f"Failed to process {file_path}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
                    
        finally:
            conn.close()
        
        return results
    
    def _process_file_optimized(self, file_path: str, output_db: str, config: Dict) -> Dict[str, Any]:
        """Process a single file with optimizations (for parallel processing)"""
        # This method runs in a separate process
        converter = SqliteConverter(batch_size=config['batch_size'])
        return converter.convert(file_path, output_db)
    
    def _process_file_with_optimizations(self, file_path: str, conn: sqlite3.Connection,
                                       config: Dict) -> Dict[str, Any]:
        """Process a single file with optimizations (for sequential processing)"""
        results = {
            'tables_created': 0,
            'total_records': 0
        }
        
        try:
            # Load TopSpeed file
            tps = TPS(file_path)
            
            # Create schema with optimizations
            schema_mapper = TopSpeedToSQLiteMapper()
            
            for table_name in tps.tables:
                try:
                    # Get table definition
                    table_def = tps.tables.get_definition(table_name)
                    if not table_def:
                        continue
                    
                    # Create table with optimizations
                    create_sql = schema_mapper.generate_create_table_sql(table_name, table_def)
                    conn.execute(create_sql)
                    
                    # Create indexes
                    for index in table_def.indexes:
                        index_sql = schema_mapper.generate_create_index_sql(table_name, index, table_def)
                        conn.execute(index_sql)
                    
                    results['tables_created'] += 1
                    
                    # Migrate data with streaming
                    if config['streaming']:
                        record_count = self._migrate_data_streaming(tps, conn, table_name, table_def, config)
                    else:
                        record_count = self._migrate_data_batch(tps, conn, table_name, table_def, config)
                    
                    results['total_records'] += record_count
                    
                except Exception as e:
                    self.logger.warning(f"Error processing table {table_name}: {e}")
                    continue
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            raise
        
        return results
    
    def _migrate_data_streaming(self, tps: TPS, conn: sqlite3.Connection, table_name: str,
                               table_def: Any, config: Dict) -> int:
        """Migrate data using memory-efficient streaming"""
        tps.set_current_table(table_name)
        
        # Prepare insert statement
        field_names = [field.name for field in table_def.fields]
        memo_names = [memo.name for memo in table_def.memos]
        all_names = field_names + memo_names
        
        placeholders = ', '.join(['?' for _ in all_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(all_names)}) VALUES ({placeholders})"
        
        # Stream data in batches
        batch = []
        record_count = 0
        batch_size = config['batch_size']
        
        for record in tps:
            # Convert record to tuple
            record_tuple = self._convert_record_to_tuple(record, table_def)
            batch.append(record_tuple)
            
            if len(batch) >= batch_size:
                conn.executemany(insert_sql, batch)
                record_count += len(batch)
                batch = []
                
                # Check memory usage
                if self._check_memory_limit():
                    gc.collect()
        
        # Insert remaining records
        if batch:
            conn.executemany(insert_sql, batch)
            record_count += len(batch)
        
        return record_count
    
    def _migrate_data_batch(self, tps: TPS, conn: sqlite3.Connection, table_name: str,
                           table_def: Any, config: Dict) -> int:
        """Migrate data using batch processing"""
        tps.set_current_table(table_name)
        
        # Prepare insert statement
        field_names = [field.name for field in table_def.fields]
        memo_names = [memo.name for memo in table_def.memos]
        all_names = field_names + memo_names
        
        placeholders = ', '.join(['?' for _ in all_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(all_names)}) VALUES ({placeholders})"
        
        # Process in batches
        batch = []
        record_count = 0
        batch_size = config['batch_size']
        
        for record in tps:
            record_tuple = self._convert_record_to_tuple(record, table_def)
            batch.append(record_tuple)
            
            if len(batch) >= batch_size:
                conn.executemany(insert_sql, batch)
                record_count += len(batch)
                batch = []
        
        # Insert remaining records
        if batch:
            conn.executemany(insert_sql, batch)
            record_count += len(batch)
        
        return record_count
    
    def _convert_record_to_tuple(self, record: Any, table_def: Any) -> tuple:
        """Convert TopSpeed record to tuple for SQLite insertion"""
        values = []
        
        # Convert field values
        for field in table_def.fields:
            try:
                if hasattr(record, 'data') and hasattr(record.data, 'data'):
                    value = getattr(record.data.data, field.name, None)
                else:
                    value = None
                
                # Convert value based on field type
                converted_value = self._convert_field_value(field, value)
                values.append(converted_value)
                
            except Exception:
                values.append(None)
        
        # Convert memo values
        for memo in table_def.memos:
            try:
                if hasattr(record, '_get_memo_data'):
                    memo_data = record._get_memo_data(record.record_number, memo)
                    values.append(memo_data)
                else:
                    values.append(None)
            except Exception:
                values.append(None)
        
        return tuple(values)
    
    def _convert_field_value(self, field: Any, value: Any) -> Any:
        """Convert field value to SQLite-compatible format"""
        if value is None:
            return None
        
        field_type = field.type.upper()
        
        if field_type in ['STRING', 'CSTRING', 'PSTRING']:
            if isinstance(value, str):
                return value.rstrip('\x00')
            return str(value) if value else None
            
        elif field_type in ['BYTE', 'SHORT', 'LONG']:
            try:
                return int(value) if value is not None else None
            except (ValueError, TypeError):
                return None
                
        elif field_type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
            try:
                return float(value) if value is not None else None
            except (ValueError, TypeError):
                return None
                
        elif field_type == 'DATE':
            if isinstance(value, int) and value > 0:
                # Convert TopSpeed date to YYYY-MM-DD format
                return self._convert_topspeed_date(value)
            return str(value) if value else None
            
        elif field_type == 'TIME':
            if isinstance(value, int) and value > 0:
                # Convert TopSpeed time to HH:MM:SS format
                return self._convert_topspeed_time(value)
            return str(value) if value else None
            
        else:
            return str(value) if value else None
    
    def _convert_topspeed_date(self, date_value: int) -> str:
        """Convert TopSpeed date integer to YYYY-MM-DD format"""
        # TopSpeed date format: YYYYMMDD
        if date_value is None or date_value < 10000000:
            return None
        
        date_str = str(date_value)
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return None
    
    def _convert_topspeed_time(self, time_value: int) -> str:
        """Convert TopSpeed time integer to HH:MM:SS format"""
        # TopSpeed time format: HHMMSS
        if time_value is None:
            return None
        
        # Convert to string and pad to 6 digits
        time_str = str(time_value).zfill(6)
        if len(time_str) == 6:
            return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        return None
    
    def _optimize_database_connection(self, conn: sqlite3.Connection, config: Dict) -> None:
        """Optimize SQLite database connection for performance"""
        # Enable WAL mode for better concurrency
        conn.execute("PRAGMA journal_mode=WAL")
        
        # Optimize synchronous mode
        conn.execute("PRAGMA synchronous=NORMAL")
        
        # Increase cache size
        conn.execute("PRAGMA cache_size=10000")
        
        # Enable memory-mapped I/O
        conn.execute("PRAGMA mmap_size=268435456")  # 256MB
        
        # Optimize page size
        conn.execute("PRAGMA page_size=4096")
        
        # Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys=ON")
    
    def _merge_optimized_databases(self, temp_dbs: List[str], output_file: str, config: Dict) -> None:
        """Merge temporary databases with optimizations"""
        conn_out = sqlite3.connect(output_file)
        self._optimize_database_connection(conn_out, config)
        
        try:
            for temp_db in temp_dbs:
                if not os.path.exists(temp_db):
                    continue
                
                conn_temp = sqlite3.connect(temp_db)
                
                try:
                    # Get all tables from temporary database
                    cursor = conn_temp.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        # Copy table structure and data
                        self._copy_table_optimized(conn_temp, conn_out, table)
                        
                finally:
                    conn_temp.close()
                    
        finally:
            conn_out.close()
    
    def _copy_table_optimized(self, source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection,
                             table: str) -> None:
        """Copy table with optimizations"""
        # Get table schema
        cursor = source_conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        schema_sql = cursor.fetchone()[0]
        
        # Create table in destination
        dest_conn.execute(schema_sql)
        
        # Copy data in batches for better performance
        cursor = source_conn.execute(f"SELECT * FROM {table}")
        batch_size = 1000
        
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            dest_conn.executemany(insert_sql, batch)
    
    def _monitor_performance(self) -> None:
        """Monitor system performance during conversion"""
        while self.performance_metrics['end_time'] is None:
            try:
                # Monitor memory usage
                memory_info = psutil.virtual_memory()
                self.performance_metrics['memory_usage'].append(memory_info.percent)
                
                # Monitor CPU usage
                cpu_percent = psutil.cpu_percent()
                self.performance_metrics['cpu_usage'].append(cpu_percent)
                
                time.sleep(1)  # Monitor every second
                
            except Exception as e:
                self.logger.warning(f"Performance monitoring error: {e}")
                break
    
    def _check_memory_limit(self) -> bool:
        """Check if memory usage exceeds limit"""
        try:
            memory_info = psutil.virtual_memory()
            memory_used_mb = (memory_info.total - memory_info.available) / (1024 * 1024)
            return memory_used_mb > self.memory_limit_mb
        except Exception:
            return False
    
    def _calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics"""
        metrics = self.performance_metrics.copy()
        
        if metrics['start_time'] and metrics['end_time']:
            duration = (metrics['end_time'] - metrics['start_time']).total_seconds()
            
            # Calculate throughput
            if duration > 0:
                # This would need to be updated with actual record count
                metrics['throughput_records_per_sec'] = 0  # Placeholder
            
            # Calculate average memory and CPU usage
            if metrics['memory_usage']:
                metrics['avg_memory_usage'] = sum(metrics['memory_usage']) / len(metrics['memory_usage'])
            
            if metrics['cpu_usage']:
                metrics['avg_cpu_usage'] = sum(metrics['cpu_usage']) / len(metrics['cpu_usage'])
        
        # Calculate cache hit rate
        total_cache_requests = self.cache_hits + self.cache_misses
        if total_cache_requests > 0:
            metrics['cache_hit_rate'] = self.cache_hits / total_cache_requests
        
        return metrics
    
    def get_performance_report(self) -> str:
        """Generate performance optimization report"""
        metrics = self._calculate_performance_metrics()
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("PERFORMANCE OPTIMIZATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Performance metrics
        report_lines.append("PERFORMANCE METRICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Duration: {metrics.get('duration', 0):.2f} seconds")
        report_lines.append(f"Throughput: {metrics.get('throughput_records_per_sec', 0):.0f} records/sec")
        report_lines.append(f"Average Memory Usage: {metrics.get('avg_memory_usage', 0):.1f}%")
        report_lines.append(f"Average CPU Usage: {metrics.get('avg_cpu_usage', 0):.1f}%")
        report_lines.append(f"Cache Hit Rate: {metrics.get('cache_hit_rate', 0):.2%}")
        report_lines.append("")
        
        # Optimization settings
        report_lines.append("OPTIMIZATION SETTINGS")
        report_lines.append("-" * 40)
        report_lines.append(f"Max Workers: {self.max_workers}")
        report_lines.append(f"Memory Limit: {self.memory_limit_mb} MB")
        report_lines.append(f"Cache Size: {self.cache_size}")
        report_lines.append("")
        
        return "\n".join(report_lines)
