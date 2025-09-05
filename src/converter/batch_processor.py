#!/usr/bin/env python3
"""
Advanced Batch Processor - Handle multiple TopSpeed files with cross-file relationships

This module provides advanced batch processing capabilities for converting multiple
TopSpeed files while maintaining relationships and optimizing performance.
"""

import os
import sqlite3
import logging
import tempfile
import shutil
from datetime import datetime
from typing import Dict, List, Any, Optional, Set, Tuple
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp

from .sqlite_converter import SqliteConverter
from .schema_mapper import TopSpeedToSQLiteMapper
from pytopspeed.tps import TPS


class BatchProcessor:
    """
    Advanced batch processor for handling multiple TopSpeed files with relationships
    """
    
    def __init__(self, 
                 batch_size: int = 1000,
                 max_workers: int = None,
                 progress_callback=None,
                 temp_dir: str = None):
        """
        Initialize batch processor
        
        Args:
            batch_size: Number of records to process in each batch
            max_workers: Maximum number of worker processes/threads
            progress_callback: Optional callback function for progress updates
            temp_dir: Temporary directory for intermediate files
        """
        self.batch_size = batch_size
        self.max_workers = max_workers or min(mp.cpu_count(), 4)
        self.progress_callback = progress_callback
        self.temp_dir = temp_dir or tempfile.gettempdir()
        self.logger = logging.getLogger(__name__)
        
        # Initialize components
        self.sqlite_converter = SqliteConverter(batch_size, progress_callback)
        self.schema_mapper = TopSpeedToSQLiteMapper()
        
        # Relationship tracking
        self.file_relationships = {}
        self.cross_references = {}
        self.global_schema = {}
        
    def process_batch(self, 
                     input_files: List[str], 
                     output_file: str,
                     merge_strategy: str = 'prefix',
                     relationship_analysis: bool = True,
                     parallel_processing: bool = True) -> Dict[str, Any]:
        """
        Process multiple TopSpeed files with advanced batch processing
        
        Args:
            input_files: List of TopSpeed files to process
            output_file: Output SQLite database file
            merge_strategy: Strategy for merging files ('prefix', 'namespace', 'separate')
            relationship_analysis: Whether to analyze cross-file relationships
            parallel_processing: Whether to use parallel processing
            
        Returns:
            Dictionary with processing results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'files_processed': 0,
            'tables_created': 0,
            'total_records': 0,
            'relationships_found': 0,
            'duration': 0,
            'errors': [],
            'warnings': [],
            'file_details': {},
            'relationship_map': {}
        }
        
        try:
            self.logger.info(f"Starting batch processing of {len(input_files)} files")
            
            # Validate input files
            valid_files = self._validate_input_files(input_files)
            if not valid_files:
                results['errors'].append("No valid input files found")
                return results
            
            # Analyze relationships if requested
            if relationship_analysis:
                self.logger.info("Analyzing cross-file relationships...")
                relationships = self._analyze_relationships(valid_files)
                results['relationship_map'] = relationships
                results['relationships_found'] = len(relationships)
            
            # Create temporary directory for intermediate processing
            temp_dir = tempfile.mkdtemp(prefix="batch_process_", dir=self.temp_dir)
            
            try:
                if parallel_processing and len(valid_files) > 1:
                    # Parallel processing
                    results = self._process_parallel(valid_files, output_file, temp_dir, 
                                                   merge_strategy, results)
                else:
                    # Sequential processing
                    results = self._process_sequential(valid_files, output_file, temp_dir,
                                                     merge_strategy, results)
                
                results['success'] = len(results['errors']) == 0
                
            finally:
                # Clean up temporary directory
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
                    
        except Exception as e:
            error_msg = f"Batch processing failed: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def _validate_input_files(self, input_files: List[str]) -> List[str]:
        """Validate input files and return list of valid files"""
        valid_files = []
        
        for file_path in input_files:
            if not os.path.exists(file_path):
                self.logger.warning(f"File not found: {file_path}")
                continue
                
            if not self._is_topspeed_file(file_path):
                self.logger.warning(f"Not a TopSpeed file: {file_path}")
                continue
                
            valid_files.append(file_path)
            
        return valid_files
    
    def _is_topspeed_file(self, file_path: str) -> bool:
        """Check if file is a TopSpeed file"""
        ext = os.path.splitext(file_path)[1].lower()
        return ext in ['.phd', '.mod', '.tps']
    
    def _analyze_relationships(self, files: List[str]) -> Dict[str, Any]:
        """
        Analyze cross-file relationships by examining table names and data
        
        Returns:
            Dictionary mapping relationships between files
        """
        relationships = {
            'table_overlaps': {},
            'data_relationships': {},
            'schema_similarities': {}
        }
        
        file_schemas = {}
        
        # Load schemas for each file
        for file_path in files:
            try:
                tps = TPS(file_path)
                schema = self._extract_schema(tps)
                file_schemas[file_path] = schema
            except Exception as e:
                self.logger.warning(f"Failed to load schema for {file_path}: {e}")
                continue
        
        # Find table overlaps
        table_names = {}
        for file_path, schema in file_schemas.items():
            for table_name in schema.get('tables', {}):
                if table_name not in table_names:
                    table_names[table_name] = []
                table_names[table_name].append(file_path)
        
        # Identify overlapping tables
        for table_name, files_with_table in table_names.items():
            if len(files_with_table) > 1:
                relationships['table_overlaps'][table_name] = files_with_table
        
        # Analyze schema similarities
        for file1, schema1 in file_schemas.items():
            for file2, schema2 in file_schemas.items():
                if file1 != file2:
                    similarity = self._calculate_schema_similarity(schema1, schema2)
                    if similarity > 0.5:  # 50% similarity threshold
                        key = f"{os.path.basename(file1)}_vs_{os.path.basename(file2)}"
                        relationships['schema_similarities'][key] = similarity
        
        return relationships
    
    def _extract_schema(self, tps: TPS) -> Dict[str, Any]:
        """Extract schema information from TPS object"""
        schema = {
            'tables': {},
            'total_tables': 0,
            'total_records': 0
        }
        
        try:
            for table_name in tps.tables:
                table_def = tps.tables.get_definition(table_name)
                if table_def:
                    schema['tables'][table_name] = {
                        'fields': len(table_def.fields),
                        'indexes': len(table_def.indexes),
                        'memos': len(table_def.memos)
                    }
                    schema['total_tables'] += 1
                    
                    # Count records (approximate)
                    try:
                        tps.set_current_table(table_name)
                        record_count = sum(1 for _ in tps)
                        schema['tables'][table_name]['record_count'] = record_count
                        schema['total_records'] += record_count
                    except Exception:
                        schema['tables'][table_name]['record_count'] = 0
                        
        except Exception as e:
            self.logger.warning(f"Error extracting schema: {e}")
            
        return schema
    
    def _calculate_schema_similarity(self, schema1: Dict, schema2: Dict) -> float:
        """Calculate similarity between two schemas"""
        tables1 = set(schema1.get('tables', {}).keys())
        tables2 = set(schema2.get('tables', {}).keys())
        
        if not tables1 or not tables2:
            return 0.0
            
        intersection = len(tables1.intersection(tables2))
        union = len(tables1.union(tables2))
        
        return intersection / union if union > 0 else 0.0
    
    def _process_parallel(self, files: List[str], output_file: str, temp_dir: str,
                         merge_strategy: str, results: Dict) -> Dict[str, Any]:
        """Process files in parallel"""
        self.logger.info(f"Processing {len(files)} files in parallel with {self.max_workers} workers")
        
        # Create temporary databases for each file
        temp_dbs = []
        for i, file_path in enumerate(files):
            temp_db = os.path.join(temp_dir, f"temp_{i}.sqlite")
            temp_dbs.append(temp_db)
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {}
            
            for i, file_path in enumerate(files):
                future = executor.submit(self._process_single_file, file_path, temp_dbs[i])
                future_to_file[future] = (file_path, i)
            
            # Collect results
            for future in as_completed(future_to_file):
                file_path, file_index = future_to_file[future]
                try:
                    file_results = future.result()
                    results['file_details'][file_path] = file_results
                    results['tables_created'] += file_results.get('tables_created', 0)
                    results['total_records'] += file_results.get('total_records', 0)
                    results['files_processed'] += 1
                    
                    if self.progress_callback:
                        self.progress_callback(results['files_processed'], len(files), 
                                             f"Processed {os.path.basename(file_path)}")
                    
                except Exception as e:
                    error_msg = f"Failed to process {file_path}: {e}"
                    self.logger.error(error_msg)
                    results['errors'].append(error_msg)
        
        # Merge temporary databases
        if results['files_processed'] > 0:
            self._merge_databases(temp_dbs, output_file, merge_strategy, results)
        
        return results
    
    def _process_sequential(self, files: List[str], output_file: str, temp_dir: str,
                           merge_strategy: str, results: Dict) -> Dict[str, Any]:
        """Process files sequentially"""
        self.logger.info(f"Processing {len(files)} files sequentially")
        
        temp_dbs = []
        
        for i, file_path in enumerate(files):
            temp_db = os.path.join(temp_dir, f"temp_{i}.sqlite")
            
            try:
                file_results = self._process_single_file(file_path, temp_db)
                results['file_details'][file_path] = file_results
                results['tables_created'] += file_results.get('tables_created', 0)
                results['total_records'] += file_results.get('total_records', 0)
                results['files_processed'] += 1
                temp_dbs.append(temp_db)
                
                if self.progress_callback:
                    self.progress_callback(results['files_processed'], len(files),
                                         f"Processed {os.path.basename(file_path)}")
                
            except Exception as e:
                error_msg = f"Failed to process {file_path}: {e}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
        
        # Merge databases
        if temp_dbs:
            self._merge_databases(temp_dbs, output_file, merge_strategy, results)
        
        return results
    
    def _process_single_file(self, file_path: str, output_db: str) -> Dict[str, Any]:
        """Process a single TopSpeed file"""
        converter = SqliteConverter(self.batch_size)
        return converter.convert(file_path, output_db)
    
    def _merge_databases(self, temp_dbs: List[str], output_file: str, 
                        merge_strategy: str, results: Dict) -> None:
        """Merge multiple SQLite databases into one"""
        self.logger.info(f"Merging {len(temp_dbs)} databases using {merge_strategy} strategy")
        
        # Create output database
        conn_out = sqlite3.connect(output_file)
        conn_out.execute("PRAGMA journal_mode=WAL")
        conn_out.execute("PRAGMA synchronous=NORMAL")
        
        try:
            for i, temp_db in enumerate(temp_dbs):
                if not os.path.exists(temp_db):
                    continue
                    
                conn_temp = sqlite3.connect(temp_db)
                
                try:
                    # Get all tables from temporary database
                    cursor = conn_temp.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        # Apply merge strategy to table name
                        if merge_strategy == 'prefix':
                            # Extract file index from temp database name
                            file_index = i
                            new_table_name = f"file_{file_index}_{table}"
                        elif merge_strategy == 'namespace':
                            new_table_name = f"db_{i}_{table}"
                        else:  # separate
                            new_table_name = table
                        
                        # Copy table structure and data
                        self._copy_table(conn_temp, conn_out, table, new_table_name)
                        
                finally:
                    conn_temp.close()
                    
        finally:
            conn_out.close()
    
    def _copy_table(self, source_conn: sqlite3.Connection, dest_conn: sqlite3.Connection,
                   source_table: str, dest_table: str) -> None:
        """Copy a table from source to destination database"""
        # Get table schema
        cursor = source_conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{source_table}'")
        schema_sql = cursor.fetchone()[0]
        
        # Modify table name in schema - handle potential path issues
        dest_schema_sql = schema_sql.replace(f"CREATE TABLE {source_table}", f"CREATE TABLE {dest_table}")
        
        # Create table in destination
        dest_conn.execute(dest_schema_sql)
        
        # Copy data using parameterized query to avoid path issues
        cursor = source_conn.execute(f"SELECT * FROM {source_table}")
        rows = cursor.fetchall()
        
        if rows:
            # Get column names
            columns = [description[0] for description in cursor.description]
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {dest_table} ({', '.join(columns)}) VALUES ({placeholders})"
            dest_conn.executemany(insert_sql, rows)
        
        dest_conn.commit()
    
    def generate_batch_report(self, results: Dict[str, Any], output_file: str = None) -> str:
        """
        Generate a detailed batch processing report
        
        Args:
            results: Batch processing results
            output_file: Optional output file for the report
            
        Returns:
            Report content as string
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("BATCH PROCESSING REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Summary
        report_lines.append("SUMMARY")
        report_lines.append("-" * 40)
        report_lines.append(f"Success: {results['success']}")
        report_lines.append(f"Files Processed: {results['files_processed']}")
        report_lines.append(f"Tables Created: {results['tables_created']}")
        report_lines.append(f"Total Records: {results['total_records']}")
        report_lines.append(f"Relationships Found: {results['relationships_found']}")
        report_lines.append(f"Duration: {results['duration']:.2f} seconds")
        report_lines.append("")
        
        # File Details
        if results['file_details']:
            report_lines.append("FILE DETAILS")
            report_lines.append("-" * 40)
            for file_path, details in results['file_details'].items():
                report_lines.append(f"File: {os.path.basename(file_path)}")
                report_lines.append(f"  Tables: {details.get('tables_created', 0)}")
                report_lines.append(f"  Records: {details.get('total_records', 0)}")
                report_lines.append(f"  Duration: {details.get('duration', 0):.2f}s")
                report_lines.append("")
        
        # Relationships
        if results['relationship_map']:
            report_lines.append("RELATIONSHIPS")
            report_lines.append("-" * 40)
            relationships = results['relationship_map']
            
            if relationships.get('table_overlaps'):
                report_lines.append("Table Overlaps:")
                for table, files in relationships['table_overlaps'].items():
                    report_lines.append(f"  {table}: {', '.join([os.path.basename(f) for f in files])}")
                report_lines.append("")
            
            if relationships.get('schema_similarities'):
                report_lines.append("Schema Similarities:")
                for comparison, similarity in relationships['schema_similarities'].items():
                    report_lines.append(f"  {comparison}: {similarity:.2%}")
                report_lines.append("")
        
        # Errors and Warnings
        if results['errors']:
            report_lines.append("ERRORS")
            report_lines.append("-" * 40)
            for error in results['errors']:
                report_lines.append(f"  {error}")
            report_lines.append("")
        
        if results['warnings']:
            report_lines.append("WARNINGS")
            report_lines.append("-" * 40)
            for warning in results['warnings']:
                report_lines.append(f"  {warning}")
            report_lines.append("")
        
        report_content = "\n".join(report_lines)
        
        # Write to file if specified
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_content)
            self.logger.info(f"Batch report written to {output_file}")
        
        return report_content
