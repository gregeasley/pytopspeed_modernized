#!/usr/bin/env python3
"""
Data Validator - Verify conversion accuracy and generate comparison reports

This module provides comprehensive data validation capabilities to ensure
conversion accuracy and identify data inconsistencies.
"""

import os
import sqlite3
import logging
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path
from collections import defaultdict, Counter

from pytopspeed.tps import TPS


class DataValidator:
    """
    Comprehensive data validator for TopSpeed to SQLite conversions
    """
    
    def __init__(self, progress_callback=None):
        """
        Initialize data validator
        
        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # Validation results
        self.validation_results = {
            'success': False,
            'total_tables': 0,
            'total_records': 0,
            'validation_errors': [],
            'data_inconsistencies': [],
            'missing_data': [],
            'extra_data': [],
            'type_mismatches': [],
            'integrity_issues': [],
            'statistics': {},
            'duration': 0
        }
    
    def validate_conversion(self, 
                          topspeed_file: str, 
                          sqlite_file: str,
                          validation_level: str = 'comprehensive',
                          generate_report: bool = True) -> Dict[str, Any]:
        """
        Validate TopSpeed to SQLite conversion
        
        Args:
            topspeed_file: Path to original TopSpeed file
            sqlite_file: Path to converted SQLite file
            validation_level: Level of validation ('basic', 'standard', 'comprehensive')
            generate_report: Whether to generate a detailed validation report
            
        Returns:
            Dictionary with validation results
        """
        start_time = datetime.now()
        results = self.validation_results.copy()
        
        try:
            self.logger.info(f"Starting validation: {topspeed_file} -> {sqlite_file}")
            
            # Load TopSpeed file
            tps = TPS(topspeed_file)
            
            # Load SQLite database
            conn = sqlite3.connect(sqlite_file)
            conn.row_factory = sqlite3.Row
            
            # Basic validation
            results.update(self._validate_basic_structure(tps, conn))
            
            if validation_level in ['standard', 'comprehensive']:
                # Standard validation
                results.update(self._validate_data_integrity(tps, conn))
                
            if validation_level == 'comprehensive':
                # Comprehensive validation
                results.update(self._validate_comprehensive(tps, conn))
            
            # Generate report if requested
            if generate_report:
                report_file = f"{sqlite_file}_validation_report.txt"
                self._generate_validation_report(results, report_file)
                results['report_file'] = report_file
            
            results['success'] = len(results['validation_errors']) == 0
            
        except Exception as e:
            error_msg = f"Validation failed: {e}"
            self.logger.error(error_msg)
            results['validation_errors'].append(error_msg)
            
        finally:
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def _validate_basic_structure(self, tps: TPS, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Validate basic structure (tables, records)"""
        results = {
            'structure_validation': {
                'tables_match': True,
                'record_counts_match': True,
                'missing_tables': [],
                'extra_tables': [],
                'record_count_differences': {}
            }
        }
        
        # Get TopSpeed tables
        tps_tables = set()
        tps_record_counts = {}
        
        for table_name in tps.tables:
            tps_tables.add(table_name)
            try:
                tps.set_current_table(table_name)
                record_count = sum(1 for _ in tps)
                tps_record_counts[table_name] = record_count
            except Exception as e:
                self.logger.warning(f"Could not count records for table {table_name}: {e}")
                tps_record_counts[table_name] = 0
        
        # Get SQLite tables
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        sqlite_tables = {row[0] for row in cursor.fetchall()}
        
        # Check for missing tables
        missing_tables = tps_tables - sqlite_tables
        if missing_tables:
            results['structure_validation']['tables_match'] = False
            results['structure_validation']['missing_tables'] = list(missing_tables)
            self.validation_results['validation_errors'].append(
                f"Missing tables in SQLite: {missing_tables}"
            )
        
        # Check for extra tables
        extra_tables = sqlite_tables - tps_tables
        if extra_tables:
            results['structure_validation']['extra_tables'] = list(extra_tables)
        
        # Validate record counts
        for table_name in tps_tables.intersection(sqlite_tables):
            try:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                sqlite_count = cursor.fetchone()[0]
                tps_count = tps_record_counts.get(table_name, 0)
                
                if tps_count != sqlite_count:
                    results['structure_validation']['record_counts_match'] = False
                    results['structure_validation']['record_count_differences'][table_name] = {
                        'topspeed': tps_count,
                        'sqlite': sqlite_count,
                        'difference': sqlite_count - tps_count
                    }
                    
                    self.validation_results['validation_errors'].append(
                        f"Record count mismatch in {table_name}: TopSpeed={tps_count}, SQLite={sqlite_count}"
                    )
                    
            except Exception as e:
                self.logger.warning(f"Could not validate record count for {table_name}: {e}")
        
        return results
    
    def _validate_data_integrity(self, tps: TPS, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Validate data integrity and consistency"""
        results = {
            'integrity_validation': {
                'data_checksums': {},
                'null_value_analysis': {},
                'data_type_validation': {},
                'constraint_violations': []
            }
        }
        
        # Sample validation for each table
        for table_name in tps.tables:
            try:
                tps.set_current_table(table_name)
                table_def = tps.tables.get_definition(table_name)
                
                if not table_def:
                    continue
                
                # Get sample records from TopSpeed
                tps_records = []
                for i, record in enumerate(tps):
                    if i >= 100:  # Sample first 100 records
                        break
                    tps_records.append(self._extract_record_data(record, table_def))
                
                # Get corresponding records from SQLite
                cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 100")
                sqlite_records = [dict(row) for row in cursor.fetchall()]
                
                # Compare data
                self._compare_record_data(table_name, tps_records, sqlite_records, results)
                
            except Exception as e:
                self.logger.warning(f"Could not validate data integrity for {table_name}: {e}")
        
        return results
    
    def _validate_comprehensive(self, tps: TPS, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Comprehensive validation including statistical analysis"""
        results = {
            'comprehensive_validation': {
                'statistical_analysis': {},
                'data_distribution': {},
                'anomaly_detection': {},
                'performance_metrics': {}
            }
        }
        
        # Statistical analysis for each table
        for table_name in tps.tables:
            try:
                stats = self._analyze_table_statistics(tps, conn, table_name)
                results['comprehensive_validation']['statistical_analysis'][table_name] = stats
                
            except Exception as e:
                self.logger.warning(f"Could not perform comprehensive validation for {table_name}: {e}")
        
        return results
    
    def _extract_record_data(self, record, table_def) -> Dict[str, Any]:
        """Extract data from TopSpeed record"""
        data = {}
        
        # Extract field data
        for field in table_def.fields:
            try:
                if hasattr(record, 'data') and hasattr(record.data, 'data'):
                    field_data = getattr(record.data.data, field.name, None)
                    data[field.name] = field_data
                else:
                    data[field.name] = None
            except Exception:
                data[field.name] = None
        
        # Extract memo data
        for memo in table_def.memos:
            try:
                if hasattr(record, '_get_memo_data'):
                    memo_data = record._get_memo_data(record.record_number, memo)
                    data[memo.name] = memo_data
                else:
                    data[memo.name] = None
            except Exception:
                data[memo.name] = None
        
        return data
    
    def _compare_record_data(self, table_name: str, tps_records: List[Dict], 
                           sqlite_records: List[Dict], results: Dict) -> None:
        """Compare TopSpeed and SQLite record data"""
        if not tps_records or not sqlite_records:
            return
        
        # Compare field by field
        tps_fields = set(tps_records[0].keys()) if tps_records else set()
        sqlite_fields = set(sqlite_records[0].keys()) if sqlite_records else set()
        
        # Check for missing fields
        missing_fields = tps_fields - sqlite_fields
        if missing_fields:
            self.validation_results['validation_errors'].append(
                f"Missing fields in {table_name}: {missing_fields}"
            )
        
        # Check for extra fields
        extra_fields = sqlite_fields - tps_fields
        if extra_fields:
            self.validation_results['validation_errors'].append(
                f"Extra fields in {table_name}: {extra_fields}"
            )
        
        # Compare data values (sample comparison)
        min_records = min(len(tps_records), len(sqlite_records))
        for i in range(min_records):
            tps_record = tps_records[i]
            sqlite_record = sqlite_records[i]
            
            for field in tps_fields.intersection(sqlite_fields):
                tps_value = tps_record.get(field)
                sqlite_value = sqlite_record.get(field)
                
                # Normalize values for comparison
                tps_normalized = self._normalize_value(tps_value)
                sqlite_normalized = self._normalize_value(sqlite_value)
                
                if tps_normalized != sqlite_normalized:
                    results['data_inconsistencies'].append({
                        'table': table_name,
                        'record': i,
                        'field': field,
                        'topspeed_value': tps_value,
                        'sqlite_value': sqlite_value
                    })
    
    def _normalize_value(self, value) -> Any:
        """Normalize value for comparison"""
        if value is None:
            return None
        
        if isinstance(value, str):
            # Remove trailing nulls and whitespace
            return value.rstrip('\x00').strip()
        
        if isinstance(value, (int, float)):
            return value
        
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='ignore')
        
        return str(value)
    
    def _analyze_table_statistics(self, tps: TPS, conn: sqlite3.Connection, 
                                 table_name: str) -> Dict[str, Any]:
        """Analyze statistical properties of a table"""
        stats = {
            'record_count': 0,
            'field_statistics': {},
            'data_quality_metrics': {},
            'anomalies': []
        }
        
        try:
            # Get record count
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            stats['record_count'] = cursor.fetchone()[0]
            
            # Get field statistics
            cursor = conn.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for column in columns:
                col_name = column[1]
                col_type = column[2]
                
                # Analyze column data
                try:
                    cursor = conn.execute(f"SELECT {col_name} FROM {table_name}")
                    values = [row[0] for row in cursor.fetchall()]
                    
                    col_stats = self._analyze_column_data(values, col_type)
                    stats['field_statistics'][col_name] = col_stats
                    
                except Exception as e:
                    self.logger.warning(f"Could not analyze column {col_name}: {e}")
            
        except Exception as e:
            self.logger.warning(f"Could not analyze statistics for {table_name}: {e}")
        
        return stats
    
    def _analyze_column_data(self, values: List[Any], col_type: str) -> Dict[str, Any]:
        """Analyze data in a column"""
        stats = {
            'total_values': len(values),
            'null_count': 0,
            'unique_count': 0,
            'data_type': col_type,
            'min_value': None,
            'max_value': None,
            'avg_value': None
        }
        
        # Filter out None values
        non_null_values = [v for v in values if v is not None]
        stats['null_count'] = len(values) - len(non_null_values)
        stats['unique_count'] = len(set(non_null_values))
        
        if non_null_values:
            # Type-specific analysis
            if col_type.upper() in ['INTEGER', 'REAL']:
                try:
                    numeric_values = [float(v) for v in non_null_values if str(v).replace('.', '').replace('-', '').isdigit()]
                    if numeric_values:
                        stats['min_value'] = min(numeric_values)
                        stats['max_value'] = max(numeric_values)
                        stats['avg_value'] = sum(numeric_values) / len(numeric_values)
                except (ValueError, TypeError):
                    pass
            
            elif col_type.upper() == 'TEXT':
                try:
                    text_lengths = [len(str(v)) for v in non_null_values]
                    if text_lengths:
                        stats['min_length'] = min(text_lengths)
                        stats['max_length'] = max(text_lengths)
                        stats['avg_length'] = sum(text_lengths) / len(text_lengths)
                except (ValueError, TypeError):
                    pass
        
        return stats
    
    def _generate_validation_report(self, results: Dict[str, Any], output_file: str) -> None:
        """Generate detailed validation report"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("DATA VALIDATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("")
        
        # Summary
        report_lines.append("VALIDATION SUMMARY")
        report_lines.append("-" * 40)
        report_lines.append(f"Success: {results['success']}")
        report_lines.append(f"Total Tables: {results['total_tables']}")
        report_lines.append(f"Total Records: {results['total_records']}")
        report_lines.append(f"Validation Errors: {len(results['validation_errors'])}")
        report_lines.append(f"Data Inconsistencies: {len(results['data_inconsistencies'])}")
        report_lines.append(f"Duration: {results['duration']:.2f} seconds")
        report_lines.append("")
        
        # Structure validation
        if 'structure_validation' in results:
            struct_val = results['structure_validation']
            report_lines.append("STRUCTURE VALIDATION")
            report_lines.append("-" * 40)
            report_lines.append(f"Tables Match: {struct_val['tables_match']}")
            report_lines.append(f"Record Counts Match: {struct_val['record_counts_match']}")
            
            if struct_val['missing_tables']:
                report_lines.append(f"Missing Tables: {struct_val['missing_tables']}")
            
            if struct_val['extra_tables']:
                report_lines.append(f"Extra Tables: {struct_val['extra_tables']}")
            
            if struct_val['record_count_differences']:
                report_lines.append("Record Count Differences:")
                for table, diff in struct_val['record_count_differences'].items():
                    report_lines.append(f"  {table}: TopSpeed={diff['topspeed']}, SQLite={diff['sqlite']}")
            
            report_lines.append("")
        
        # Validation errors
        if results['validation_errors']:
            report_lines.append("VALIDATION ERRORS")
            report_lines.append("-" * 40)
            for error in results['validation_errors']:
                report_lines.append(f"  {error}")
            report_lines.append("")
        
        # Data inconsistencies
        if results['data_inconsistencies']:
            report_lines.append("DATA INCONSISTENCIES")
            report_lines.append("-" * 40)
            for inconsistency in results['data_inconsistencies'][:10]:  # Show first 10
                report_lines.append(f"  Table: {inconsistency['table']}, Field: {inconsistency['field']}")
                report_lines.append(f"    TopSpeed: {inconsistency['topspeed_value']}")
                report_lines.append(f"    SQLite: {inconsistency['sqlite_value']}")
                report_lines.append("")
            
            if len(results['data_inconsistencies']) > 10:
                report_lines.append(f"  ... and {len(results['data_inconsistencies']) - 10} more")
            report_lines.append("")
        
        # Write report
        with open(output_file, 'w') as f:
            f.write('\n'.join(report_lines))
        
        self.logger.info(f"Validation report written to {output_file}")
    
    def compare_databases(self, db1_file: str, db2_file: str) -> Dict[str, Any]:
        """
        Compare two SQLite databases for differences
        
        Args:
            db1_file: Path to first SQLite database
            db2_file: Path to second SQLite database
            
        Returns:
            Dictionary with comparison results
        """
        results = {
            'success': False,
            'differences': [],
            'table_comparisons': {},
            'schema_differences': [],
            'data_differences': []
        }
        
        try:
            conn1 = sqlite3.connect(db1_file)
            conn2 = sqlite3.connect(db2_file)
            
            # Compare schemas
            schema1 = self._get_database_schema(conn1)
            schema2 = self._get_database_schema(conn2)
            
            # Find schema differences
            tables1 = set(schema1.keys())
            tables2 = set(schema2.keys())
            
            missing_tables = tables1 - tables2
            extra_tables = tables2 - tables1
            
            if missing_tables:
                results['schema_differences'].append(f"Missing tables in db2: {missing_tables}")
            
            if extra_tables:
                results['schema_differences'].append(f"Extra tables in db2: {extra_tables}")
            
            # Compare common tables
            common_tables = tables1.intersection(tables2)
            for table in common_tables:
                table_diff = self._compare_table_data(conn1, conn2, table)
                if table_diff:
                    results['table_comparisons'][table] = table_diff
            
            results['success'] = True
            
        except Exception as e:
            self.logger.error(f"Database comparison failed: {e}")
            results['error'] = str(e)
        
        return results
    
    def _get_database_schema(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        """Get database schema information"""
        schema = {}
        
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            cursor = conn.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            schema[table] = {
                'columns': [{'name': col[1], 'type': col[2], 'notnull': col[3]} for col in columns],
                'record_count': conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            }
        
        return schema
    
    def _compare_table_data(self, conn1: sqlite3.Connection, conn2: sqlite3.Connection, 
                           table: str) -> Dict[str, Any]:
        """Compare data in a specific table between two databases"""
        differences = {
            'record_count_diff': 0,
            'data_differences': []
        }
        
        try:
            # Compare record counts
            count1 = conn1.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            count2 = conn2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            differences['record_count_diff'] = count2 - count1
            
            # Sample data comparison (first 100 records)
            cursor1 = conn1.execute(f"SELECT * FROM {table} LIMIT 100")
            cursor2 = conn2.execute(f"SELECT * FROM {table} LIMIT 100")
            
            # Get column names
            columns1 = [description[0] for description in cursor1.description]
            columns2 = [description[0] for description in cursor2.description]
            
            rows1 = [dict(zip(columns1, row)) for row in cursor1.fetchall()]
            rows2 = [dict(zip(columns2, row)) for row in cursor2.fetchall()]
            
            # Compare rows
            min_rows = min(len(rows1), len(rows2))
            for i in range(min_rows):
                if rows1[i] != rows2[i]:
                    differences['data_differences'].append({
                        'row': i,
                        'db1_data': rows1[i],
                        'db2_data': rows2[i]
                    })
            
        except Exception as e:
            self.logger.warning(f"Could not compare table {table}: {e}")
        
        return differences
