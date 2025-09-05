#!/usr/bin/env python3
"""
Unit tests for BatchProcessor
"""

import pytest
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from converter.batch_processor import BatchProcessor


class TestBatchProcessor:
    """Test cases for BatchProcessor"""
    
    def test_initialization(self):
        """Test BatchProcessor initialization"""
        processor = BatchProcessor(
            batch_size=500,
            max_workers=2,
            progress_callback=lambda x, y, z: None
        )
        
        assert processor.batch_size == 500
        assert processor.max_workers == 2
        assert processor.progress_callback is not None
        assert processor.file_relationships == {}
        assert processor.cross_references == {}
        assert processor.global_schema == {}
    
    def test_validate_input_files(self):
        """Test input file validation"""
        processor = BatchProcessor()
        
        # Test with non-existent files
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent = os.path.join(temp_dir, "nonexistent.phd")
            result = processor._validate_input_files([non_existent])
            assert result == []
        
        # Test with valid files (mocked)
        with patch('os.path.exists', return_value=True):
            with patch.object(processor, '_is_topspeed_file', return_value=True):
                result = processor._validate_input_files(["test.phd", "test.mod"])
                assert result == ["test.phd", "test.mod"]
    
    def test_is_topspeed_file(self):
        """Test TopSpeed file detection"""
        processor = BatchProcessor()
        
        assert processor._is_topspeed_file("test.phd") is True
        assert processor._is_topspeed_file("test.mod") is True
        assert processor._is_topspeed_file("test.tps") is True
        assert processor._is_topspeed_file("test.txt") is False
        assert processor._is_topspeed_file("test") is False
    
    def test_analyze_relationships(self):
        """Test relationship analysis"""
        processor = BatchProcessor()
        
        # Mock TPS objects
        mock_tps1 = Mock()
        mock_tps2 = Mock()
        
        # Mock table definitions
        mock_table_def1 = Mock()
        mock_table_def1.fields = [Mock(name="field1"), Mock(name="field2")]
        mock_table_def1.indexes = [Mock()]
        mock_table_def1.memos = [Mock()]
        
        mock_table_def2 = Mock()
        mock_table_def2.fields = [Mock(name="field1"), Mock(name="field3")]
        mock_table_def2.indexes = [Mock()]
        mock_table_def2.memos = [Mock()]
        
        # Mock TPS methods
        mock_tables1 = Mock()
        mock_tables1.__iter__ = Mock(return_value=iter(["TABLE1", "TABLE2"]))
        mock_tables1.get_definition = Mock(side_effect=lambda name: mock_table_def1 if name == "TABLE1" else mock_table_def2)
        mock_tps1.tables = mock_tables1
        mock_tps1.set_current_table = Mock()
        mock_tps1.__iter__ = Mock(return_value=iter([Mock(), Mock()]))
        
        mock_tables2 = Mock()
        mock_tables2.__iter__ = Mock(return_value=iter(["TABLE1", "TABLE3"]))
        mock_tables2.get_definition = Mock(side_effect=lambda name: mock_table_def1 if name == "TABLE1" else mock_table_def2)
        mock_tps2.tables = mock_tables2
        mock_tps2.set_current_table = Mock()
        mock_tps2.__iter__ = Mock(return_value=iter([Mock()]))
        
        # Mock TPS constructor
        with patch('converter.batch_processor.TPS', side_effect=[mock_tps1, mock_tps2]):
            relationships = processor._analyze_relationships(["file1.phd", "file2.phd"])
            
            assert "table_overlaps" in relationships
            assert "schema_similarities" in relationships
            assert "TABLE1" in relationships["table_overlaps"]
    
    def test_extract_schema(self):
        """Test schema extraction"""
        processor = BatchProcessor()
        
        # Mock table definition first
        mock_table_def = Mock()
        mock_table_def.fields = [Mock(name="field1"), Mock(name="field2")]
        mock_table_def.indexes = [Mock()]
        mock_table_def.memos = [Mock()]

        # Mock TPS object
        mock_tps = Mock()
        mock_tables = Mock()
        mock_tables.__iter__ = Mock(return_value=iter(["TABLE1"]))
        mock_tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.tables = mock_tables

        mock_tps.set_current_table = Mock()
        mock_tps.__iter__ = Mock(return_value=iter([Mock(), Mock()]))
        
        schema = processor._extract_schema(mock_tps)
        
        assert "tables" in schema
        assert "total_tables" in schema
        assert "total_records" in schema
        assert "TABLE1" in schema["tables"]
        assert schema["total_tables"] == 1
    
    def test_calculate_schema_similarity(self):
        """Test schema similarity calculation"""
        processor = BatchProcessor()
        
        schema1 = {"tables": {"TABLE1": {}, "TABLE2": {}}}
        schema2 = {"tables": {"TABLE1": {}, "TABLE3": {}}}
        
        similarity = processor._calculate_schema_similarity(schema1, schema2)
        assert similarity == 1/3  # 1 common table out of 3 total unique tables
    
    def test_process_single_file(self):
        """Test single file processing"""
        processor = BatchProcessor()
        
        with patch('converter.batch_processor.SqliteConverter') as mock_converter_class:
            mock_converter = Mock()
            mock_converter.convert.return_value = {
                'success': True,
                'tables_created': 5,
                'total_records': 100
            }
            mock_converter_class.return_value = mock_converter
            
            result = processor._process_single_file("test.phd", "output.sqlite")
            
            assert result['success'] is True
            assert result['tables_created'] == 5
            assert result['total_records'] == 100
            mock_converter.convert.assert_called_once_with("test.phd", "output.sqlite")
    
    def test_merge_databases(self):
        """Test database merging"""
        processor = BatchProcessor()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create temporary databases
            temp_db1 = os.path.join(temp_dir, "temp1.sqlite")
            temp_db2 = os.path.join(temp_dir, "temp2.sqlite")
            output_db = os.path.join(temp_dir, "output.sqlite")
            
            # Create temporary databases with test data
            import sqlite3
            
            # Create temp_db1
            conn1 = sqlite3.connect(temp_db1)
            conn1.execute("CREATE TABLE table1 (id INTEGER, name TEXT)")
            conn1.execute("INSERT INTO table1 VALUES (1, 'test1')")
            conn1.commit()
            conn1.close()
            
            # Create temp_db2
            conn2 = sqlite3.connect(temp_db2)
            conn2.execute("CREATE TABLE table2 (id INTEGER, value TEXT)")
            conn2.execute("INSERT INTO table2 VALUES (2, 'test2')")
            conn2.commit()
            conn2.close()
            
            # Mock results dictionary
            results = {'errors': []}
            
            # Test merging
            processor._merge_databases([temp_db1, temp_db2], output_db, "prefix", results)
            
            # Verify output database
            conn_out = sqlite3.connect(output_db)
            cursor = conn_out.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            assert "temp1_table1" in tables or "file_0_table1" in tables
            assert "temp2_table2" in tables or "file_1_table2" in tables
            
            conn_out.close()
    
    def test_copy_table(self):
        """Test table copying"""
        processor = BatchProcessor()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            source_db = os.path.join(temp_dir, "source.sqlite")
            dest_db = os.path.join(temp_dir, "dest.sqlite")
            
            import sqlite3
            
            # Create source database
            conn_source = sqlite3.connect(source_db)
            conn_source.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")
            conn_source.execute("INSERT INTO test_table VALUES (1, 'test')")
            conn_source.commit()
            conn_source.close()
            
            # Create destination database
            conn_dest = sqlite3.connect(dest_db)
            conn_dest.close()
            
            # Copy table
            conn_source = sqlite3.connect(source_db)
            conn_dest = sqlite3.connect(dest_db)
            
            try:
                processor._copy_table(conn_source, conn_dest, "test_table", "copied_table")
                
                # Verify copy
                cursor = conn_dest.execute("SELECT * FROM copied_table")
                rows = cursor.fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, 'test')
                
            finally:
                conn_source.close()
                conn_dest.close()
    
    def test_generate_batch_report(self):
        """Test batch report generation"""
        processor = BatchProcessor()
        
        results = {
            'success': True,
            'files_processed': 2,
            'tables_created': 10,
            'total_records': 1000,
            'relationships_found': 3,
            'duration': 5.5,
            'errors': [],
            'warnings': [],
            'file_details': {
                'file1.phd': {'tables_created': 5, 'total_records': 500, 'duration': 2.5},
                'file2.mod': {'tables_created': 5, 'total_records': 500, 'duration': 3.0}
            },
            'relationship_map': {
                'table_overlaps': {'TABLE1': ['file1.phd', 'file2.mod']},
                'schema_similarities': {'file1_vs_file2': 0.8}
            }
        }
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            report_file = f.name
        
        try:
            report_content = processor.generate_batch_report(results, report_file)
            
            assert "BATCH PROCESSING REPORT" in report_content
            assert "Files Processed: 2" in report_content
            assert "Tables Created: 10" in report_content
            assert "Total Records: 1000" in report_content
            assert "Relationships Found: 3" in report_content
            
            # Verify file was created
            assert os.path.exists(report_file)
            
        finally:
            if os.path.exists(report_file):
                os.unlink(report_file)
    
    def test_process_batch_sequential(self):
        """Test sequential batch processing"""
        processor = BatchProcessor(max_workers=1)
        
        with patch.object(processor, '_validate_input_files', return_value=["test1.phd", "test2.mod"]):
            with patch.object(processor, '_analyze_relationships', return_value={}):
                with patch.object(processor, '_process_sequential') as mock_sequential:
                    mock_sequential.return_value = {
                        'success': True,
                        'files_processed': 2,
                        'tables_created': 10,
                        'total_records': 1000,
                        'errors': []
                    }
                    
                    results = processor.process_batch(
                        input_files=["test1.phd", "test2.mod"],
                        output_file="output.sqlite",
                        parallel_processing=False
                    )
                    
                    assert results['success'] is True
                    assert results['files_processed'] == 2
                    mock_sequential.assert_called_once()
    
    def test_process_batch_parallel(self):
        """Test parallel batch processing"""
        processor = BatchProcessor(max_workers=2)
        
        with patch.object(processor, '_validate_input_files', return_value=["test1.phd", "test2.mod"]):
            with patch.object(processor, '_analyze_relationships', return_value={}):
                with patch.object(processor, '_process_parallel') as mock_parallel:
                    mock_parallel.return_value = {
                        'success': True,
                        'files_processed': 2,
                        'tables_created': 10,
                        'total_records': 1000,
                        'errors': []
                    }
                    
                    results = processor.process_batch(
                        input_files=["test1.phd", "test2.mod"],
                        output_file="output.sqlite",
                        parallel_processing=True
                    )
                    
                    assert results['success'] is True
                    assert results['files_processed'] == 2
                    mock_parallel.assert_called_once()
    
    def test_process_batch_error_handling(self):
        """Test batch processing error handling"""
        processor = BatchProcessor()
        
        with patch.object(processor, '_validate_input_files', return_value=[]):
            results = processor.process_batch(
                input_files=["nonexistent.phd"],
                output_file="output.sqlite"
            )
            
            assert results['success'] is False
            assert len(results['errors']) > 0
            assert "No valid input files found" in results['errors']
    
    def test_process_batch_with_relationships(self):
        """Test batch processing with relationship analysis"""
        processor = BatchProcessor()
        
        with patch.object(processor, '_validate_input_files', return_value=["test1.phd", "test2.mod"]):
            with patch.object(processor, '_analyze_relationships') as mock_analyze:
                mock_analyze.return_value = {
                    'table_overlaps': {'TABLE1': ['test1.phd', 'test2.mod']},
                    'schema_similarities': {'test1_vs_test2': 0.8}
                }
                
                with patch.object(processor, '_process_sequential') as mock_sequential:
                    mock_sequential.return_value = {
                        'success': True,
                        'files_processed': 2,
                        'tables_created': 10,
                        'total_records': 1000,
                        'errors': []
                    }
                    
                    results = processor.process_batch(
                        input_files=["test1.phd", "test2.mod"],
                        output_file="output.sqlite",
                        relationship_analysis=True
                    )
                    
                    assert results['success'] is True
                    assert results['relationships_found'] == 2
                    assert 'relationship_map' in results
                    mock_analyze.assert_called_once()
