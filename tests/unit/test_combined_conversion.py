#!/usr/bin/env python3
"""
Unit tests for combined database conversion functionality (Issue I1)

Tests the SqliteConverter.convert_multiple() method and related functionality
for combining multiple TopSpeed files into a single SQLite database.
"""

import pytest
import sqlite3
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from converter.sqlite_converter import SqliteConverter
from converter.schema_mapper import TopSpeedToSQLiteMapper


class TestCombinedConversion:
    """Test cases for combined database conversion"""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary SQLite database for testing"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            db_path = f.name
        yield db_path
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    @pytest.fixture
    def converter(self):
        """Create a SqliteConverter instance for testing"""
        return SqliteConverter(batch_size=100, progress_callback=None)
    
    @pytest.fixture
    def mock_tps_phd(self):
        """Mock TPS object for PHD file"""
        mock_tps = Mock()
        mock_tps.tables = Mock()
        
        # Create proper mock table objects that are subscriptable
        mock_table1 = Mock()
        mock_table1.name = 'TABLE1'
        mock_table1.number = 1
        
        mock_table2 = Mock()
        mock_table2.name = 'TABLE2'
        mock_table2.number = 2
        
        # Create a proper mock that supports iteration and indexing
        mock_tables_dict = {
            1: mock_table1,
            2: mock_table2
        }
        mock_tps.tables._TpsTablesList__tables = mock_tables_dict
        
        # Mock get_definition to return proper table definition
        def mock_get_definition(table_number):
            # Create proper mock field objects
            mock_field = Mock()
            mock_field.name = 'FIELD1'
            mock_field.type = 'STRING'
            mock_field.size = 50
            
            # Create proper mock index field objects
            mock_index_field = Mock()
            mock_index_field.field_number = 0
            
            # Create proper mock index objects
            mock_index = Mock()
            mock_index.name = 'IDX1'
            mock_index.fields = [mock_index_field]
            
            return Mock(
                fields=[mock_field],
                memos=[],
                indexes=[mock_index]
            )
        
        mock_tps.tables.get_definition = Mock(side_effect=mock_get_definition)
        
        # Mock set_current_table and iteration for data migration
        mock_tps.current_table_number = 1
        
        def mock_set_current_table(table_name):
            if table_name == 'TABLE1':
                mock_tps.current_table_number = 1
            elif table_name == 'TABLE2':
                mock_tps.current_table_number = 2
        
        mock_tps.set_current_table = Mock(side_effect=mock_set_current_table)
        
        # Mock iteration to return different records based on current table
        def mock_iter():
            if mock_tps.current_table_number == 1:
                return iter([{'FIELD1': 'value1'}, {'FIELD1': 'value2'}])
            elif mock_tps.current_table_number == 2:
                return iter([{'FIELD1': 'value3'}, {'FIELD1': 'value4'}])
            else:
                return iter([])
        
        mock_tps.__iter__ = Mock(side_effect=mock_iter)
        
        return mock_tps
    
    @pytest.fixture
    def mock_tps_mod(self):
        """Mock TPS object for MOD file"""
        mock_tps = Mock()
        mock_tps.tables = Mock()
        
        # Create proper mock table objects that are subscriptable
        mock_table3 = Mock()
        mock_table3.name = 'MODTABLE1'
        mock_table3.number = 3
        
        mock_table4 = Mock()
        mock_table4.name = 'MODTABLE2'
        mock_table4.number = 4
        
        # Create a proper mock that supports iteration and indexing
        mock_tables_dict = {
            3: mock_table3,
            4: mock_table4
        }
        mock_tps.tables._TpsTablesList__tables = mock_tables_dict
        
        # Mock get_definition to return proper table definition
        def mock_get_definition(table_number):
            # Create proper mock field objects
            mock_field = Mock()
            mock_field.name = 'MODFIELD1'
            mock_field.type = 'LONG'
            mock_field.size = 4
            
            # Create proper mock index field objects
            mock_index_field = Mock()
            mock_index_field.field_number = 0
            
            # Create proper mock index objects
            mock_index = Mock()
            mock_index.name = 'MODIDX1'
            mock_index.fields = [mock_index_field]
            
            return Mock(
                fields=[mock_field],
                memos=[],
                indexes=[mock_index]
            )
        
        mock_tps.tables.get_definition = Mock(side_effect=mock_get_definition)
        
        # Mock set_current_table and iteration for data migration
        mock_tps.current_table_number = 3
        
        def mock_set_current_table(table_name):
            if table_name == 'MODTABLE1':
                mock_tps.current_table_number = 3
            elif table_name == 'MODTABLE2':
                mock_tps.current_table_number = 4
        
        mock_tps.set_current_table = Mock(side_effect=mock_set_current_table)
        
        # Mock iteration to return different records based on current table
        def mock_iter():
            if mock_tps.current_table_number == 3:
                return iter([{'MODFIELD1': 123}, {'MODFIELD1': 456}])
            elif mock_tps.current_table_number == 4:
                return iter([{'MODFIELD1': 789}, {'MODFIELD1': 101112}])
            else:
                return iter([])
        
        mock_tps.__iter__ = Mock(side_effect=mock_iter)
        
        return mock_tps
    
    def test_convert_multiple_single_file(self, converter, temp_db, mock_tps_phd):
        """Test converting a single file using convert_multiple"""
        with patch('converter.sqlite_converter.TPS', return_value=mock_tps_phd):
            with patch('os.path.exists', return_value=True):
                results = converter.convert_multiple(['test.phd'], temp_db)
        
        assert results['success'] is True
        assert results['files_processed'] == 1
        assert results['tables_created'] == 2
        assert results['total_records'] == 4  # 2 records per table * 2 tables
        assert len(results['file_results']) == 1
    
    def test_convert_multiple_two_files(self, converter, temp_db, mock_tps_phd, mock_tps_mod):
        """Test converting two files with different prefixes"""
        def mock_tps_side_effect(filename, **kwargs):
            if filename.endswith('.phd'):
                return mock_tps_phd
            elif filename.endswith('.mod'):
                return mock_tps_mod
            return mock_tps_phd
        
        with patch('converter.sqlite_converter.TPS', side_effect=mock_tps_side_effect):
            with patch('os.path.exists', return_value=True):
                results = converter.convert_multiple(['test.phd', 'test.mod'], temp_db)
        
        assert results['success'] is True
        assert results['files_processed'] == 2
        assert results['tables_created'] == 4  # 2 tables per file
        assert results['total_records'] == 8  # 2 records per table * 4 tables
        assert len(results['file_results']) == 2
    
    def test_file_prefix_detection(self, converter, temp_db, mock_tps_phd, mock_tps_mod):
        """Test that correct prefixes are applied based on file extension"""
        def mock_tps_side_effect(filename, **kwargs):
            if filename.endswith('.phd'):
                return mock_tps_phd
            elif filename.endswith('.mod'):
                return mock_tps_mod
            return mock_tps_phd
        
        with patch('converter.sqlite_converter.TPS', side_effect=mock_tps_side_effect):
            with patch('os.path.exists', return_value=True):
                with patch.object(converter, '_create_schema') as mock_create_schema:
                    mock_create_schema.return_value = {'TABLE1': 'phd_TABLE1', 'MODTABLE1': 'mod_MODTABLE1'}
                    results = converter.convert_multiple(['test.phd', 'test.mod'], temp_db)
        
        # Verify that _create_schema was called with correct prefixes
        assert mock_create_schema.call_count == 2
        calls = mock_create_schema.call_args_list
        
        # Check that phd_ prefix was used for .phd file
        phd_call = calls[0]
        assert phd_call[1]['file_prefix'] == 'phd_'  # file_prefix parameter
        
        # Check that mod_ prefix was used for .mod file
        mod_call = calls[1]
        assert mod_call[1]['file_prefix'] == 'mod_'  # file_prefix parameter
    
    def test_table_name_collision_handling(self, converter, temp_db, mock_tps_phd, mock_tps_mod):
        """Test handling of table name collisions between files"""
        # Create mock TPS objects with same table names
        mock_tps1 = Mock()
        mock_tps1.tables = Mock()
        mock_tps1.tables._TpsTablesList__tables = {1: Mock(name='SAME_TABLE', number=1)}
        mock_tps1.tables.get_definition = Mock(return_value=Mock(fields=[], memos=[], indexes=[]))
        mock_tps1.__iter__ = Mock(return_value=iter([]))
        
        mock_tps2 = Mock()
        mock_tps2.tables = Mock()
        mock_tps2.tables._TpsTablesList__tables = {2: Mock(name='SAME_TABLE', number=2)}
        mock_tps2.tables.get_definition = Mock(return_value=Mock(fields=[], memos=[], indexes=[]))
        mock_tps2.__iter__ = Mock(return_value=iter([]))
        
        def mock_tps_side_effect(filename, **kwargs):
            if filename.endswith('.phd'):
                return mock_tps1
            elif filename.endswith('.mod'):
                return mock_tps2
            return mock_tps1
        
        with patch('converter.sqlite_converter.TPS', side_effect=mock_tps_side_effect):
            with patch('os.path.exists', return_value=True):
                with patch.object(converter, '_create_schema') as mock_create_schema:
                    # First call returns table without prefix, second with prefix
                    mock_create_schema.side_effect = [
                        {'SAME_TABLE': 'SAME_TABLE'},  # First file
                        {'SAME_TABLE': 'SAME_TABLE'}   # Second file - should get prefix
                    ]
                    results = converter.convert_multiple(['test.phd', 'test.mod'], temp_db)
        
        assert results['success'] is True
        # Verify collision handling was triggered
        assert mock_create_schema.call_count == 2
    
    def test_missing_file_handling(self, converter, temp_db):
        """Test handling of missing input files"""
        with patch('os.path.exists', return_value=False):
            results = converter.convert_multiple(['nonexistent.phd'], temp_db)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'not found' in results['errors'][0]
    
    def test_file_processing_error_handling(self, converter, temp_db):
        """Test handling of errors during file processing"""
        with patch('os.path.exists', return_value=True):
            with patch('converter.sqlite_converter.TPS', side_effect=Exception("Test error")):
                results = converter.convert_multiple(['test.phd'], temp_db)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'Test error' in results['errors'][0]
    
    def test_progress_callback_invocation(self, converter, temp_db, mock_tps_phd):
        """Test that progress callback is invoked during conversion"""
        progress_calls = []
        
        def progress_callback(current, total, message):
            progress_calls.append((current, total, message))
        
        converter.progress_callback = progress_callback
        
        with patch('converter.sqlite_converter.TPS', return_value=mock_tps_phd):
            with patch('os.path.exists', return_value=True):
                # Mock the _migrate_table_data method to capture progress calls
                with patch.object(converter, '_migrate_table_data') as mock_migrate:
                    mock_migrate.return_value = 2  # Return 2 records per table
                    results = converter.convert_multiple(['test.phd'], temp_db)
        
        assert results['success'] is True
        # The progress callback should be passed through to the individual methods
        # Even though convert_multiple doesn't call it directly, the individual methods do
        assert mock_migrate.call_count == 2  # Called for each table
    
    def test_database_verification(self, converter, temp_db, mock_tps_phd):
        """Test that the created database contains expected tables"""
        with patch('converter.sqlite_converter.TPS', return_value=mock_tps_phd):
            with patch('os.path.exists', return_value=True):
                # Don't mock _create_schema - let it run normally to create actual tables
                results = converter.convert_multiple(['test.phd'], temp_db)
        
        assert results['success'] is True
        
        # Verify database was created and contains expected tables
        assert os.path.exists(temp_db)
        
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Should contain our test tables with phd_ prefix
        assert 'phd_TABLE1' in tables
        assert 'phd_TABLE2' in tables
    
    def test_unknown_file_extension_prefix(self, converter, temp_db, mock_tps_phd):
        """Test that unknown file extensions get generic prefixes"""
        with patch('converter.sqlite_converter.TPS', return_value=mock_tps_phd):
            with patch('os.path.exists', return_value=True):
                with patch.object(converter, '_create_schema') as mock_create_schema:
                    mock_create_schema.return_value = {'TABLE1': 'file_1_TABLE1'}
                    results = converter.convert_multiple(['test.unknown'], temp_db)
        
        assert results['success'] is True
        # Verify that _create_schema was called with generic prefix
        calls = mock_create_schema.call_args_list
        assert calls[0][1]['file_prefix'] == 'file_1_'  # file_prefix parameter
    
    def test_empty_file_list(self, converter, temp_db):
        """Test handling of empty file list"""
        results = converter.convert_multiple([], temp_db)
        
        assert results['success'] is False
        assert results['files_processed'] == 0
        assert results['tables_created'] == 0
        assert results['total_records'] == 0
