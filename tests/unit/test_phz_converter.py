#!/usr/bin/env python3
"""
Unit tests for PHZ converter functionality (Issue I2)

Tests the PhzConverter class for handling .phz files (zip archives
containing .phd and .mod files).
"""

import pytest
import tempfile
import os
import zipfile
from unittest.mock import Mock, patch, MagicMock

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from converter.phz_converter import PhzConverter


class TestPhzConverter:
    """Test cases for PHZ converter"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def temp_phz_file(self, temp_dir):
        """Create a temporary .phz file for testing"""
        phz_path = os.path.join(temp_dir, 'test.phz')
        
        # Create a zip file with .phd and .mod files
        with zipfile.ZipFile(phz_path, 'w') as zip_ref:
            # Add a .phd file
            zip_ref.writestr('TxWells.PHD', b'fake phd content')
            # Add a .mod file
            zip_ref.writestr('TxWells.mod', b'fake mod content')
            # Add some other files
            zip_ref.writestr('readme.txt', b'readme content')
            zip_ref.writestr('config.ini', b'config content')
        
        return phz_path
    
    @pytest.fixture
    def temp_phz_file_no_topspeed(self, temp_dir):
        """Create a .phz file without TopSpeed files"""
        phz_path = os.path.join(temp_dir, 'test_no_topspeed.phz')
        
        with zipfile.ZipFile(phz_path, 'w') as zip_ref:
            zip_ref.writestr('readme.txt', b'readme content')
            zip_ref.writestr('config.ini', b'config content')
        
        return phz_path
    
    @pytest.fixture
    def temp_phz_file_invalid(self, temp_dir):
        """Create an invalid .phz file"""
        phz_path = os.path.join(temp_dir, 'test_invalid.phz')
        
        # Write invalid zip content
        with open(phz_path, 'wb') as f:
            f.write(b'This is not a valid zip file')
        
        return phz_path
    
    @pytest.fixture
    def converter(self):
        """Create a PhzConverter instance for testing"""
        return PhzConverter(batch_size=100, progress_callback=None)
    
    def test_list_phz_contents_success(self, converter, temp_phz_file):
        """Test successful listing of .phz file contents"""
        results = converter.list_phz_contents(temp_phz_file)
        
        assert results['success'] is True
        assert len(results['phz_contents']) == 4
        assert 'TxWells.PHD' in results['phd_files']
        assert 'TxWells.mod' in results['mod_files']
        assert 'readme.txt' in results['other_files']
        assert 'config.ini' in results['other_files']
        assert len(results['errors']) == 0
    
    def test_list_phz_contents_no_topspeed_files(self, converter, temp_phz_file_no_topspeed):
        """Test listing .phz file with no TopSpeed files"""
        results = converter.list_phz_contents(temp_phz_file_no_topspeed)
        
        assert results['success'] is True
        assert len(results['phd_files']) == 0
        assert len(results['mod_files']) == 0
        assert len(results['other_files']) == 2
        assert len(results['errors']) == 0
    
    def test_list_phz_contents_invalid_file(self, converter, temp_phz_file_invalid):
        """Test listing invalid .phz file"""
        results = converter.list_phz_contents(temp_phz_file_invalid)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'Invalid .phz file' in results['errors'][0]
    
    def test_list_phz_contents_missing_file(self, converter, temp_dir):
        """Test listing non-existent .phz file"""
        missing_file = os.path.join(temp_dir, 'missing.phz')
        results = converter.list_phz_contents(missing_file)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'not found' in results['errors'][0]
    
    def test_convert_phz_success(self, converter, temp_phz_file, temp_dir):
        """Test successful .phz conversion"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        # Mock the SQLite converter
        with patch.object(converter.sqlite_converter, 'convert_multiple') as mock_convert:
            mock_convert.return_value = {
                'success': True,
                'tables_created': 4,
                'total_records': 100,
                'files_processed': 2,
                'file_results': {
                    'TxWells.PHD': {'tables': 2, 'records': 50},
                    'TxWells.mod': {'tables': 2, 'records': 50}
                }
            }
            
            results = converter.convert_phz(temp_phz_file, output_file)
        
        assert results['success'] is True
        assert results['tables_created'] == 4
        assert results['total_records'] == 100
        assert len(results['extracted_files']) == 2
        assert 'TxWells.PHD' in results['extracted_files']
        assert 'TxWells.mod' in results['extracted_files']
        assert len(results['errors']) == 0
        
        # Verify that convert_multiple was called with extracted files
        mock_convert.assert_called_once()
        call_args = mock_convert.call_args[0]
        assert len(call_args[0]) == 2  # Two input files
        assert call_args[1] == output_file
    
    def test_convert_phz_no_topspeed_files(self, converter, temp_phz_file_no_topspeed, temp_dir):
        """Test .phz conversion with no TopSpeed files"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        results = converter.convert_phz(temp_phz_file_no_topspeed, output_file)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'No .phd or .mod files found' in results['errors'][0]
    
    def test_convert_phz_invalid_file(self, converter, temp_phz_file_invalid, temp_dir):
        """Test .phz conversion with invalid file"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        results = converter.convert_phz(temp_phz_file_invalid, output_file)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'Invalid .phz file' in results['errors'][0]
    
    def test_convert_phz_missing_file(self, converter, temp_dir):
        """Test .phz conversion with missing file"""
        missing_file = os.path.join(temp_dir, 'missing.phz')
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        results = converter.convert_phz(missing_file, output_file)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
        assert 'not found' in results['errors'][0]
    
    def test_convert_phz_sqlite_converter_error(self, converter, temp_phz_file, temp_dir):
        """Test .phz conversion when SQLite converter fails"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        # Mock the SQLite converter to fail
        with patch.object(converter.sqlite_converter, 'convert_multiple') as mock_convert:
            mock_convert.return_value = {
                'success': False,
                'errors': ['SQLite conversion failed']
            }
            
            results = converter.convert_phz(temp_phz_file, output_file)
        
        assert results['success'] is False
        assert len(results['errors']) > 0
    
    def test_convert_phz_temporary_directory_cleanup(self, converter, temp_phz_file, temp_dir):
        """Test that temporary directories are cleaned up"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        with patch.object(converter.sqlite_converter, 'convert_multiple') as mock_convert:
            mock_convert.return_value = {'success': True, 'tables_created': 0, 'total_records': 0}
            
            with patch('shutil.rmtree') as mock_rmtree:
                results = converter.convert_phz(temp_phz_file, output_file)
                
                # Verify that cleanup was attempted
                mock_rmtree.assert_called_once()
    
    def test_convert_phz_progress_callback(self, converter, temp_phz_file, temp_dir):
        """Test that progress callback is invoked during conversion"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        progress_calls = []
        
        def progress_callback(current, total, message):
            progress_calls.append((current, total, message))
        
        converter.progress_callback = progress_callback
        
        with patch.object(converter.sqlite_converter, 'convert_multiple') as mock_convert:
            mock_convert.return_value = {'success': True, 'tables_created': 0, 'total_records': 0}
            
            results = converter.convert_phz(temp_phz_file, output_file)
        
        assert results['success'] is True
        # Progress callback should be passed through to SQLite converter
        mock_convert.assert_called_once()
    
    def test_convert_phz_file_categorization(self, converter, temp_dir):
        """Test proper categorization of files in .phz archive"""
        phz_path = os.path.join(temp_dir, 'test_categorization.phz')
        
        # Create a zip file with various file types
        with zipfile.ZipFile(phz_path, 'w') as zip_ref:
            zip_ref.writestr('file1.phd', b'phd content')
            zip_ref.writestr('file2.PHD', b'phd content uppercase')
            zip_ref.writestr('file3.mod', b'mod content')
            zip_ref.writestr('file4.MOD', b'mod content uppercase')
            zip_ref.writestr('file5.tps', b'tps content')
            zip_ref.writestr('file6.txt', b'text content')
            zip_ref.writestr('file7.doc', b'doc content')
        
        results = converter.list_phz_contents(phz_path)
        
        assert results['success'] is True
        assert len(results['phd_files']) == 2  # file1.phd, file2.PHD
        assert len(results['mod_files']) == 2  # file3.mod, file4.MOD
        assert len(results['other_files']) == 3  # file5.tps, file6.txt, file7.doc
        assert 'file1.phd' in results['phd_files']
        assert 'file2.PHD' in results['phd_files']
        assert 'file3.mod' in results['mod_files']
        assert 'file4.MOD' in results['mod_files']
    
    def test_convert_phz_duration_tracking(self, converter, temp_phz_file, temp_dir):
        """Test that conversion duration is tracked"""
        output_file = os.path.join(temp_dir, 'output.sqlite')
        
        with patch.object(converter.sqlite_converter, 'convert_multiple') as mock_convert:
            mock_convert.return_value = {'success': True, 'tables_created': 0, 'total_records': 0}
            
            results = converter.convert_phz(temp_phz_file, output_file)
        
        assert results['success'] is True
        assert 'duration' in results
        assert results['duration'] >= 0
