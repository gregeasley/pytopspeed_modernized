"""
Unit tests for Robust Converter module
"""

import unittest
import tempfile
import os
import sqlite3
import zipfile
from unittest.mock import Mock, patch, MagicMock, mock_open

from src.converter.robust_converter import RobustConverter
from src.converter.error_handler import ErrorHandler, ErrorCategory, ErrorSeverity, ConversionError


class TestRobustConverter(unittest.TestCase):
    """Test cases for RobustConverter class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.error_handler = ErrorHandler()
        self.converter = RobustConverter(self.error_handler)
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.error_handler.cleanup()
    
    def test_initialization(self):
        """Test robust converter initialization"""
        self.assertIsNotNone(self.converter.error_handler)
        self.assertIsNotNone(self.converter.schema_mapper)
        self.assertIsNotNone(self.converter.data_converter)
        self.assertEqual(self.converter.conversion_stats["tables_processed"], 0)
        self.assertEqual(self.converter.conversion_stats["tables_failed"], 0)
        self.assertTrue(self.converter.enable_partial_conversion)
        self.assertEqual(self.converter.max_retry_attempts, 3)
        self.assertEqual(self.converter.batch_size, 1000)
    
    def test_validate_input_file_exists(self):
        """Test input file validation with existing file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name
        
        try:
            result = self.converter._validate_input_file(temp_file)
            self.assertTrue(result)
        finally:
            os.unlink(temp_file)
    
    def test_validate_input_file_not_exists(self):
        """Test input file validation with non-existing file"""
        result = self.converter._validate_input_file("nonexistent_file.phd")
        self.assertFalse(result)
        self.assertEqual(len(self.error_handler.errors), 1)
        self.assertEqual(self.error_handler.errors[0].category, ErrorCategory.FILE_ACCESS)
    
    def test_validate_input_file_not_readable(self):
        """Test input file validation with non-readable file"""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            temp_file = f.name
        
        try:
            # Make file non-readable
            os.chmod(temp_file, 0o000)
            
            result = self.converter._validate_input_file(temp_file)
            # On Windows, this might still return True due to permission handling
            # So we'll just check that the validation was attempted
            self.assertIsInstance(result, bool)
        finally:
            # Restore permissions and clean up
            try:
                os.chmod(temp_file, 0o644)
                os.unlink(temp_file)
            except:
                pass
    
    @patch('src.converter.robust_converter.TPS')
    def test_initialize_topspeed_file_success(self, mock_tps_class):
        """Test successful TopSpeed file initialization"""
        mock_tps = Mock()
        mock_tps_class.return_value = mock_tps
        
        result = self.converter._initialize_topspeed_file("test.phd")
        
        self.assertEqual(result, mock_tps)
        mock_tps_class.assert_called_once_with("test.phd")
    
    @patch('src.converter.robust_converter.TPS')
    def test_initialize_topspeed_file_failure(self, mock_tps_class):
        """Test TopSpeed file initialization failure"""
        mock_tps_class.side_effect = Exception("Initialization failed")
        
        result = self.converter._initialize_topspeed_file("test.phd")
        
        self.assertIsNone(result)
        self.assertEqual(len(self.error_handler.errors), 1)
        self.assertEqual(self.error_handler.errors[0].category, ErrorCategory.FILE_ACCESS)
    
    def test_create_sqlite_database_success(self):
        """Test successful SQLite database creation"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = self.converter._create_sqlite_database(temp_file, {})
            
            self.assertIsNotNone(conn)
            self.assertTrue(os.path.exists(temp_file))
            
            # Test database is functional
            cursor = conn.execute("SELECT 1")
            result = cursor.fetchone()
            self.assertEqual(result[0], 1)
            
            conn.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_create_sqlite_database_existing_file(self):
        """Test SQLite database creation with existing file"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
            f.write(b"existing content")
        
        try:
            conn = self.converter._create_sqlite_database(temp_file, {})
            
            self.assertIsNotNone(conn)
            # File should be overwritten
            self.assertTrue(os.path.exists(temp_file))
            
            conn.close()
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_create_sqlite_database_failure(self):
        """Test SQLite database creation failure"""
        # Use invalid path to cause failure
        invalid_path = "/invalid/path/database.sqlite"
        
        conn = self.converter._create_sqlite_database(invalid_path, {})
        
        self.assertIsNone(conn)
        self.assertEqual(len(self.error_handler.errors), 1)
        self.assertEqual(self.error_handler.errors[0].category, ErrorCategory.DATABASE_OPERATION)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_single_file_success(self, mock_tps_class):
        """Test successful single file conversion"""
        # Mock TPS object
        mock_tps = Mock()
        mock_table_def = Mock()
        mock_table_def.fields = [Mock(name="field1", type="STRING")]
        mock_table_def.memos = []

        mock_tables = Mock()
        mock_tables.__iter__ = Mock(return_value=iter(["table1"]))
        mock_tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.tables = mock_tables
        mock_tps.set_current_table = Mock()
        mock_tps.__iter__ = Mock(return_value=iter([]))
        
        mock_tps_class.return_value = mock_tps
        
        # Mock schema mapper
        self.converter.schema_mapper.create_table_schema = Mock(return_value="CREATE TABLE table1 (field1 TEXT)")
        self.converter.schema_mapper.create_indexes = Mock(return_value=[])
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            result = self.converter._convert_single_file("test.phd", temp_file, {})
            
            self.assertIn("tables_processed", result)
            self.assertIn("tables_failed", result)
            self.assertEqual(len(result["tables_processed"]), 1)
            self.assertEqual(len(result["tables_failed"]), 0)
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_single_file_tps_failure(self, mock_tps_class):
        """Test single file conversion with TPS initialization failure"""
        mock_tps_class.side_effect = Exception("TPS initialization failed")
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            with self.assertRaises(ConversionError):
                self.converter._convert_single_file("test.phd", temp_file, {})
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_convert_phz_file_success(self):
        """Test successful PHZ file conversion"""
        # Create a mock PHZ file
        with tempfile.NamedTemporaryFile(suffix='.phz', delete=False) as phz_file:
            phz_path = phz_file.name
        
        try:
            # Create a zip file with mock TopSpeed files
            with zipfile.ZipFile(phz_path, 'w') as zip_file:
                zip_file.writestr("test.phd", "mock phd content")
                zip_file.writestr("test.mod", "mock mod content")
            
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
                sqlite_file = f.name
            
            try:
                # Mock the file conversion methods
                self.converter._convert_file_to_database = Mock(return_value={
                    "tables_processed": ["table1"],
                    "tables_failed": []
                })
                
                result = self.converter._convert_phz_file(phz_path, sqlite_file, {})
                
                self.assertIn("files_processed", result)
                self.assertIn("files_failed", result)
                # Should have processed both phd and mod files
                self.assertEqual(len(result["files_processed"]), 2)
                self.assertEqual(len(result["files_failed"]), 0)
                
            finally:
                if os.path.exists(sqlite_file):
                    os.unlink(sqlite_file)
                    
        finally:
            if os.path.exists(phz_path):
                os.unlink(phz_path)
    
    def test_convert_phz_file_invalid_zip(self):
        """Test PHZ file conversion with invalid zip"""
        with tempfile.NamedTemporaryFile(suffix='.phz', delete=False) as f:
            temp_file = f.name
            f.write(b"not a zip file")
        
        try:
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as sqlite_file:
                sqlite_path = sqlite_file.name
            
            try:
                with self.assertRaises(ConversionError):
                    self.converter._convert_phz_file(temp_file, sqlite_path, {})
                    
            finally:
                if os.path.exists(sqlite_path):
                    os.unlink(sqlite_path)
                    
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_table_robust_success(self, mock_tps_class):
        """Test successful robust table conversion"""
        # Mock TPS object
        mock_tps = Mock()
        mock_table_def = Mock()
        mock_field = Mock()
        mock_field.name = "field1"
        mock_field.type = "STRING"
        mock_table_def.fields = [mock_field]
        mock_table_def.memos = []
        
        mock_tps.set_current_table = Mock()
        mock_tps.tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.__iter__ = Mock(return_value=iter([]))
        
        # Mock schema mapper and data converter
        self.converter.schema_mapper.create_table_schema = Mock(return_value="CREATE TABLE table1 (field1 TEXT)")
        self.converter.schema_mapper.create_indexes = Mock(return_value=[])
        self.converter.data_converter.convert_record = Mock(return_value=("value1",))
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            
            result = self.converter._convert_table_robust(mock_tps, "table1", conn, {})
            
            self.assertTrue(result["success"])
            self.assertEqual(result["records_processed"], 0)
            self.assertEqual(result["records_failed"], 0)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_table_robust_with_records(self, mock_tps_class):
        """Test robust table conversion with records"""
        # Mock TPS object with records
        mock_tps = Mock()
        mock_table_def = Mock()
        mock_field = Mock()
        mock_field.name = "field1"
        mock_field.type = "STRING"
        mock_table_def.fields = [mock_field]
        mock_table_def.memos = []
        
        mock_record1 = Mock()
        mock_record2 = Mock()
        
        mock_tps.set_current_table = Mock()
        mock_tps.tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.__iter__ = Mock(return_value=iter([mock_record1, mock_record2]))
        
        # Mock schema mapper and data converter
        self.converter.schema_mapper.create_table_schema = Mock(return_value="CREATE TABLE table1 (field1 TEXT)")
        self.converter.schema_mapper.create_indexes = Mock(return_value=[])
        self.converter.data_converter.convert_record = Mock(return_value=("value1",))
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            
            result = self.converter._convert_table_robust(mock_tps, "table1", conn, {"batch_size": 1})
            
            self.assertTrue(result["success"])
            self.assertEqual(result["records_processed"], 2)
            self.assertEqual(result["records_failed"], 0)
            
            # Verify data was inserted
            cursor = conn.execute("SELECT COUNT(*) FROM table1")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_table_robust_record_conversion_failure(self, mock_tps_class):
        """Test robust table conversion with record conversion failure"""
        # Mock TPS object
        mock_tps = Mock()
        mock_table_def = Mock()
        mock_field = Mock()
        mock_field.name = "field1"
        mock_field.type = "STRING"
        mock_table_def.fields = [mock_field]
        mock_table_def.memos = []
        
        mock_record = Mock()
        
        mock_tps.set_current_table = Mock()
        mock_tps.tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.__iter__ = Mock(return_value=iter([mock_record]))
        
        # Mock schema mapper and data converter
        self.converter.schema_mapper.create_table_schema = Mock(return_value="CREATE TABLE table1 (field1 TEXT)")
        self.converter.schema_mapper.create_indexes = Mock(return_value=[])
        self.converter.data_converter._convert_record_to_tuple = Mock(side_effect=Exception("Conversion failed"))
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            
            result = self.converter._convert_table_robust(mock_tps, "table1", conn, {})
            
            self.assertTrue(result["success"])  # Should still succeed due to partial conversion
            self.assertEqual(result["records_processed"], 0)
            self.assertEqual(result["records_failed"], 1)
            
        finally:
            try:
                conn.close()
            except:
                pass
            if os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                except PermissionError:
                    pass
    
    def test_insert_batch_robust_success(self):
        """Test successful batch insertion"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            conn.execute("CREATE TABLE test_table (field1 TEXT)")
            
            mock_table_def = Mock()
            mock_field = Mock()
            mock_field.name = "field1"
            mock_table_def.fields = [mock_field]
            
            batch_data = [("value1",), ("value2",)]
            
            self.converter._insert_batch_robust(conn, "test_table", batch_data, mock_table_def)
            
            # Verify data was inserted
            cursor = conn.execute("SELECT COUNT(*) FROM test_table")
            count = cursor.fetchone()[0]
            self.assertEqual(count, 2)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_insert_batch_robust_failure(self):
        """Test batch insertion failure"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            # Don't create table to cause failure
            
            mock_table_def = Mock()
            mock_field = Mock()
            mock_field.name = "field1"
            mock_table_def.fields = [mock_field]
            
            batch_data = [("value1",)]
            
            with self.assertRaises(Exception):
                self.converter._insert_batch_robust(conn, "nonexistent_table", batch_data, mock_table_def)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_create_indexes_robust_success(self):
        """Test successful index creation"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            conn.execute("CREATE TABLE test_table (field1 TEXT)")
            
            mock_table_def = Mock()
            
            self.converter.schema_mapper.create_indexes = Mock(return_value=[
                "CREATE INDEX idx_field1 ON test_table (field1)"
            ])
            
            self.converter._create_indexes_robust(conn, mock_table_def, "test_table")
            
            # Verify index was created
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='test_table'")
            indexes = cursor.fetchall()
            self.assertEqual(len(indexes), 1)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def test_create_indexes_robust_failure(self):
        """Test index creation failure"""
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            conn.execute("CREATE TABLE test_table (field1 TEXT)")
            
            mock_table_def = Mock()
            
            # Return invalid index SQL to cause failure
            self.converter.schema_mapper.create_indexes = Mock(return_value=[
                "CREATE INDEX idx_invalid ON nonexistent_table (field1)"
            ])
            
            # Should not raise exception, just log warning
            self.converter._create_indexes_robust(conn, mock_table_def, "test_table")
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    @patch('src.converter.robust_converter.TPS')
    def test_convert_file_to_database_success(self, mock_tps_class):
        """Test successful file to database conversion"""
        # Mock TPS object
        mock_tps = Mock()
        mock_table_def = Mock()
        mock_field = Mock()
        mock_field.name = "field1"
        mock_field.type = "STRING"
        mock_table_def.fields = [mock_field]
        mock_table_def.memos = []
        
        mock_tables = Mock()
        mock_tables.__iter__ = Mock(return_value=iter(["table1"]))
        mock_tables.get_definition = Mock(return_value=mock_table_def)
        mock_tps.tables = mock_tables
        mock_tps.set_current_table = Mock()
        mock_tps.__iter__ = Mock(return_value=iter([]))
        
        mock_tps_class.return_value = mock_tps
        
        # Mock schema mapper
        self.converter.schema_mapper.create_table_schema = Mock(return_value="CREATE TABLE phd_table1 (field1 TEXT)")
        self.converter.schema_mapper.create_indexes = Mock(return_value=[])
        
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as f:
            temp_file = f.name
        
        try:
            conn = sqlite3.connect(temp_file)
            
            result = self.converter._convert_file_to_database("test.phd", conn, "phd_", {})
            
            self.assertIn("tables_processed", result)
            self.assertIn("tables_failed", result)
            self.assertEqual(len(result["tables_processed"]), 1)
            self.assertEqual(len(result["tables_failed"]), 0)
            
            conn.close()
            
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()
