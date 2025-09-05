"""
Unit tests for Error Handler module
"""

import unittest
import tempfile
import os
import json
import shutil
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.converter.error_handler import (
    ErrorHandler, ErrorCategory, ErrorSeverity, ErrorRecord,
    RecoveryStrategy, ConversionError, RecoveryError
)


class TestErrorHandler(unittest.TestCase):
    """Test cases for ErrorHandler class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.error_handler = ErrorHandler()
    
    def tearDown(self):
        """Clean up test fixtures"""
        self.error_handler.cleanup()
    
    def test_initialization(self):
        """Test error handler initialization"""
        self.assertEqual(len(self.error_handler.errors), 0)
        self.assertEqual(len(self.error_handler.recovery_strategies), 4)  # Built-in strategies
        self.assertTrue(self.error_handler.enable_auto_recovery)
        self.assertEqual(self.error_handler.max_errors_before_abort, 100)
    
    def test_log_error_info(self):
        """Test logging info level error"""
        error_record = self.error_handler.log_error(
            ErrorCategory.FILE_ACCESS,
            ErrorSeverity.INFO,
            "Test info message",
            {"test": "data"}
        )
        
        self.assertEqual(len(self.error_handler.errors), 1)
        self.assertEqual(error_record.category, ErrorCategory.FILE_ACCESS)
        self.assertEqual(error_record.severity, ErrorSeverity.INFO)
        self.assertEqual(error_record.message, "Test info message")
        self.assertEqual(error_record.details["test"], "data")
    
    def test_log_error_with_exception(self):
        """Test logging error with exception"""
        try:
            raise ValueError("Test exception")
        except ValueError as e:
            error_record = self.error_handler.log_error(
                ErrorCategory.DATA_PARSING,
                ErrorSeverity.ERROR,
                "Test error with exception",
                {"test": "data"},
                e
            )
        
        self.assertEqual(len(self.error_handler.errors), 1)
        self.assertIsNotNone(error_record.stack_trace)
        self.assertIn("ValueError: Test exception", error_record.stack_trace)
    
    def test_log_error_critical_abort(self):
        """Test that critical errors trigger abort when limit reached"""
        self.error_handler.max_errors_before_abort = 2
        
        # Log first error
        self.error_handler.log_error(
            ErrorCategory.SYSTEM,
            ErrorSeverity.CRITICAL,
            "First critical error"
        )
        
        # Log second error - should trigger abort
        with self.assertRaises(RuntimeError) as context:
            self.error_handler.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.CRITICAL,
                "Second critical error"
            )
        
        self.assertIn("Too many errors occurred", str(context.exception))
    
    def test_add_recovery_strategy(self):
        """Test adding custom recovery strategy"""
        def test_handler(error_record):
            return "Test recovery"
        
        strategy = RecoveryStrategy(
            name="test_strategy",
            description="Test recovery strategy",
            handler=test_handler,
            applicable_errors=[ErrorCategory.FILE_ACCESS]
        )
        
        self.error_handler.add_recovery_strategy(strategy)
        
        self.assertIn("test_strategy", self.error_handler.recovery_strategies)
        self.assertEqual(self.error_handler.recovery_strategies["test_strategy"], strategy)
    
    def test_attempt_recovery_success(self):
        """Test successful recovery attempt"""
        def successful_handler(error_record):
            return "Recovery successful"
        
        strategy = RecoveryStrategy(
            name="successful_strategy",
            description="Always successful recovery",
            handler=successful_handler,
            applicable_errors=[ErrorCategory.VALIDATION]  # Use category without built-in strategies
        )
        
        self.error_handler.add_recovery_strategy(strategy)
        
        error_record = ErrorRecord(
            timestamp=datetime.now(),
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.ERROR,
            message="Test error"
        )
        
        result = self.error_handler._attempt_recovery(error_record)
        
        self.assertIsNotNone(result)
        self.assertIn("Recovery successful", result)
        self.assertEqual(self.error_handler.recovery_attempts["successful_strategy_validation"], 1)
    
    def test_attempt_recovery_failure(self):
        """Test failed recovery attempt"""
        def failing_handler(error_record):
            return None
        
        strategy = RecoveryStrategy(
            name="failing_strategy",
            description="Always failing recovery",
            handler=failing_handler,
            applicable_errors=[ErrorCategory.VALIDATION]  # Use category without built-in strategies
        )
        
        self.error_handler.add_recovery_strategy(strategy)
        
        error_record = ErrorRecord(
            timestamp=datetime.now(),
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.ERROR,
            message="Test error"
        )
        
        result = self.error_handler._attempt_recovery(error_record)
        
        self.assertIsNone(result)
    
    def test_attempt_recovery_max_attempts(self):
        """Test recovery respects max attempts"""
        def handler(error_record):
            return "Recovery attempt"
        
        strategy = RecoveryStrategy(
            name="limited_strategy",
            description="Limited attempts recovery",
            handler=handler,
            applicable_errors=[ErrorCategory.VALIDATION],  # Use category without built-in strategies
            max_attempts=2
        )
        
        self.error_handler.add_recovery_strategy(strategy)
        
        error_record = ErrorRecord(
            timestamp=datetime.now(),
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.ERROR,
            message="Test error"
        )
        
        # First attempt
        result1 = self.error_handler._attempt_recovery(error_record)
        self.assertIsNotNone(result1)
        
        # Second attempt
        result2 = self.error_handler._attempt_recovery(error_record)
        self.assertIsNotNone(result2)
        
        # Third attempt should fail (max attempts reached)
        result3 = self.error_handler._attempt_recovery(error_record)
        self.assertIsNone(result3)
    
    def test_create_backup(self):
        """Test creating backup file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("Test content")
            temp_file = f.name
        
        try:
            backup_path = self.error_handler.create_backup(temp_file)
            
            self.assertIsNotNone(backup_path)
            self.assertTrue(os.path.exists(backup_path))
            self.assertIn(temp_file, self.error_handler.backup_files)
            
            # Verify backup content
            with open(backup_path, 'r') as f:
                content = f.read()
            self.assertEqual(content, "Test content")
            
        finally:
            os.unlink(temp_file)
    
    def test_restore_backup(self):
        """Test restoring from backup"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("Original content")
            temp_file = f.name
        
        try:
            # Create backup
            backup_path = self.error_handler.create_backup(temp_file)
            
            # Modify original file
            with open(temp_file, 'w') as f:
                f.write("Modified content")
            
            # Restore from backup
            success = self.error_handler.restore_backup(temp_file)
            
            self.assertTrue(success)
            
            # Verify content restored
            with open(temp_file, 'r') as f:
                content = f.read()
            self.assertEqual(content, "Original content")
            
        finally:
            os.unlink(temp_file)
    
    def test_create_checkpoint(self):
        """Test creating checkpoint"""
        checkpoint_data = {"test": "data", "number": 42}
        
        checkpoint_path = self.error_handler.create_checkpoint("test_checkpoint", checkpoint_data)
        
        self.assertIsNotNone(checkpoint_path)
        self.assertTrue(os.path.exists(checkpoint_path))
        self.assertIn("test_checkpoint", self.error_handler.checkpoint_files)
        
        # Verify checkpoint content
        with open(checkpoint_path, 'r') as f:
            data = json.load(f)
        
        self.assertEqual(data["data"]["test"], "data")
        self.assertEqual(data["data"]["number"], 42)
        self.assertIn("timestamp", data)
    
    def test_restore_checkpoint(self):
        """Test restoring from checkpoint"""
        checkpoint_data = {"test": "data", "number": 42}
        
        # Create checkpoint
        checkpoint_path = self.error_handler.create_checkpoint("test_checkpoint", checkpoint_data)
        
        # Restore checkpoint
        restored_data = self.error_handler.restore_checkpoint("test_checkpoint")
        
        self.assertIsNotNone(restored_data)
        self.assertEqual(restored_data["test"], "data")
        self.assertEqual(restored_data["number"], 42)
    
    def test_get_error_summary(self):
        """Test getting error summary"""
        # Log some errors
        self.error_handler.log_error(
            ErrorCategory.FILE_ACCESS,
            ErrorSeverity.ERROR,
            "File access error"
        )
        self.error_handler.log_error(
            ErrorCategory.DATA_PARSING,
            ErrorSeverity.WARNING,
            "Data parsing warning"
        )
        self.error_handler.log_error(
            ErrorCategory.FILE_ACCESS,
            ErrorSeverity.INFO,
            "File access info"
        )
        
        summary = self.error_handler.get_error_summary()
        
        self.assertEqual(summary["total_errors"], 3)
        self.assertEqual(summary["errors_by_category"]["file_access"], 2)
        self.assertEqual(summary["errors_by_category"]["data_parsing"], 1)
        self.assertEqual(summary["errors_by_severity"]["error"], 1)
        self.assertEqual(summary["errors_by_severity"]["warning"], 1)
        self.assertEqual(summary["errors_by_severity"]["info"], 1)
        self.assertEqual(len(summary["recent_errors"]), 3)
    
    def test_generate_error_report(self):
        """Test generating error report"""
        # Log some errors
        self.error_handler.log_error(
            ErrorCategory.FILE_ACCESS,
            ErrorSeverity.ERROR,
            "Test error",
            {"test": "data"}
        )
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            report_path = f.name
        
        try:
            result_path = self.error_handler.generate_error_report(report_path)
            
            self.assertEqual(result_path, report_path)
            self.assertTrue(os.path.exists(report_path))
            
            # Verify report content
            with open(report_path, 'r') as f:
                report = json.load(f)
            
            self.assertIn("generated_at", report)
            self.assertIn("summary", report)
            self.assertIn("detailed_errors", report)
            self.assertEqual(len(report["detailed_errors"]), 1)
            self.assertEqual(report["detailed_errors"][0]["message"], "Test error")
            
        finally:
            os.unlink(report_path)
    
    def test_cleanup(self):
        """Test cleanup of temporary files"""
        # Create some temporary files
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            temp_file = f.name
            f.write("Test content")
        
        # Create backup and checkpoint
        backup_path = self.error_handler.create_backup(temp_file)
        checkpoint_path = self.error_handler.create_checkpoint("test", {"data": "test"})
        
        # Verify files exist
        self.assertTrue(os.path.exists(backup_path))
        self.assertTrue(os.path.exists(checkpoint_path))
        
        # Cleanup
        self.error_handler.cleanup()
        
        # Verify files are cleaned up
        self.assertFalse(os.path.exists(backup_path))
        self.assertFalse(os.path.exists(checkpoint_path))
        self.assertEqual(len(self.error_handler.backup_files), 0)
        self.assertEqual(len(self.error_handler.checkpoint_files), 0)
        
        # Clean up test file
        os.unlink(temp_file)
    
    def test_context_manager(self):
        """Test error handler as context manager"""
        with ErrorHandler() as handler:
            handler.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.INFO,
                "Context manager test"
            )
            self.assertEqual(len(handler.errors), 1)
        
        # Handler should be cleaned up after context exit
        # (We can't easily test this without accessing private state)
    
    def test_context_manager_with_exception(self):
        """Test context manager with exception"""
        with self.assertRaises(ValueError):
            with ErrorHandler() as handler:
                handler.log_error(
                    ErrorCategory.SYSTEM,
                    ErrorSeverity.INFO,
                    "Before exception"
                )
                raise ValueError("Test exception")
        
        # Should have logged the exception
        # (Handler is cleaned up, so we can't easily verify this)


class TestConversionError(unittest.TestCase):
    """Test cases for ConversionError class"""
    
    def test_conversion_error_basic(self):
        """Test basic conversion error"""
        error = ConversionError("Test conversion error")
        
        self.assertEqual(str(error), "Test conversion error")
        self.assertEqual(error.category, ErrorCategory.CONVERSION)
        self.assertEqual(error.details, {})
    
    def test_conversion_error_with_details(self):
        """Test conversion error with details"""
        details = {"file": "test.phd", "table": "test_table"}
        error = ConversionError("Test error", ErrorCategory.FILE_ACCESS, details)
        
        self.assertEqual(str(error), "Test error")
        self.assertEqual(error.category, ErrorCategory.FILE_ACCESS)
        self.assertEqual(error.details, details)


class TestRecoveryError(unittest.TestCase):
    """Test cases for RecoveryError class"""
    
    def test_recovery_error_basic(self):
        """Test basic recovery error"""
        error = RecoveryError("Test recovery error")
        
        self.assertEqual(str(error), "Test recovery error")
        self.assertIsNone(error.original_error)
    
    def test_recovery_error_with_original(self):
        """Test recovery error with original error"""
        original = ValueError("Original error")
        error = RecoveryError("Test recovery error", original)
        
        self.assertEqual(str(error), "Test recovery error")
        self.assertEqual(error.original_error, original)


if __name__ == '__main__':
    unittest.main()
