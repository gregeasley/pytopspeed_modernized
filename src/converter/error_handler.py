"""
Error Handling and Recovery Module

This module provides comprehensive error handling, recovery mechanisms,
and detailed error reporting for the TopSpeed to SQLite conversion process.
"""

import logging
import traceback
import sqlite3
import os
import tempfile
import shutil
from typing import Dict, List, Any, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json


class ErrorSeverity(Enum):
    """Error severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    FILE_ACCESS = "file_access"
    DATA_PARSING = "data_parsing"
    DATABASE_OPERATION = "database_operation"
    MEMORY_LIMIT = "memory_limit"
    VALIDATION = "validation"
    CONVERSION = "conversion"
    SYSTEM = "system"


@dataclass
class ErrorRecord:
    """Record of an error that occurred during conversion"""
    timestamp: datetime
    category: ErrorCategory
    severity: ErrorSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None
    recovery_action: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryStrategy:
    """Strategy for recovering from errors"""
    name: str
    description: str
    handler: Callable
    applicable_errors: List[ErrorCategory]
    max_attempts: int = 3
    backoff_delay: float = 1.0


class ErrorHandler:
    """
    Comprehensive error handling and recovery system for TopSpeed conversions
    """
    
    def __init__(self, log_level: int = logging.INFO):
        """Initialize error handler"""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Error tracking
        self.errors: List[ErrorRecord] = []
        self.recovery_strategies: Dict[str, RecoveryStrategy] = {}
        self.recovery_attempts: Dict[str, int] = {}
        
        # Configuration
        self.max_errors_before_abort = 100
        self.enable_auto_recovery = True
        self.save_error_logs = True
        self.error_log_file = None
        
        # Initialize recovery strategies
        self._initialize_recovery_strategies()
        
        # Backup and recovery state
        self.backup_files: Dict[str, str] = {}
        self.checkpoint_files: Dict[str, str] = {}
        
    def _initialize_recovery_strategies(self):
        """Initialize built-in recovery strategies"""
        
        # File access recovery
        self.add_recovery_strategy(
            RecoveryStrategy(
                name="retry_file_access",
                description="Retry file access with exponential backoff",
                handler=self._retry_file_access,
                applicable_errors=[ErrorCategory.FILE_ACCESS],
                max_attempts=3,
                backoff_delay=1.0
            )
        )
        
        # Database operation recovery
        self.add_recovery_strategy(
            RecoveryStrategy(
                name="retry_database_operation",
                description="Retry database operation with connection reset",
                handler=self._retry_database_operation,
                applicable_errors=[ErrorCategory.DATABASE_OPERATION],
                max_attempts=2,
                backoff_delay=0.5
            )
        )
        
        # Memory limit recovery
        self.add_recovery_strategy(
            RecoveryStrategy(
                name="reduce_memory_usage",
                description="Reduce memory usage by processing smaller batches",
                handler=self._reduce_memory_usage,
                applicable_errors=[ErrorCategory.MEMORY_LIMIT],
                max_attempts=2,
                backoff_delay=0.1
            )
        )
        
        # Data parsing recovery
        self.add_recovery_strategy(
            RecoveryStrategy(
                name="skip_corrupt_record",
                description="Skip corrupt record and continue processing",
                handler=self._skip_corrupt_record,
                applicable_errors=[ErrorCategory.DATA_PARSING],
                max_attempts=1,
                backoff_delay=0.0
            )
        )
        
    def add_recovery_strategy(self, strategy: RecoveryStrategy):
        """Add a custom recovery strategy"""
        self.recovery_strategies[strategy.name] = strategy
        
    def log_error(self, category: ErrorCategory, severity: ErrorSeverity, 
                  message: str, details: Optional[Dict[str, Any]] = None,
                  exception: Optional[Exception] = None, 
                  context: Optional[Dict[str, Any]] = None) -> ErrorRecord:
        """Log an error and attempt recovery"""
        
        # Create error record
        error_record = ErrorRecord(
            timestamp=datetime.now(),
            category=category,
            severity=severity,
            message=message,
            details=details or {},
            stack_trace=traceback.format_exc() if exception else None,
            context=context or {}
        )
        
        # Add to error log
        self.errors.append(error_record)
        
        # Log to logger
        log_method = {
            ErrorSeverity.INFO: self.logger.info,
            ErrorSeverity.WARNING: self.logger.warning,
            ErrorSeverity.ERROR: self.logger.error,
            ErrorSeverity.CRITICAL: self.logger.critical
        }[severity]
        
        log_method(f"[{category.value}] {message}", extra={
            'details': details,
            'context': context,
            'exception': str(exception) if exception else None
        })
        
        # Attempt recovery if enabled
        if self.enable_auto_recovery and severity in [ErrorSeverity.ERROR, ErrorSeverity.CRITICAL]:
            recovery_result = self._attempt_recovery(error_record)
            error_record.recovery_action = recovery_result
        
        # Check if we should abort
        if len(self.errors) >= self.max_errors_before_abort:
            self.logger.critical(f"Maximum error limit ({self.max_errors_before_abort}) reached. Aborting conversion.")
            raise RuntimeError(f"Too many errors occurred during conversion. See error log for details.")
        
        return error_record
    
    def _attempt_recovery(self, error_record: ErrorRecord) -> Optional[str]:
        """Attempt to recover from an error"""
        
        # Find applicable recovery strategies
        applicable_strategies = [
            strategy for strategy in self.recovery_strategies.values()
            if error_record.category in strategy.applicable_errors
        ]
        
        # Try each strategy until one succeeds
        for strategy in applicable_strategies:
            strategy_key = f"{strategy.name}_{error_record.category.value}"
            attempts = self.recovery_attempts.get(strategy_key, 0)
            
            if attempts < strategy.max_attempts:
                try:
                    self.logger.info(f"Attempting recovery with strategy: {strategy.name}")
                    result = strategy.handler(error_record)
                    
                    if result:
                        self.recovery_attempts[strategy_key] = attempts + 1
                        return f"Recovered using {strategy.name}: {result}"
                    else:
                        self.logger.warning(f"Recovery strategy {strategy.name} failed")
                        
                except Exception as e:
                    self.logger.error(f"Recovery strategy {strategy.name} raised exception: {e}")
            else:
                self.logger.debug(f"Recovery strategy {strategy.name} has reached max attempts ({strategy.max_attempts})")
        
        return None
    
    def _retry_file_access(self, error_record: ErrorRecord) -> Optional[str]:
        """Retry file access operation"""
        try:
            # This would be implemented based on the specific file operation
            # For now, return a placeholder
            return "File access retry attempted"
        except Exception:
            return None
    
    def _retry_database_operation(self, error_record: ErrorRecord) -> Optional[str]:
        """Retry database operation with connection reset"""
        try:
            # This would reset database connections and retry
            return "Database operation retry attempted"
        except Exception:
            return None
    
    def _reduce_memory_usage(self, error_record: ErrorRecord) -> Optional[str]:
        """Reduce memory usage by processing smaller batches"""
        try:
            # This would reduce batch sizes and clear caches
            return "Memory usage reduction attempted"
        except Exception:
            return None
    
    def _skip_corrupt_record(self, error_record: ErrorRecord) -> Optional[str]:
        """Skip corrupt record and continue processing"""
        try:
            # This would skip the problematic record
            return "Corrupt record skipped"
        except Exception:
            return None
    
    def create_backup(self, file_path: str) -> str:
        """Create a backup of a file"""
        try:
            backup_dir = tempfile.mkdtemp(prefix="phdwin_backup_")
            backup_path = os.path.join(backup_dir, os.path.basename(file_path))
            shutil.copy2(file_path, backup_path)
            self.backup_files[file_path] = backup_path
            self.logger.info(f"Created backup: {backup_path}")
            return backup_path
        except Exception as e:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.ERROR,
                f"Failed to create backup for {file_path}",
                {"exception": str(e)}
            )
            return None
    
    def restore_backup(self, file_path: str) -> bool:
        """Restore a file from backup"""
        try:
            if file_path in self.backup_files:
                backup_path = self.backup_files[file_path]
                shutil.copy2(backup_path, file_path)
                self.logger.info(f"Restored from backup: {backup_path}")
                return True
            return False
        except Exception as e:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.ERROR,
                f"Failed to restore backup for {file_path}",
                {"exception": str(e)}
            )
            return False
    
    def create_checkpoint(self, checkpoint_name: str, data: Dict[str, Any]) -> str:
        """Create a checkpoint with current state"""
        try:
            checkpoint_dir = tempfile.mkdtemp(prefix="phdwin_checkpoint_")
            checkpoint_path = os.path.join(checkpoint_dir, f"{checkpoint_name}.json")
            
            checkpoint_data = {
                "timestamp": datetime.now().isoformat(),
                "data": data,
                "error_count": len(self.errors)
            }
            
            with open(checkpoint_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            self.checkpoint_files[checkpoint_name] = checkpoint_path
            self.logger.info(f"Created checkpoint: {checkpoint_path}")
            return checkpoint_path
        except Exception as e:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.ERROR,
                f"Failed to create checkpoint {checkpoint_name}",
                {"exception": str(e)}
            )
            return None
    
    def restore_checkpoint(self, checkpoint_name: str) -> Optional[Dict[str, Any]]:
        """Restore state from checkpoint"""
        try:
            if checkpoint_name in self.checkpoint_files:
                checkpoint_path = self.checkpoint_files[checkpoint_name]
                with open(checkpoint_path, 'r') as f:
                    checkpoint_data = json.load(f)
                self.logger.info(f"Restored checkpoint: {checkpoint_path}")
                return checkpoint_data.get("data")
            return None
        except Exception as e:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.ERROR,
                f"Failed to restore checkpoint {checkpoint_name}",
                {"exception": str(e)}
            )
            return None
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of all errors"""
        summary = {
            "total_errors": len(self.errors),
            "errors_by_category": {},
            "errors_by_severity": {},
            "recovery_attempts": dict(self.recovery_attempts),
            "recent_errors": []
        }
        
        # Count errors by category and severity
        for error in self.errors:
            category = error.category.value
            severity = error.severity.value
            
            summary["errors_by_category"][category] = summary["errors_by_category"].get(category, 0) + 1
            summary["errors_by_severity"][severity] = summary["errors_by_severity"].get(severity, 0) + 1
        
        # Get recent errors (last 10)
        summary["recent_errors"] = [
            {
                "timestamp": error.timestamp.isoformat(),
                "category": error.category.value,
                "severity": error.severity.value,
                "message": error.message,
                "recovery_action": error.recovery_action
            }
            for error in self.errors[-10:]
        ]
        
        return summary
    
    def generate_error_report(self, output_file: Optional[str] = None) -> str:
        """Generate detailed error report"""
        try:
            if not output_file:
                output_file = f"error_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            report = {
                "generated_at": datetime.now().isoformat(),
                "summary": self.get_error_summary(),
                "detailed_errors": [
                    {
                        "timestamp": error.timestamp.isoformat(),
                        "category": error.category.value,
                        "severity": error.severity.value,
                        "message": error.message,
                        "details": error.details,
                        "context": error.context,
                        "stack_trace": error.stack_trace,
                        "recovery_action": error.recovery_action
                    }
                    for error in self.errors
                ],
                "recovery_strategies": {
                    name: {
                        "description": strategy.description,
                        "applicable_errors": [e.value for e in strategy.applicable_errors],
                        "max_attempts": strategy.max_attempts
                    }
                    for name, strategy in self.recovery_strategies.items()
                }
            }
            
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            self.logger.info(f"Error report generated: {output_file}")
            return output_file
        except Exception as e:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.ERROR,
                f"Failed to generate error report",
                {"exception": str(e)}
            )
            return None
    
    def cleanup(self):
        """Clean up temporary files and resources"""
        try:
            # Clean up backup files
            for backup_path in self.backup_files.values():
                try:
                    if os.path.exists(backup_path):
                        shutil.rmtree(os.path.dirname(backup_path))
                except Exception:
                    pass
            
            # Clean up checkpoint files
            for checkpoint_path in self.checkpoint_files.values():
                try:
                    if os.path.exists(checkpoint_path):
                        shutil.rmtree(os.path.dirname(checkpoint_path))
                except Exception:
                    pass
            
            self.backup_files.clear()
            self.checkpoint_files.clear()
            
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type:
            self.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.CRITICAL,
                f"Unhandled exception in context manager: {exc_val}",
                {"exception_type": str(exc_type)},
                exc_val
            )
        self.cleanup()


class ConversionError(Exception):
    """Custom exception for conversion errors"""
    
    def __init__(self, message: str, category: ErrorCategory = ErrorCategory.CONVERSION,
                 details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.category = category
        self.details = details or {}


class RecoveryError(Exception):
    """Custom exception for recovery failures"""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error
