#!/usr/bin/env python3
"""
Robust TopSpeed to SQLite Conversion Example

This example demonstrates the robust error handling and recovery system
for converting TopSpeed files to SQLite databases.
"""

import os
import sys
import logging
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from converter.robust_converter import RobustConverter
from converter.error_handler import ErrorHandler, ErrorCategory, ErrorSeverity


def setup_logging():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('robust_conversion.log')
        ]
    )


def demonstrate_error_handling():
    """Demonstrate error handling capabilities"""
    print("=== Error Handling Demonstration ===\n")
    
    # Create error handler
    error_handler = ErrorHandler()
    
    # Log different types of errors
    print("1. Logging various error types:")
    
    # Info level
    error_handler.log_error(
        ErrorCategory.FILE_ACCESS,
        ErrorSeverity.INFO,
        "Starting file processing",
        {"file": "example.phd"}
    )
    
    # Warning level
    error_handler.log_error(
        ErrorCategory.DATA_PARSING,
        ErrorSeverity.WARNING,
        "Skipping corrupt record",
        {"record_id": 123, "reason": "Invalid data format"}
    )
    
    # Error level
    error_handler.log_error(
        ErrorCategory.DATABASE_OPERATION,
        ErrorSeverity.ERROR,
        "Failed to create index",
        {"table": "test_table", "index": "idx_field1"}
    )
    
    # Critical level
    error_handler.log_error(
        ErrorCategory.SYSTEM,
        ErrorSeverity.CRITICAL,
        "Out of memory during conversion",
        {"memory_used": "2GB", "memory_limit": "1GB"}
    )
    
    print(f"   Total errors logged: {len(error_handler.errors)}")
    
    # Show error summary
    print("\n2. Error Summary:")
    summary = error_handler.get_error_summary()
    print(f"   Total errors: {summary['total_errors']}")
    print(f"   Errors by category: {summary['errors_by_category']}")
    print(f"   Errors by severity: {summary['errors_by_severity']}")
    
    # Generate error report
    print("\n3. Generating error report...")
    report_file = error_handler.generate_error_report("error_demo_report.json")
    if report_file:
        print(f"   Error report saved to: {report_file}")
    
    # Cleanup
    error_handler.cleanup()
    print("\n=== Error Handling Demo Complete ===\n")


def demonstrate_robust_conversion():
    """Demonstrate robust conversion with error handling"""
    print("=== Robust Conversion Demonstration ===\n")
    
    # Create robust converter
    converter = RobustConverter()
    
    # Configure conversion options
    options = {
        "create_backup": True,
        "batch_size": 500,
        "enable_partial_conversion": True
    }
    
    # Example file paths (these would be real files in practice)
    input_files = [
        "assets/TxWells.PHD",
        "assets/TxWells.mod",
        "assets/TxWells.phz"
    ]
    
    print("1. Testing file validation:")
    for input_file in input_files:
        if os.path.exists(input_file):
            print(f"   ✓ {input_file} - exists and accessible")
            is_valid = converter._validate_input_file(input_file)
            print(f"     Validation result: {is_valid}")
        else:
            print(f"   ✗ {input_file} - not found")
            # This will demonstrate error handling
            is_valid = converter._validate_input_file(input_file)
            print(f"     Validation result: {is_valid}")
    
    print(f"\n2. Error handler status:")
    print(f"   Total errors: {len(converter.error_handler.errors)}")
    
    # Show recent errors
    if converter.error_handler.errors:
        print("   Recent errors:")
        for error in converter.error_handler.errors[-3:]:
            print(f"     - [{error.severity.value}] {error.message}")
    
    print("\n3. Conversion statistics:")
    stats = converter.conversion_stats
    print(f"   Tables processed: {stats['tables_processed']}")
    print(f"   Tables failed: {stats['tables_failed']}")
    print(f"   Records processed: {stats['records_processed']}")
    print(f"   Records failed: {stats['records_failed']}")
    
    print("\n=== Robust Conversion Demo Complete ===\n")


def demonstrate_recovery_strategies():
    """Demonstrate recovery strategies"""
    print("=== Recovery Strategies Demonstration ===\n")
    
    error_handler = ErrorHandler()
    
    print("1. Available recovery strategies:")
    for name, strategy in error_handler.recovery_strategies.items():
        print(f"   - {name}: {strategy.description}")
        print(f"     Applicable to: {[e.value for e in strategy.applicable_errors]}")
        print(f"     Max attempts: {strategy.max_attempts}")
    
    print("\n2. Adding custom recovery strategy:")
    
    def custom_recovery_handler(error_record):
        """Custom recovery handler"""
        return f"Custom recovery for {error_record.category.value} error"
    
    from converter.error_handler import RecoveryStrategy
    
    custom_strategy = RecoveryStrategy(
        name="custom_recovery",
        description="Custom recovery strategy for demonstration",
        handler=custom_recovery_handler,
        applicable_errors=[ErrorCategory.CONVERSION],
        max_attempts=2
    )
    
    error_handler.add_recovery_strategy(custom_strategy)
    print(f"   Added custom strategy: {custom_strategy.name}")
    
    print("\n3. Testing recovery with custom strategy:")
    
    # Log an error that should trigger custom recovery
    error_record = error_handler.log_error(
        ErrorCategory.CONVERSION,
        ErrorSeverity.ERROR,
        "Test conversion error for recovery",
        {"test": "data"}
    )
    
    print(f"   Recovery action: {error_record.recovery_action}")
    
    # Cleanup
    error_handler.cleanup()
    print("\n=== Recovery Strategies Demo Complete ===\n")


def demonstrate_backup_and_checkpoint():
    """Demonstrate backup and checkpoint functionality"""
    print("=== Backup and Checkpoint Demonstration ===\n")
    
    error_handler = ErrorHandler()
    
    # Create a test file
    test_file = "test_data.txt"
    with open(test_file, 'w') as f:
        f.write("Original content")
    
    print("1. Creating backup:")
    backup_path = error_handler.create_backup(test_file)
    if backup_path:
        print(f"   Backup created: {backup_path}")
        print(f"   Backup exists: {os.path.exists(backup_path)}")
    
    print("\n2. Modifying original file:")
    with open(test_file, 'w') as f:
        f.write("Modified content")
    print("   File modified")
    
    print("\n3. Restoring from backup:")
    success = error_handler.restore_backup(test_file)
    print(f"   Restore successful: {success}")
    
    # Verify content
    with open(test_file, 'r') as f:
        content = f.read()
    print(f"   Restored content: '{content}'")
    
    print("\n4. Creating checkpoint:")
    checkpoint_data = {
        "conversion_progress": 50,
        "tables_processed": ["table1", "table2"],
        "current_table": "table3"
    }
    
    checkpoint_path = error_handler.create_checkpoint("conversion_progress", checkpoint_data)
    if checkpoint_path:
        print(f"   Checkpoint created: {checkpoint_path}")
    
    print("\n5. Restoring from checkpoint:")
    restored_data = error_handler.restore_checkpoint("conversion_progress")
    if restored_data:
        print(f"   Checkpoint restored: {restored_data}")
    
    # Cleanup
    error_handler.cleanup()
    os.unlink(test_file)
    print("\n=== Backup and Checkpoint Demo Complete ===\n")


def main():
    """Main demonstration function"""
    print("TopSpeed to SQLite Robust Conversion System")
    print("=" * 50)
    
    # Set up logging
    setup_logging()
    
    try:
        # Run demonstrations
        demonstrate_error_handling()
        demonstrate_robust_conversion()
        demonstrate_recovery_strategies()
        demonstrate_backup_and_checkpoint()
        
        print("All demonstrations completed successfully!")
        
    except Exception as e:
        print(f"Error during demonstration: {e}")
        logging.exception("Demonstration failed")
    
    finally:
        # Clean up any remaining files
        cleanup_files = [
            "robust_conversion.log",
            "error_demo_report.json",
            "test_data.txt"
        ]
        
        for file in cleanup_files:
            if os.path.exists(file):
                try:
                    os.unlink(file)
                except Exception:
                    pass


if __name__ == "__main__":
    main()
