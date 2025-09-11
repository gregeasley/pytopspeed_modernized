#!/usr/bin/env python3
"""
Test runner for resilience feature tests

This script runs all resilience-related tests including unit tests,
integration tests, and performance tests.
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def run_tests(test_type="all", verbose=False, coverage=False):
    """
    Run resilience tests
    
    Args:
        test_type: Type of tests to run ('unit', 'integration', 'performance', 'all')
        verbose: Enable verbose output
        coverage: Enable coverage reporting
    """
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add coverage
    if coverage:
        cmd.extend(["--cov=src/converter", "--cov-report=html", "--cov-report=term"])
    
    # Determine test paths
    test_dir = Path(__file__).parent
    
    if test_type == "unit":
        test_paths = [
            test_dir / "unit" / "test_resilience_enhancer.py",
            test_dir / "unit" / "test_resilience_config.py",
            test_dir / "unit" / "test_sqlite_converter_enhancements.py"
        ]
    elif test_type == "integration":
        test_paths = [
            test_dir / "integration" / "test_resilience_integration.py"
        ]
    elif test_type == "performance":
        test_paths = [
            test_dir / "performance" / "test_resilience_performance.py"
        ]
    elif test_type == "all":
        test_paths = [
            test_dir / "unit",
            test_dir / "integration",
            test_dir / "performance"
        ]
    else:
        print(f"Unknown test type: {test_type}")
        return False
    
    # Add test paths to command
    for path in test_paths:
        if path.exists():
            cmd.append(str(path))
        else:
            print(f"Warning: Test path does not exist: {path}")
    
    # Add test discovery options
    cmd.extend([
        "--tb=short",  # Short traceback format
        "--strict-markers",  # Strict marker handling
        "--disable-warnings",  # Disable warnings for cleaner output
    ])
    
    print(f"Running {test_type} tests...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("-" * 60)
        print(f"✅ {test_type.title()} tests passed!")
        return True
    except subprocess.CalledProcessError as e:
        print("-" * 60)
        print(f"❌ {test_type.title()} tests failed with exit code {e.returncode}")
        return False
    except FileNotFoundError:
        print("❌ pytest not found. Please install pytest: pip install pytest")
        return False


def run_specific_test(test_file, verbose=False):
    """
    Run a specific test file
    
    Args:
        test_file: Path to test file
        verbose: Enable verbose output
    """
    cmd = ["python", "-m", "pytest"]
    
    if verbose:
        cmd.append("-v")
    
    cmd.extend([
        str(test_file),
        "--tb=short",
        "--strict-markers",
        "--disable-warnings"
    ])
    
    print(f"Running specific test: {test_file}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("-" * 60)
        print(f"✅ Test {test_file} passed!")
        return True
    except subprocess.CalledProcessError as e:
        print("-" * 60)
        print(f"❌ Test {test_file} failed with exit code {e.returncode}")
        return False


def list_available_tests():
    """List all available test files"""
    test_dir = Path(__file__).parent
    
    print("Available resilience tests:")
    print("-" * 40)
    
    # Unit tests
    unit_dir = test_dir / "unit"
    if unit_dir.exists():
        print("Unit Tests:")
        for test_file in unit_dir.glob("test_*.py"):
            print(f"  - {test_file.name}")
    
    # Integration tests
    integration_dir = test_dir / "integration"
    if integration_dir.exists():
        print("\nIntegration Tests:")
        for test_file in integration_dir.glob("test_*.py"):
            print(f"  - {test_file.name}")
    
    # Performance tests
    performance_dir = test_dir / "performance"
    if performance_dir.exists():
        print("\nPerformance Tests:")
        for test_file in performance_dir.glob("test_*.py"):
            print(f"  - {test_file.name}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Run resilience feature tests")
    parser.add_argument(
        "test_type",
        nargs="?",
        choices=["unit", "integration", "performance", "all"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "-c", "--coverage",
        action="store_true",
        help="Enable coverage reporting"
    )
    parser.add_argument(
        "-f", "--file",
        help="Run specific test file"
    )
    parser.add_argument(
        "-l", "--list",
        action="store_true",
        help="List available tests"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_available_tests()
        return
    
    if args.file:
        success = run_specific_test(args.file, args.verbose)
    else:
        success = run_tests(args.test_type, args.verbose, args.coverage)
    
    if success:
        print("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
