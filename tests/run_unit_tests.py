#!/usr/bin/env python3
"""
Unit test runner for the phdwin_reader project
"""

import sys
import os
import subprocess
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))


def run_unit_tests():
    """Run all unit tests"""
    
    print("🧪 Running Unit Tests for phdwin_reader...")
    print("=" * 60)
    
    # Change to project root directory
    os.chdir(project_root)
    
    # Run pytest on unit tests
    cmd = [
        sys.executable, "-m", "pytest", 
        "tests/unit/", 
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "--color=yes",  # Colored output
        "-x",  # Stop on first failure
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running tests: {e}")
        return False


def run_specific_test(test_file):
    """Run a specific test file"""
    
    print(f"🧪 Running {test_file}...")
    print("=" * 60)
    
    # Change to project root directory
    os.chdir(project_root)
    
    # Run pytest on specific test file
    cmd = [
        sys.executable, "-m", "pytest", 
        f"tests/unit/{test_file}", 
        "-v",  # Verbose output
        "--tb=short",  # Short traceback format
        "--color=yes",  # Colored output
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running test: {e}")
        return False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run unit tests for phdwin_reader")
    parser.add_argument("--test", help="Run specific test file")
    parser.add_argument("--list", action="store_true", help="List available test files")
    
    args = parser.parse_args()
    
    if args.list:
        print("Available test files:")
        test_dir = Path(__file__).parent / "unit"
        for test_file in test_dir.glob("test_*.py"):
            print(f"  - {test_file.name}")
        return
    
    if args.test:
        success = run_specific_test(args.test)
    else:
        success = run_unit_tests()
    
    if success:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
