#!/usr/bin/env python3
"""
Comprehensive unit test runner for all pytopspeed functionality

This script runs all unit tests including the new features:
- Combined database conversion (I1)
- PHZ file support (I2) 
- Reverse conversion (I3)
"""

import sys
import os
import subprocess
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


def run_unit_tests():
    """Run all unit tests"""
    print("=" * 80)
    print("COMPREHENSIVE UNIT TEST SUITE")
    print("=" * 80)
    print("Running all unit tests for pytopspeed modernized library...")
    print()
    
    # Test files to run
    test_files = [
        "tests/unit/test_tps_parser.py",
        "tests/unit/test_schema_mapper.py", 
        "tests/unit/test_sqlite_converter.py",
        "tests/unit/test_combined_conversion.py",  # I1
        "tests/unit/test_phz_converter.py",        # I2
        "tests/unit/test_reverse_converter.py"     # I3
    ]
    
    total_tests = 0
    total_passed = 0
    total_failed = 0
    failed_tests = []
    
    for test_file in test_files:
        if not os.path.exists(test_file):
            print(f"⚠️  Test file not found: {test_file}")
            continue
            
        print(f"Running {test_file}...")
        print("-" * 60)
        
        try:
            # Run pytest on the test file
            result = subprocess.run([
                sys.executable, "-m", "pytest", 
                test_file, 
                "-v", 
                "--tb=short"
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
            
            # Parse output for test counts
            output_lines = result.stdout.split('\n')
            test_count = 0
            passed_count = 0
            failed_count = 0
            
            for line in output_lines:
                if "PASSED" in line:
                    passed_count += 1
                    test_count += 1
                elif "FAILED" in line:
                    failed_count += 1
                    test_count += 1
                elif "ERROR" in line:
                    failed_count += 1
                    test_count += 1
            
            total_tests += test_count
            total_passed += passed_count
            total_failed += failed_count
            
            if result.returncode == 0:
                print(f"✅ {test_file}: {passed_count} tests passed")
            else:
                print(f"❌ {test_file}: {failed_count} tests failed")
                failed_tests.append(test_file)
                print("STDOUT:", result.stdout)
                print("STDERR:", result.stderr)
            
            print()
            
        except Exception as e:
            print(f"❌ Error running {test_file}: {e}")
            failed_tests.append(test_file)
            print()
    
    # Summary
    print("=" * 80)
    print("UNIT TEST SUMMARY")
    print("=" * 80)
    print(f"Total tests: {total_tests}")
    print(f"Passed: {total_passed}")
    print(f"Failed: {total_failed}")
    print(f"Success rate: {(total_passed/total_tests*100):.1f}%" if total_tests > 0 else "No tests run")
    
    if failed_tests:
        print(f"\nFailed test files:")
        for test_file in failed_tests:
            print(f"  - {test_file}")
        print(f"\n❌ Unit tests completed with failures")
        return False
    else:
        print(f"\n✅ All unit tests passed successfully!")
        return True


def run_specific_test_suite(suite_name):
    """Run a specific test suite"""
    test_suites = {
        'parser': 'tests/unit/test_tps_parser.py',
        'schema': 'tests/unit/test_schema_mapper.py',
        'sqlite': 'tests/unit/test_sqlite_converter.py',
        'combined': 'tests/unit/test_combined_conversion.py',  # I1
        'phz': 'tests/unit/test_phz_converter.py',            # I2
        'reverse': 'tests/unit/test_reverse_converter.py'     # I3
    }
    
    if suite_name not in test_suites:
        print(f"❌ Unknown test suite: {suite_name}")
        print(f"Available suites: {', '.join(test_suites.keys())}")
        return False
    
    test_file = test_suites[suite_name]
    print(f"Running {suite_name} test suite: {test_file}")
    print("-" * 60)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            test_file, 
            "-v", 
            "--tb=short"
        ], cwd=Path(__file__).parent.parent)
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Error running {suite_name} tests: {e}")
        return False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run unit tests for pytopspeed modernized library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all_unit_tests.py                    # Run all tests
  python run_all_unit_tests.py --suite combined   # Run combined conversion tests (I1)
  python run_all_unit_tests.py --suite phz        # Run PHZ converter tests (I2)
  python run_all_unit_tests.py --suite reverse    # Run reverse converter tests (I3)
        """
    )
    
    parser.add_argument('--suite', choices=[
        'parser', 'schema', 'sqlite', 'combined', 'phz', 'reverse'
    ], help='Run specific test suite')
    
    args = parser.parse_args()
    
    if args.suite:
        success = run_specific_test_suite(args.suite)
    else:
        success = run_unit_tests()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
