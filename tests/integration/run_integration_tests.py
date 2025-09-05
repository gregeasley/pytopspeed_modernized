#!/usr/bin/env python3
"""
Integration test runner for the phdwin_reader project
"""

import sys
import os
import subprocess
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))


def run_integration_tests():
    """Run all integration tests"""
    
    print("🧪 Running Integration Tests for phdwin_reader...")
    print("=" * 80)
    
    # Change to project root directory
    os.chdir(project_root)
    
    test_scripts = [
        ("End-to-End Conversion", "tests/integration/test_end_to_end_conversion.py"),
        ("Performance Testing", "tests/integration/test_performance.py"),
        ("MOD File Conversion", "tests/integration/test_mod_conversion.py"),
        ("SQLite Converter", "tests/integration/test_sqlite_converter.py"),
        ("Schema Mapper", "tests/integration/test_schema_mapper.py"),
        ("PHD Parser", "tests/integration/test_phd_parser.py")
    ]
    
    results = {}
    
    for test_name, test_script in test_scripts:
        print(f"\n🔧 Running {test_name}...")
        print("-" * 60)
        
        try:
            result = subprocess.run([
                sys.executable, test_script
            ], capture_output=False, text=True, timeout=300)  # 5 minute timeout
            
            results[test_name] = {
                'success': result.returncode == 0,
                'returncode': result.returncode
            }
            
            if result.returncode == 0:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED (exit code: {result.returncode})")
                
        except subprocess.TimeoutExpired:
            print(f"⏰ {test_name} TIMEOUT (5 minutes)")
            results[test_name] = {
                'success': False,
                'returncode': -1,
                'error': 'Timeout'
            }
        except Exception as e:
            print(f"💥 {test_name} ERROR: {e}")
            results[test_name] = {
                'success': False,
                'returncode': -1,
                'error': str(e)
            }
    
    # Summary
    print(f"\n📊 Integration Test Summary")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    for test_name, result in results.items():
        if result['success']:
            print(f"✅ {test_name}: PASSED")
            passed += 1
        else:
            print(f"❌ {test_name}: FAILED")
            if 'error' in result:
                print(f"   Error: {result['error']}")
            failed += 1
    
    print(f"\n📈 Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print(f"\n🎉 All integration tests passed!")
        return True
    else:
        print(f"\n❌ {failed} integration test(s) failed!")
        return False


def run_specific_test(test_name):
    """Run a specific integration test"""
    
    test_scripts = {
        "end-to-end": "tests/integration/test_end_to_end_conversion.py",
        "performance": "tests/integration/test_performance.py",
        "mod": "tests/integration/test_mod_conversion.py",
        "sqlite": "tests/integration/test_sqlite_converter.py",
        "schema": "tests/integration/test_schema_mapper.py",
        "parser": "tests/integration/test_phd_parser.py"
    }
    
    if test_name not in test_scripts:
        print(f"❌ Unknown test: {test_name}")
        print(f"Available tests: {', '.join(test_scripts.keys())}")
        return False
    
    print(f"🧪 Running {test_name} integration test...")
    print("=" * 60)
    
    # Change to project root directory
    os.chdir(project_root)
    
    try:
        result = subprocess.run([
            sys.executable, test_scripts[test_name]
        ], capture_output=False, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"\n✅ {test_name} test passed!")
            return True
        else:
            print(f"\n❌ {test_name} test failed!")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"\n⏰ {test_name} test timed out!")
        return False
    except Exception as e:
        print(f"\n💥 {test_name} test error: {e}")
        return False


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run integration tests for phdwin_reader")
    parser.add_argument("--test", help="Run specific test (end-to-end, performance, mod, sqlite, schema, parser)")
    parser.add_argument("--list", action="store_true", help="List available tests")
    
    args = parser.parse_args()
    
    if args.list:
        print("Available integration tests:")
        print("  end-to-end  - End-to-end conversion testing")
        print("  performance - Performance and memory testing")
        print("  mod         - MOD file conversion testing")
        print("  sqlite      - SQLite converter testing")
        print("  schema      - Schema mapper testing")
        print("  parser      - PHD parser testing")
        return
    
    if args.test:
        success = run_specific_test(args.test)
    else:
        success = run_integration_tests()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
