#!/usr/bin/env python3
"""
Example: Data Validation

Demonstrates comprehensive data validation capabilities including:
- Conversion accuracy verification
- Data integrity checks
- Statistical analysis
- Comparison reports
"""

import sys
import argparse
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.data_validator import DataValidator
from converter.sqlite_converter import SqliteConverter


def main():
    """Main example function."""
    parser = argparse.ArgumentParser(description='Data Validation Example')
    parser.add_argument('--topspeed-file', default='assets/TxWells.PHD',
                       help='Original TopSpeed file')
    parser.add_argument('--sqlite-file', default='validation_test.sqlite',
                       help='Converted SQLite file')
    parser.add_argument('--validation-level', choices=['basic', 'standard', 'comprehensive'],
                       default='comprehensive', help='Level of validation to perform')
    parser.add_argument('--generate-report', action='store_true', default=True,
                       help='Generate detailed validation report')
    parser.add_argument('--convert-first', action='store_true', default=True,
                       help='Convert TopSpeed file to SQLite first')
    
    args = parser.parse_args()
    
    def progress_callback(current, total, message=""):
        """Progress callback function."""
        if total > 0:
            percentage = (current / total) * 100
            print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)
        else:
            print(f"\r{message}", end='', flush=True)
    
    try:
        print("🔍 Data Validation Example")
        print("=" * 50)
        
        # Convert TopSpeed to SQLite if requested
        if args.convert_first:
            print("🔄 Converting TopSpeed file to SQLite...")
            converter = SqliteConverter(progress_callback=progress_callback)
            conversion_results = converter.convert(args.topspeed_file, args.sqlite_file)
            
            if not conversion_results['success']:
                print(f"❌ Conversion failed: {conversion_results['errors']}")
                return 1
            
            print()  # New line after progress
            print(f"✅ Conversion completed: {conversion_results['total_records']} records")
            print()
        
        # Initialize data validator
        validator = DataValidator(progress_callback=progress_callback)
        
        print(f"🔍 Starting {args.validation_level} validation...")
        print(f"📁 TopSpeed file: {args.topspeed_file}")
        print(f"🗃️  SQLite file: {args.sqlite_file}")
        print()
        
        # Perform validation
        validation_results = validator.validate_conversion(
            topspeed_file=args.topspeed_file,
            sqlite_file=args.sqlite_file,
            validation_level=args.validation_level,
            generate_report=args.generate_report
        )
        
        print()  # New line after progress
        print()
        
        # Display validation results
        print("📋 VALIDATION RESULTS")
        print("=" * 50)
        print(f"✅ Success: {validation_results['success']}")
        print(f"🗃️  Total Tables: {validation_results['total_tables']}")
        print(f"📊 Total Records: {validation_results['total_records']}")
        print(f"❌ Validation Errors: {len(validation_results['validation_errors'])}")
        print(f"⚠️  Data Inconsistencies: {len(validation_results['data_inconsistencies'])}")
        print(f"⏱️  Duration: {validation_results['duration']:.2f} seconds")
        print()
        
        # Display structure validation
        if 'structure_validation' in validation_results:
            struct_val = validation_results['structure_validation']
            print("🏗️  STRUCTURE VALIDATION")
            print("-" * 30)
            print(f"📋 Tables Match: {'✅' if struct_val['tables_match'] else '❌'}")
            print(f"📊 Record Counts Match: {'✅' if struct_val['record_counts_match'] else '❌'}")
            
            if struct_val['missing_tables']:
                print(f"❌ Missing Tables: {struct_val['missing_tables']}")
            
            if struct_val['extra_tables']:
                print(f"➕ Extra Tables: {struct_val['extra_tables']}")
            
            if struct_val['record_count_differences']:
                print("📊 Record Count Differences:")
                for table, diff in struct_val['record_count_differences'].items():
                    print(f"   📋 {table}: TopSpeed={diff['topspeed']}, SQLite={diff['sqlite']}")
            print()
        
        # Display validation errors
        if validation_results['validation_errors']:
            print("❌ VALIDATION ERRORS")
            print("-" * 30)
            for error in validation_results['validation_errors']:
                print(f"   {error}")
            print()
        
        # Display data inconsistencies
        if validation_results['data_inconsistencies']:
            print("⚠️  DATA INCONSISTENCIES")
            print("-" * 30)
            # Show first 5 inconsistencies
            for inconsistency in validation_results['data_inconsistencies'][:5]:
                print(f"📋 Table: {inconsistency['table']}")
                print(f"   Field: {inconsistency['field']}")
                print(f"   TopSpeed: {inconsistency['topspeed_value']}")
                print(f"   SQLite: {inconsistency['sqlite_value']}")
                print()
            
            if len(validation_results['data_inconsistencies']) > 5:
                print(f"   ... and {len(validation_results['data_inconsistencies']) - 5} more")
            print()
        
        # Display comprehensive validation results
        if 'comprehensive_validation' in validation_results:
            comp_val = validation_results['comprehensive_validation']
            if comp_val.get('statistical_analysis'):
                print("📊 STATISTICAL ANALYSIS")
                print("-" * 30)
                for table, stats in comp_val['statistical_analysis'].items():
                    print(f"📋 {table}:")
                    print(f"   Records: {stats.get('record_count', 0)}")
                    print(f"   Fields: {len(stats.get('field_statistics', {}))}")
                    print()
        
        # Display report information
        if args.generate_report and 'report_file' in validation_results:
            print("📝 VALIDATION REPORT")
            print("-" * 30)
            print(f"📄 Report file: {validation_results['report_file']}")
            print()
        
        # Display summary
        print("🎯 VALIDATION SUMMARY")
        print("=" * 50)
        if validation_results['success']:
            print("✅ Validation completed successfully!")
            print(f"📊 Validated {validation_results['total_records']} records in {validation_results['total_tables']} tables")
            
            if len(validation_results['validation_errors']) == 0:
                print("🎉 No validation errors found!")
            else:
                print(f"⚠️  Found {len(validation_results['validation_errors'])} validation errors")
            
            if len(validation_results['data_inconsistencies']) == 0:
                print("🎉 No data inconsistencies found!")
            else:
                print(f"⚠️  Found {len(validation_results['data_inconsistencies'])} data inconsistencies")
            
            print(f"⏱️  Validation time: {validation_results['duration']:.2f} seconds")
        else:
            print("❌ Validation failed!")
            print(f"   Errors: {len(validation_results['validation_errors'])}")
            for error in validation_results['validation_errors']:
                print(f"   - {error}")
        
        return 0 if validation_results['success'] else 1
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
