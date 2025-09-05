#!/usr/bin/env python3
"""
Example script: Convert multiple TopSpeed files (.phd, .mod) to a single SQLite database

This script demonstrates how to use the modernized pytopspeed library
to convert multiple Clarion TopSpeed files to a single SQLite database,
combining tables from both .phd and .mod files.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.sqlite_converter import SqliteConverter


def progress_callback(current, total, message):
    """Progress callback function for conversion updates"""
    if total > 0:
        percentage = (current / total * 100)
        print(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")
    else:
        print(f"Progress: {current} - {message}")


def convert_combined_to_sqlite(input_files, output_file, batch_size=1000):
    """
    Convert multiple TopSpeed files to a single SQLite database
    
    Args:
        input_files: List of paths to input files (.phd, .mod, .tps)
        output_file: Path to output SQLite file
        batch_size: Number of records to process in each batch
    """
    
    print(f"Converting multiple TopSpeed files to SQLite database...")
    print(f"Input files: {', '.join(input_files)}")
    print(f"Output file: {output_file}")
    print(f"Batch size: {batch_size}")
    print("-" * 60)
    
    # Check if input files exist
    missing_files = []
    for input_file in input_files:
        if not os.path.exists(input_file):
            missing_files.append(input_file)
    
    if missing_files:
        print(f"Error: Input files not found: {', '.join(missing_files)}")
        return False
    
    # Initialize converter
    converter = SqliteConverter(batch_size=batch_size, progress_callback=progress_callback)
    
    # Perform conversion
    results = converter.convert_multiple(input_files, output_file)
    
    # Display results
    print("\n" + "="*60)
    print("COMBINED CONVERSION RESULTS")
    print("="*60)
    print(f"Success: {results['success']}")
    print(f"Files processed: {results['files_processed']}")
    print(f"Tables created: {results['tables_created']}")
    print(f"Total records migrated: {results['total_records']}")
    print(f"Duration: {results['duration']:.2f} seconds")
    
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
    
    # Display per-file results
    print(f"\nPer-file results:")
    for file_path, file_result in results['file_results'].items():
        status = "✅ SUCCESS" if file_result['success'] else "❌ FAILED"
        print(f"  {os.path.basename(file_path)}: {status}")
        if file_result['success']:
            print(f"    Tables: {file_result['tables_created']}")
            print(f"    Records: {file_result['records_migrated']}")
        else:
            print(f"    Error: {file_result.get('error', 'Unknown error')}")
    
    if results['success']:
        print(f"\n✅ Combined conversion completed successfully!")
        print(f"SQLite database created: {output_file}")
        
        # Verify the created database
        if os.path.exists(output_file):
            print(f"\nVerifying created database...")
            verify_database(output_file)
    
    return results['success']


def verify_database(db_file):
    """Verify the created SQLite database"""
    import sqlite3
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    try:
        # Get table count
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = cursor.fetchone()[0]
        print(f"  Tables in database: {table_count}")
        
        # Get total record count
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        total_records = 0
        tables_with_data = 0
        phd_tables = 0
        mod_tables = 0
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            total_records += record_count
            if record_count > 0:
                tables_with_data += 1
                print(f"  {table_name}: {record_count} records")
                
                # Count tables by source (based on naming convention)
                if table_name.startswith('file_1_'):
                    phd_tables += 1
                elif table_name.startswith('file_2_'):
                    mod_tables += 1
        
        print(f"  Total records: {total_records}")
        print(f"  Tables with data: {tables_with_data}")
        print(f"  PHD tables: {phd_tables}")
        print(f"  MOD tables: {mod_tables}")
        
        # Show sample data from a table with records
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            record_count = cursor.fetchone()[0]
            if record_count > 0:
                print(f"\n  Sample data from {table_name}:")
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                for i, row in enumerate(rows):
                    # Show first 5 columns to avoid overwhelming output
                    sample_row = row[:5] if len(row) > 5 else row
                    print(f"    Row {i+1}: {sample_row}")
                break
        
    except Exception as e:
        print(f"  Error verifying database: {e}")
    finally:
        conn.close()


def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Convert multiple TopSpeed files (.phd, .mod) to a single SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_combined_to_sqlite.py assets/TxWells.PHD assets/TxWells.mod output.sqlite
  python convert_combined_to_sqlite.py assets/TxWells.PHD assets/TxWells.mod output.sqlite --batch-size 500
        """
    )
    
    parser.add_argument('input_files', nargs='+', help='Paths to input files (.phd, .mod, .tps)')
    parser.add_argument('output_file', help='Path to output SQLite file')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of records to process in each batch (default: 1000)')
    
    args = parser.parse_args()
    
    # Convert the files
    success = convert_combined_to_sqlite(args.input_files, args.output_file, args.batch_size)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
