#!/usr/bin/env python3
"""
Example script: Convert .phz files to SQLite database

This script demonstrates how to use the PhzConverter to convert
.phz files (zip archives containing .phd and .mod files) to SQLite databases.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.phz_converter import PhzConverter


def progress_callback(current, total, message):
    """Progress callback function for conversion updates"""
    if total > 0:
        percentage = (current / total * 100)
        print(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")
    else:
        print(f"Progress: {current} - {message}")


def convert_phz_file(phz_file: str, output_file: str, batch_size: int = 1000):
    """
    Convert a .phz file to SQLite database
    
    Args:
        phz_file: Path to input .phz file
        output_file: Path to output SQLite file
        batch_size: Number of records to process in each batch
    """
    print("Converting .phz file to SQLite database...")
    print(f"Input file: {phz_file}")
    print(f"Output file: {output_file}")
    print(f"Batch size: {batch_size}")
    print("-" * 60)
    
    # Initialize converter
    converter = PhzConverter(batch_size=batch_size, progress_callback=progress_callback)
    
    # First, list the contents of the .phz file
    print("Listing .phz file contents...")
    contents = converter.list_phz_contents(phz_file)
    
    if not contents['success']:
        print(f"❌ Error listing .phz contents: {contents['errors']}")
        return False
    
    print(f"✅ .phz file contents:")
    print(f"  Total files: {len(contents['phz_contents'])}")
    print(f"  .phd files: {len(contents['phd_files'])} - {contents['phd_files']}")
    print(f"  .mod files: {len(contents['mod_files'])} - {contents['mod_files']}")
    if contents['other_files']:
        print(f"  Other files: {len(contents['other_files'])} - {contents['other_files']}")
    print()
    
    # Convert the .phz file
    print("Starting conversion...")
    results = converter.convert_phz(phz_file, output_file)
    
    # Display results
    print("=" * 60)
    print("PHZ CONVERSION RESULTS")
    print("=" * 60)
    print(f"Success: {results['success']}")
    print(f"Files processed: {results.get('files_processed', 0)}")
    print(f"Tables created: {results['tables_created']}")
    print(f"Total records migrated: {results['total_records']}")
    print(f"Duration: {results['duration']:.2f} seconds")
    
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
    
    if results.get('extracted_files'):
        print(f"Extracted files: {results['extracted_files']}")
    
    if results.get('file_results'):
        print("\nPer-file results:")
        for file_path, file_result in results['file_results'].items():
            status = "✅ SUCCESS" if file_result.get('success', True) else "❌ FAILED"
            print(f"  {os.path.basename(file_path)}: {status}")
            print(f"    Tables: {file_result.get('tables', 0)}")
            print(f"    Records: {file_result.get('records', 0)}")
    
    if results['success']:
        print(f"\n✅ PHZ conversion completed successfully!")
        print(f"SQLite database created: {output_file}")
        
        # Verify the created database
        print("\nVerifying created database...")
        try:
            import sqlite3
            conn = sqlite3.connect(output_file)
            cursor = conn.cursor()
            
            # Get table count
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            
            # Get total record count
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            total_records = 0
            phd_tables = 0
            mod_tables = 0
            
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                record_count = cursor.fetchone()[0]
                total_records += record_count
                
                if table_name.startswith('phd_'):
                    phd_tables += 1
                elif table_name.startswith('mod_'):
                    mod_tables += 1
                
                if record_count > 0:
                    print(f"  {table_name}: {record_count} records")
            
            conn.close()
            
            print(f"\nDatabase verification:")
            print(f"  Tables in database: {table_count}")
            print(f"  Total records: {total_records}")
            print(f"  PHD tables: {phd_tables}")
            print(f"  MOD tables: {mod_tables}")
            
        except Exception as e:
            print(f"❌ Error verifying database: {e}")
        
        return True
    else:
        print(f"\n❌ PHZ conversion failed!")
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Convert .phz files (zip archives containing .phd and .mod files) to SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_phz_to_sqlite.py assets/TxWells.phz output.sqlite
  python convert_phz_to_sqlite.py assets/TxWells.phz output.sqlite --batch-size 500
        """
    )
    
    parser.add_argument('phz_file', help='Path to input .phz file')
    parser.add_argument('output_file', help='Path to output SQLite file')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Number of records to process in each batch (default: 1000)')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.phz_file):
        print(f"❌ Error: Input file not found: {args.phz_file}")
        sys.exit(1)
    
    # Validate file extension
    if not args.phz_file.lower().endswith('.phz'):
        print(f"❌ Error: Input file must have .phz extension: {args.phz_file}")
        sys.exit(1)
    
    # Convert the file
    success = convert_phz_file(args.phz_file, args.output_file, args.batch_size)
    
    if success:
        print(f"\n🎉 Conversion completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Conversion failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
