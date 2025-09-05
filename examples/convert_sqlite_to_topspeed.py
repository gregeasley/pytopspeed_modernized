#!/usr/bin/env python3
"""
Example script: Convert SQLite database back to TopSpeed files

This script demonstrates how to use the ReverseConverter to convert
SQLite databases back to TopSpeed .phd and .mod files.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.reverse_converter import ReverseConverter


def progress_callback(current, total, message):
    """Progress callback function for conversion updates"""
    if total > 0:
        percentage = (current / total * 100)
        print(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")
    else:
        print(f"Progress: {current} - {message}")


def convert_sqlite_to_topspeed(sqlite_file: str, output_dir: str):
    """
    Convert SQLite database back to TopSpeed files
    
    Args:
        sqlite_file: Path to input SQLite file
        output_dir: Directory to write output files
    """
    print("Converting SQLite database back to TopSpeed files...")
    print(f"Input file: {sqlite_file}")
    print(f"Output directory: {output_dir}")
    print("-" * 60)
    
    # Initialize converter
    converter = ReverseConverter(progress_callback=progress_callback)
    
    # Convert the database
    print("Starting reverse conversion...")
    results = converter.convert_sqlite_to_topspeed(sqlite_file, output_dir)
    
    # Display results
    print("=" * 60)
    print("REVERSE CONVERSION RESULTS")
    print("=" * 60)
    print(f"Success: {results['success']}")
    print(f"Files created: {len(results['files_created'])}")
    print(f"Tables processed: {results['tables_processed']}")
    print(f"Records processed: {results['records_processed']}")
    print(f"Duration: {results['duration']:.2f} seconds")
    
    if results['errors']:
        print(f"Errors: {len(results['errors'])}")
        for error in results['errors']:
            print(f"  - {error}")
    
    if results['files_created']:
        print(f"\nFiles created:")
        for file_path in results['files_created']:
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            print(f"  - {file_path} ({file_size:,} bytes)")
    
    if results['success']:
        print(f"\n✅ Reverse conversion completed successfully!")
        print(f"TopSpeed files created in: {output_dir}")
        
        # Verify the created files
        print("\nVerifying created files...")
        for file_path in results['files_created']:
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                print(f"  ✅ {os.path.basename(file_path)}: {file_size:,} bytes")
            else:
                print(f"  ❌ {os.path.basename(file_path)}: File not found")
        
        return True
    else:
        print(f"\n❌ Reverse conversion failed!")
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Convert SQLite database back to TopSpeed .phd and .mod files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python convert_sqlite_to_topspeed.py test_conversion.sqlite output/
  python convert_sqlite_to_topspeed.py test_combined_conversion.sqlite output/
        """
    )
    
    parser.add_argument('sqlite_file', help='Path to input SQLite file')
    parser.add_argument('output_dir', help='Directory to write output TopSpeed files')
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.sqlite_file):
        print(f"❌ Error: Input file not found: {args.sqlite_file}")
        sys.exit(1)
    
    # Validate file extension
    if not args.sqlite_file.lower().endswith('.sqlite') and not args.sqlite_file.lower().endswith('.db'):
        print(f"❌ Error: Input file must be a SQLite database: {args.sqlite_file}")
        sys.exit(1)
    
    # Convert the database
    success = convert_sqlite_to_topspeed(args.sqlite_file, args.output_dir)
    
    if success:
        print(f"\n🎉 Reverse conversion completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Reverse conversion failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
