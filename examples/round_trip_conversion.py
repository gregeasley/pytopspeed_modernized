#!/usr/bin/env python3
"""
Example script: Complete round-trip conversion demonstration

This script demonstrates the complete round-trip conversion:
1. Convert TopSpeed files (.phd, .mod) to SQLite
2. Convert SQLite back to TopSpeed files
3. Compare the results

Note: The reverse conversion creates simplified TopSpeed files that may not
be fully compatible with the original TopSpeed format, but demonstrate
the conversion capability.
"""

import os
import sys
import argparse
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.sqlite_converter import SqliteConverter
from converter.reverse_converter import ReverseConverter


def progress_callback(current, total, message):
    """Progress callback function for conversion updates"""
    if total > 0:
        percentage = (current / total * 100)
        print(f"Progress: {current}/{total} ({percentage:.1f}%) - {message}")
    else:
        print(f"Progress: {current} - {message}")


def round_trip_conversion(input_files: list, output_dir: str):
    """
    Perform complete round-trip conversion
    
    Args:
        input_files: List of input TopSpeed files
        output_dir: Directory for intermediate and output files
    """
    print("=" * 80)
    print("ROUND-TRIP CONVERSION DEMONSTRATION")
    print("=" * 80)
    print(f"Input files: {input_files}")
    print(f"Output directory: {output_dir}")
    print()
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Convert TopSpeed to SQLite
    print("STEP 1: Converting TopSpeed files to SQLite...")
    print("-" * 50)
    
    sqlite_file = os.path.join(output_dir, "intermediate.sqlite")
    converter = SqliteConverter(batch_size=1000, progress_callback=progress_callback)
    
    if len(input_files) == 1:
        # Single file conversion
        results = converter.convert(input_files[0], sqlite_file)
    else:
        # Multiple file conversion
        results = converter.convert_multiple(input_files, sqlite_file)
    
    if not results['success']:
        print(f"❌ Step 1 failed: {results['errors']}")
        return False
    
    print(f"✅ Step 1 completed successfully!")
    print(f"  SQLite file: {sqlite_file}")
    print(f"  Tables: {results['tables_created']}")
    print(f"  Records: {results['total_records']}")
    print(f"  Duration: {results['duration']:.2f} seconds")
    print()
    
    # Step 2: Convert SQLite back to TopSpeed
    print("STEP 2: Converting SQLite back to TopSpeed files...")
    print("-" * 50)
    
    reverse_output_dir = os.path.join(output_dir, "reverse_output")
    reverse_converter = ReverseConverter(progress_callback=progress_callback)
    
    reverse_results = reverse_converter.convert_sqlite_to_topspeed(sqlite_file, reverse_output_dir)
    
    if not reverse_results['success']:
        print(f"❌ Step 2 failed: {reverse_results['errors']}")
        return False
    
    print(f"✅ Step 2 completed successfully!")
    print(f"  Output directory: {reverse_output_dir}")
    print(f"  Files created: {len(reverse_results['files_created'])}")
    print(f"  Tables processed: {reverse_results['tables_processed']}")
    print(f"  Records processed: {reverse_results['records_processed']}")
    print(f"  Duration: {reverse_results['duration']:.2f} seconds")
    print()
    
    # Step 3: Compare results
    print("STEP 3: Comparing results...")
    print("-" * 50)
    
    print("Original files:")
    for file_path in input_files:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"  {os.path.basename(file_path)}: {size:,} bytes")
        else:
            print(f"  {os.path.basename(file_path)}: Not found")
    
    print("\nGenerated files:")
    for file_path in reverse_results['files_created']:
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"  {os.path.basename(file_path)}: {size:,} bytes")
        else:
            print(f"  {os.path.basename(file_path)}: Not found")
    
    print(f"\nSQLite intermediate file:")
    if os.path.exists(sqlite_file):
        size = os.path.getsize(sqlite_file)
        print(f"  {os.path.basename(sqlite_file)}: {size:,} bytes")
    
    # Summary
    print("\n" + "=" * 80)
    print("ROUND-TRIP CONVERSION SUMMARY")
    print("=" * 80)
    print(f"✅ Forward conversion: {results['tables_created']} tables, {results['total_records']} records")
    print(f"✅ Reverse conversion: {reverse_results['tables_processed']} tables, {reverse_results['records_processed']} records")
    print(f"✅ Files created: {len(reverse_results['files_created'])}")
    print(f"✅ Total duration: {results['duration'] + reverse_results['duration']:.2f} seconds")
    
    if results['total_records'] == reverse_results['records_processed']:
        print("✅ Record count matches - conversion successful!")
    else:
        print("⚠️  Record count mismatch - some data may be lost")
    
    print(f"\n🎉 Round-trip conversion completed successfully!")
    print(f"All files are available in: {output_dir}")
    
    return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Demonstrate complete round-trip conversion between TopSpeed and SQLite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single file round-trip
  python round_trip_conversion.py assets/TxWells.PHD output/
  
  # Multiple files round-trip
  python round_trip_conversion.py assets/TxWells.PHD assets/TxWells.mod output/
  
  # PHZ file round-trip
  python round_trip_conversion.py assets/TxWells.phz output/
        """
    )
    
    parser.add_argument('input_files', nargs='+', help='Input TopSpeed files (.phd, .mod, .phz)')
    parser.add_argument('output_dir', help='Directory for intermediate and output files')
    
    args = parser.parse_args()
    
    # Validate input files
    for file_path in args.input_files:
        if not os.path.exists(file_path):
            print(f"❌ Error: Input file not found: {file_path}")
            sys.exit(1)
    
    # Perform round-trip conversion
    success = round_trip_conversion(args.input_files, args.output_dir)
    
    if success:
        print(f"\n🎉 Round-trip conversion completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Round-trip conversion failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
