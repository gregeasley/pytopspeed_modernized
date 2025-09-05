#!/usr/bin/env python3
"""
Main CLI entry point for pytopspeed modernized library

This script provides a comprehensive command-line interface for converting
TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite and back.
"""

import sys
import os
import argparse
import logging
from pathlib import Path
from typing import List, Optional

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from converter.sqlite_converter import SqliteConverter
from converter.phz_converter import PhzConverter
from converter.reverse_converter import ReverseConverter


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def progress_callback(current: int, total: int, message: str = "") -> None:
    """Progress callback for conversion operations"""
    if total > 0:
        percentage = (current / total) * 100
        print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)
    else:
        print(f"\r{message}", end='', flush=True)


def convert_to_sqlite(args) -> int:
    """Convert TopSpeed files to SQLite"""
    try:
        # Determine if we're dealing with multiple files or a single file
        input_files = args.input_files
        
        # Check if any input files are .phz files
        phz_files = [f for f in input_files if f.lower().endswith('.phz')]
        regular_files = [f for f in input_files if not f.lower().endswith('.phz')]
        
        if phz_files and regular_files:
            print("Error: Cannot mix .phz files with other file types in a single conversion")
            return 1
        
        if phz_files:
            # Handle .phz files
            if len(phz_files) > 1:
                print("Error: Only one .phz file can be converted at a time")
                return 1
            
            phz_file = phz_files[0]
            print(f"Converting PHZ file: {phz_file}")
            
            converter = PhzConverter(
                batch_size=args.batch_size,
                progress_callback=progress_callback if not args.quiet else None
            )
            
            results = converter.convert_phz(phz_file, args.output)
            
        else:
            # Handle regular files (.phd, .mod, .tps)
            if len(regular_files) == 1:
                # Single file conversion
                input_file = regular_files[0]
                print(f"Converting single file: {input_file}")
                
                converter = SqliteConverter(
                    batch_size=args.batch_size,
                    progress_callback=progress_callback if not args.quiet else None
                )
                
                results = converter.convert(input_file, args.output)
                
            else:
                # Multiple file conversion
                print(f"Converting {len(regular_files)} files to combined database")
                
                converter = SqliteConverter(
                    batch_size=args.batch_size,
                    progress_callback=progress_callback if not args.quiet else None
                )
                
                results = converter.convert_multiple(regular_files, args.output)
        
        # Print results
        print()  # New line after progress
        if results['success']:
            print(f"SUCCESS: Conversion completed successfully!")
            print(f"   Tables created: {results['tables_created']}")
            print(f"   Records migrated: {results['total_records']}")
            print(f"   Duration: {results['duration']:.2f} seconds")
            print(f"   Output file: {args.output}")
            
            if 'files_processed' in results:
                print(f"   Files processed: {results['files_processed']}")
            
            if 'extracted_files' in results:
                print(f"   Extracted files: {', '.join(results['extracted_files'])}")
                
        else:
            print(f"ERROR: Conversion failed!")
            for error in results['errors']:
                print(f"   Error: {error}")
            return 1
            
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


def convert_to_topspeed(args) -> int:
    """Convert SQLite database back to TopSpeed files"""
    try:
        print(f"Converting SQLite database: {args.input}")
        
        converter = ReverseConverter(
            progress_callback=progress_callback if not args.quiet else None
        )
        
        results = converter.convert_sqlite_to_topspeed(args.input, args.output_dir)
        
        # Print results
        print()  # New line after progress
        if results['success']:
            print(f"SUCCESS: Reverse conversion completed successfully!")
            print(f"   Tables processed: {results['tables_processed']}")
            print(f"   Records processed: {results['records_processed']}")
            print(f"   Duration: {results['duration']:.2f} seconds")
            print(f"   Files created: {', '.join(results['files_created'])}")
            print(f"   Output directory: {args.output_dir}")
                
        else:
            print(f"ERROR: Reverse conversion failed!")
            for error in results['errors']:
                print(f"   Error: {error}")
            return 1
            
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


def list_phz_contents(args) -> int:
    """List contents of a .phz file"""
    try:
        print(f"Listing contents of PHZ file: {args.phz_file}")
        
        converter = PhzConverter()
        results = converter.list_phz_contents(args.phz_file)
        
        if results['success']:
            print(f"SUCCESS: PHZ file contents:")
            print(f"   Total files: {len(results['phz_contents'])}")
            print(f"   PHD files: {len(results['phd_files'])}")
            print(f"   MOD files: {len(results['mod_files'])}")
            print(f"   Other files: {len(results['other_files'])}")
            
            if results['phd_files']:
                print(f"   PHD files: {', '.join(results['phd_files'])}")
            if results['mod_files']:
                print(f"   MOD files: {', '.join(results['mod_files'])}")
            if results['other_files']:
                print(f"   Other files: {', '.join(results['other_files'])}")
        else:
            print(f"ERROR: Failed to list PHZ contents!")
            for error in results['errors']:
                print(f"   Error: {error}")
            return 1
            
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Convert TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite and back",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert a single .phd file to SQLite
  pytopspeed convert assets/TxWells.PHD output.sqlite
  
  # Convert multiple files to a combined SQLite database
  pytopspeed convert assets/TxWells.PHD assets/TxWells.mod combined.sqlite
  
  # Convert a .phz file to SQLite
  pytopspeed convert assets/TxWells.phz output.sqlite
  
  # Convert SQLite back to TopSpeed files
  pytopspeed reverse input.sqlite output_directory/
  
  # List contents of a .phz file
  pytopspeed list assets/TxWells.phz
        """
    )
    
    parser.add_argument('--version', action='version', version='pytopspeed modernized 1.0.0')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress progress output')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Convert command
    convert_parser = subparsers.add_parser('convert', help='Convert TopSpeed files to SQLite')
    convert_parser.add_argument('input_files', nargs='+', help='Input TopSpeed files (.phd, .mod, .tps, .phz)')
    convert_parser.add_argument('output', help='Output SQLite database file')
    convert_parser.add_argument('--batch-size', type=int, default=1000, 
                              help='Batch size for data migration (default: 1000)')
    
    # Reverse command
    reverse_parser = subparsers.add_parser('reverse', help='Convert SQLite back to TopSpeed files')
    reverse_parser.add_argument('input', help='Input SQLite database file')
    reverse_parser.add_argument('output_dir', help='Output directory for TopSpeed files')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List contents of a .phz file')
    list_parser.add_argument('phz_file', help='Input .phz file to list')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Handle commands
    if args.command == 'convert':
        return convert_to_sqlite(args)
    elif args.command == 'reverse':
        return convert_to_topspeed(args)
    elif args.command == 'list':
        return list_phz_contents(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
