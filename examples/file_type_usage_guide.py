#!/usr/bin/env python3
"""
File Type Usage Guide for Pytopspeed Modernized

This example demonstrates the correct usage of different converter classes
for different file types. This addresses the common confusion about which
converter to use for which file type.
"""

import os
import sys

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from converter.sqlite_converter import SqliteConverter
from converter.phz_converter import PhzConverter
from converter.reverse_converter import ReverseConverter


def demonstrate_single_file_conversion():
    """
    Demonstrate conversion of single TopSpeed files (.phd, .mod, .tps)
    """
    print("=== Single TopSpeed File Conversion ===")
    print("Use: SqliteConverter.convert()")
    print("For: .phd, .mod, .tps files")
    print()
    
    # Example usage (commented out since we don't have actual files)
    # converter = SqliteConverter()
    # result = converter.convert('input.phd', 'output.sqlite')
    # print(f"Success: {result['success']}, Records: {result['total_records']}")
    
    print("Example code:")
    print("""
from converter.sqlite_converter import SqliteConverter

converter = SqliteConverter()
result = converter.convert('input.phd', 'output.sqlite')
print(f"Success: {result['success']}, Records: {result['total_records']}")
""")


def demonstrate_multiple_file_conversion():
    """
    Demonstrate conversion of multiple TopSpeed files to a combined database
    """
    print("=== Multiple TopSpeed Files to Combined Database ===")
    print("Use: SqliteConverter.convert_multiple()")
    print("For: Multiple .phd, .mod, .tps files")
    print()
    
    print("Example code:")
    print("""
from converter.sqlite_converter import SqliteConverter

converter = SqliteConverter()
result = converter.convert_multiple(
    ['file1.phd', 'file2.mod', 'file3.tps'], 
    'combined.sqlite'
)
print(f"Files processed: {result['files_processed']}")
""")


def demonstrate_phz_conversion():
    """
    Demonstrate conversion of .phz files (zip archives)
    """
    print("=== PHZ File Conversion (Zip Archives) ===")
    print("Use: PhzConverter.convert_phz()")
    print("For: .phz files (zip archives containing TopSpeed files)")
    print()
    
    print("Example code:")
    print("""
from converter.phz_converter import PhzConverter

converter = PhzConverter()
result = converter.convert_phz('input.phz', 'output.sqlite')
print(f"Extracted files: {result['extracted_files']}")
""")


def demonstrate_reverse_conversion():
    """
    Demonstrate reverse conversion from SQLite to TopSpeed files
    """
    print("=== Reverse Conversion (SQLite to TopSpeed) ===")
    print("Use: ReverseConverter.convert_sqlite_to_topspeed()")
    print("For: .sqlite files")
    print()
    
    print("Example code:")
    print("""
from converter.reverse_converter import ReverseConverter

converter = ReverseConverter()
result = converter.convert_sqlite_to_topspeed(
    'input.sqlite', 
    'output_directory/'
)
print(f"Generated files: {result['generated_files']}")
""")


def demonstrate_common_mistakes():
    """
    Demonstrate common mistakes and their solutions
    """
    print("=== Common Mistakes and Solutions ===")
    print()
    
    print("❌ WRONG: Using SqliteConverter.convert() with .phz files")
    print("""
# This will fail with error: 'TPS' object has no attribute 'tables'
converter = SqliteConverter()
result = converter.convert('input.phz', 'output.sqlite')
""")
    
    print("✅ CORRECT: Use PhzConverter.convert_phz() for .phz files")
    print("""
from converter.phz_converter import PhzConverter
converter = PhzConverter()
result = converter.convert_phz('input.phz', 'output.sqlite')
""")
    
    print("❌ WRONG: Incorrect import paths")
    print("""
# This will fail with ModuleNotFoundError
from sqlite_converter import SqliteConverter
""")
    
    print("✅ CORRECT: Use proper import paths")
    print("""
from converter.sqlite_converter import SqliteConverter
from converter.phz_converter import PhzConverter
from converter.reverse_converter import ReverseConverter
""")


def main():
    """
    Main function to demonstrate all usage patterns
    """
    print("Pytopspeed Modernized - File Type Usage Guide")
    print("=" * 50)
    print()
    
    demonstrate_single_file_conversion()
    print()
    
    demonstrate_multiple_file_conversion()
    print()
    
    demonstrate_phz_conversion()
    print()
    
    demonstrate_reverse_conversion()
    print()
    
    demonstrate_common_mistakes()
    print()
    
    print("=== Summary ===")
    print("| File Type | Extension | Converter Class | Method |")
    print("|-----------|-----------|-----------------|--------|")
    print("| Single TopSpeed | .phd, .mod, .tps | SqliteConverter | convert() |")
    print("| Multiple TopSpeed | .phd, .mod, .tps | SqliteConverter | convert_multiple() |")
    print("| PHZ Archive | .phz | PhzConverter | convert_phz() |")
    print("| Reverse Conversion | .sqlite | ReverseConverter | convert_sqlite_to_topspeed() |")


if __name__ == "__main__":
    main()
