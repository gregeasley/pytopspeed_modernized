#!/usr/bin/env python3
"""
Test script to verify .mod file conversion functionality
"""

import os
import sys
import sqlite3
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed import TPS
from converter.sqlite_converter import SqliteConverter


def test_mod_file_conversion():
    """Test conversion of .mod file to SQLite"""
    
    print("Testing .MOD File Conversion...")
    print("=" * 50)
    
    # Test with .mod file
    mod_file = 'assets/TxWells.mod'
    output_file = 'test_mod_conversion.sqlite'
    
    if not os.path.exists(mod_file):
        print(f"Error: MOD file not found: {mod_file}")
        return False
    
    print(f"Converting: {mod_file} -> {output_file}")
    
    # Initialize converter
    converter = SqliteConverter(batch_size=100)
    
    # Convert the file
    results = converter.convert(mod_file, output_file)
    
    if results['success']:
        print(f"\n✅ Conversion successful!")
        print(f"Tables created: {results['tables_created']}")
        print(f"Total records: {results['total_records']}")
        print(f"Duration: {results['duration']:.2f} seconds")
        
        # Verify the created database
        print(f"\nVerifying created database: {output_file}")
        if os.path.exists(output_file):
            conn = sqlite3.connect(output_file)
            cursor = conn.cursor()
            
            # Get table count
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            print(f"  Tables in database: {table_count}")
            
            # Get record counts for each table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            total_records = 0
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                total_records += record_count
                if record_count > 0:
                    print(f"  {table_name}: {record_count} records")
            
            print(f"  Total records: {total_records}")
            
            # Show sample data from first table with data
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                if record_count > 0:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                    rows = cursor.fetchall()
                    print(f"\n  Sample data from {table_name}:")
                    for i, row in enumerate(rows, 1):
                        print(f"    Row {i}: {row}")
                    break
            
            conn.close()
            
            # Clean up
            os.unlink(output_file)
            print(f"\n✅ .MOD file conversion test completed!")
            return True
        else:
            print(f"❌ Output file not created: {output_file}")
            return False
    else:
        print(f"❌ Conversion failed!")
        for error in results['errors']:
            print(f"  Error: {error}")
        return False


def test_mod_file_structure():
    """Test the structure of .mod file"""
    
    print("\nTesting .MOD File Structure...")
    print("=" * 50)
    
    mod_file = 'assets/TxWells.mod'
    
    if not os.path.exists(mod_file):
        print(f"Error: MOD file not found: {mod_file}")
        return False
    
    try:
        # Load the .mod file
        tps = TPS(mod_file, encoding='cp1251', cached=True, check=True)
        
        print(f"File: {mod_file}")
        print(f"File size: {os.path.getsize(mod_file):,} bytes")
        print(f"Header signature: {tps.header.top_speed_mark}")
        print(f"Total pages: {len(tps.pages.list())}")
        print(f"Total tables: {len(tps.tables._TpsTablesList__tables)}")
        
        # Show some table information
        print(f"\nTable information:")
        named_tables = 0
        for table_number in tps.tables._TpsTablesList__tables:
            table = tps.tables._TpsTablesList__tables[table_number]
            if table.name and table.name != '':
                named_tables += 1
                if named_tables <= 10:  # Show first 10 named tables
                    print(f"  Table {table_number}: {table.name}")
        
        print(f"  ... and {len(tps.tables._TpsTablesList__tables) - named_tables} unnamed tables")
        print(f"  Total named tables: {named_tables}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading .mod file: {e}")
        return False


def main():
    """Main test function"""
    
    print("🧪 Testing .MOD File Conversion")
    print("=" * 60)
    
    # Test 1: File structure
    structure_ok = test_mod_file_structure()
    
    # Test 2: Conversion
    conversion_ok = test_mod_file_conversion()
    
    # Summary
    print(f"\n📊 Test Summary:")
    print(f"  Structure test: {'✅ PASS' if structure_ok else '❌ FAIL'}")
    print(f"  Conversion test: {'✅ PASS' if conversion_ok else '❌ FAIL'}")
    
    if structure_ok and conversion_ok:
        print(f"\n🎉 All .MOD file tests passed!")
        return True
    else:
        print(f"\n❌ Some .MOD file tests failed!")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
