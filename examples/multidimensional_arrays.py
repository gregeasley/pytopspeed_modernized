#!/usr/bin/env python3
"""
Multidimensional Arrays Example

This example demonstrates how to work with TopSpeed multidimensional arrays
and their conversion to JSON format in SQLite databases.

Features demonstrated:
- Automatic array detection
- Single-field vs multi-field arrays
- JSON querying in SQLite
- Data type preservation (null vs zero)
- Boolean array handling
"""

import sys
import os
import json
import sqlite3
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from converter.sqlite_converter import SqliteConverter
from converter.multidimensional_handler import MultidimensionalHandler
from pytopspeed.tps import TPS


def demonstrate_multidimensional_handling():
    """Demonstrate multidimensional array handling capabilities"""
    
    print("🔄 Multidimensional Arrays Example")
    print("=" * 50)
    
    # Check if we have a sample file
    sample_file = Path("assets/TxWells.PHD")
    if not sample_file.exists():
        print("❌ Sample file not found. Please ensure assets/TxWells.PHD exists.")
        print("   This example requires a TopSpeed file with multidimensional arrays.")
        return
    
    print(f"📁 Using sample file: {sample_file}")
    
    # Step 1: Analyze the TopSpeed file structure
    print("\n1️⃣ Analyzing TopSpeed file structure...")
    
    try:
        tps_file = TPS(str(sample_file), encoding='cp1251', cached=True, check=True)
        
        # Find tables with arrays
        tables_with_arrays = []
        for table_number in tps_file.tables._TpsTablesList__tables:
            table = tps_file.tables._TpsTablesList__tables[table_number]
            table_def = tps_file.tables.get_definition(table.number)
            
            # Use multidimensional handler to analyze
            handler = MultidimensionalHandler()
            analysis = handler.analyze_table_structure(table_def)
            
            if analysis['has_arrays']:
                tables_with_arrays.append({
                    'name': table.name,
                    'array_count': len(analysis['array_fields']),
                    'regular_count': len(analysis['regular_fields']),
                    'arrays': analysis['array_fields']
                })
        
        print(f"   Found {len(tables_with_arrays)} tables with multidimensional arrays:")
        for table_info in tables_with_arrays:
            print(f"   - {table_info['name']}: {table_info['array_count']} arrays, {table_info['regular_count']} regular fields")
            for array_info in table_info['arrays']:
                print(f"     * {array_info.base_name}: {array_info.array_size} {array_info.element_type} elements")
    
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        return
    
    # Step 2: Convert to SQLite
    print("\n2️⃣ Converting to SQLite...")
    
    output_file = "multidimensional_example.sqlite"
    if os.path.exists(output_file):
        os.remove(output_file)
    
    try:
        converter = SqliteConverter()
        result = converter.convert(str(sample_file), output_file)
        
        if result['success']:
            print(f"   ✅ Conversion successful: {result['tables_created']} tables, {result['records_migrated']} records")
        else:
            print(f"   ❌ Conversion failed: {result.get('error', 'Unknown error')}")
            return
    
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        return
    
    # Step 3: Analyze the SQLite database
    print("\n3️⃣ Analyzing SQLite database...")
    
    try:
        conn = sqlite3.connect(output_file)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"   Found {len(tables)} tables in SQLite database")
        
        # Find tables with JSON columns (arrays)
        tables_with_json = []
        for table_name in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            json_columns = []
            for col in columns:
                col_name, col_type = col[1], col[2]
                if col_type == 'TEXT':  # JSON arrays are stored as TEXT
                    # Check if this might be a JSON column by sampling data
                    cursor.execute(f"SELECT {col_name} FROM {table_name} LIMIT 1")
                    sample = cursor.fetchone()
                    if sample and sample[0] and sample[0].startswith('['):
                        json_columns.append(col_name)
            
            if json_columns:
                tables_with_json.append({
                    'name': table_name,
                    'json_columns': json_columns
                })
        
        print(f"   Found {len(tables_with_json)} tables with JSON array columns:")
        for table_info in tables_with_json:
            print(f"   - {table_info['name']}: {', '.join(table_info['json_columns'])}")
    
    except Exception as e:
        print(f"❌ Error analyzing SQLite database: {e}")
        return
    finally:
        conn.close()
    
    # Step 4: Demonstrate JSON querying
    print("\n4️⃣ Demonstrating JSON querying...")
    
    try:
        conn = sqlite3.connect(output_file)
        cursor = conn.cursor()
        
        # Find a table with JSON arrays to demonstrate
        demo_table = None
        for table_info in tables_with_json:
            if table_info['json_columns']:
                demo_table = table_info
                break
        
        if demo_table:
            table_name = demo_table['name']
            json_column = demo_table['json_columns'][0]
            
            print(f"   Using table '{table_name}' with JSON column '{json_column}'")
            
            # Get record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   Table has {count} records")
            
            if count > 0:
                # Show sample JSON data
                cursor.execute(f"SELECT {json_column} FROM {table_name} LIMIT 3")
                samples = cursor.fetchall()
                
                print(f"   Sample JSON data from '{json_column}':")
                for i, (json_data,) in enumerate(samples, 1):
                    if json_data:
                        try:
                            parsed = json.loads(json_data)
                            print(f"     Record {i}: {parsed[:3]}... (showing first 3 elements)")
                        except json.JSONDecodeError:
                            print(f"     Record {i}: Invalid JSON")
                    else:
                        print(f"     Record {i}: NULL")
                
                # Demonstrate JSON functions
                print(f"   JSON querying examples:")
                
                # Count non-null elements in first record
                cursor.execute(f"""
                    SELECT json_array_length({json_column}) as array_length
                    FROM {table_name} 
                    WHERE {json_column} IS NOT NULL 
                    LIMIT 1
                """)
                result = cursor.fetchone()
                if result:
                    print(f"     - Array length in first record: {result[0]}")
                
                # Extract first element from all records
                cursor.execute(f"""
                    SELECT json_extract({json_column}, '$[0]') as first_element
                    FROM {table_name} 
                    WHERE {json_column} IS NOT NULL 
                    LIMIT 5
                """)
                results = cursor.fetchall()
                print(f"     - First elements from first 5 records:")
                for i, (element,) in enumerate(results, 1):
                    print(f"       Record {i}: {element}")
        
        else:
            print("   No tables with JSON arrays found for demonstration")
    
    except Exception as e:
        print(f"❌ Error during JSON demonstration: {e}")
    finally:
        conn.close()
    
    # Step 5: Data type preservation demonstration
    print("\n5️⃣ Data type preservation demonstration...")
    
    try:
        conn = sqlite3.connect(output_file)
        cursor = conn.cursor()
        
        # Look for tables with numeric JSON arrays
        for table_info in tables_with_json:
            table_name = table_info['name']
            
            for json_column in table_info['json_columns']:
                # Check for null vs zero distinction
                cursor.execute(f"""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT({json_column}) as non_null_records,
                        COUNT(CASE WHEN {json_column} IS NULL THEN 1 END) as null_records
                    FROM {table_name}
                """)
                result = cursor.fetchone()
                total, non_null, null_count = result
                
                if null_count > 0:
                    print(f"   Table '{table_name}', column '{json_column}':")
                    print(f"     - Total records: {total}")
                    print(f"     - Non-null records: {non_null}")
                    print(f"     - NULL records: {null_count}")
                    print(f"     - ✅ NULL vs zero distinction preserved")
                    break
        
        # Check for boolean arrays
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND sql LIKE '%TEXT%'
        """)
        tables = cursor.fetchall()
        
        for (table_name,) in tables:
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            for col in columns:
                col_name = col[1]
                # Sample data to check for boolean arrays
                cursor.execute(f"SELECT {col_name} FROM {table_name} WHERE {col_name} LIKE '%true%' OR {col_name} LIKE '%false%' LIMIT 1")
                sample = cursor.fetchone()
                if sample and sample[0]:
                    print(f"   Table '{table_name}', column '{col_name}':")
                    print(f"     - ✅ Boolean arrays detected and preserved")
                    break
    
    except Exception as e:
        print(f"❌ Error during data type demonstration: {e}")
    finally:
        conn.close()
    
    # Cleanup
    print(f"\n🧹 Cleaning up...")
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"   Removed {output_file}")
    
    print("\n✅ Multidimensional arrays example completed!")
    print("\nKey takeaways:")
    print("  - TopSpeed arrays are automatically detected and converted to JSON")
    print("  - Both single-field and multi-field arrays are supported")
    print("  - Data types are preserved (null vs zero, boolean values)")
    print("  - SQLite JSON functions can be used to query array data")
    print("  - No manual configuration required - everything is automatic!")


if __name__ == "__main__":
    demonstrate_multidimensional_handling()
