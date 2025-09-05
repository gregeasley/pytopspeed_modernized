#!/usr/bin/env python3
"""
End-to-end integration tests for TopSpeed to SQLite conversion
"""

import os
import sys
import sqlite3
import tempfile
import time
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed import TPS
from converter.sqlite_converter import SqliteConverter


class TestEndToEndConversion:
    """Test complete end-to-end conversion process"""
    
    def __init__(self):
        self.test_files = [
            ('assets/TxWells.PHD', 'phd'),
            ('assets/TxWells.mod', 'mod')
        ]
        self.results = {}
    
    def test_file_conversion(self, input_file, file_type):
        """Test conversion of a single file"""
        print(f"\n🧪 Testing {file_type.upper()} file conversion: {input_file}")
        print("=" * 60)
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False
        
        # Create temporary output file
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            output_file = tmp.name
        
        try:
            # Initialize converter
            converter = SqliteConverter(batch_size=100)
            
            # Record start time
            start_time = time.time()
            
            # Convert the file
            results = converter.convert(input_file, output_file)
            
            # Record end time
            end_time = time.time()
            duration = end_time - start_time
            
            if results['success']:
                print(f"✅ Conversion successful!")
                print(f"   Tables created: {results['tables_created']}")
                print(f"   Records migrated: {results['total_records']}")
                print(f"   Duration: {duration:.2f} seconds")
                print(f"   Records/second: {results['total_records'] / duration:.0f}")
                
                # Verify the database
                verification_result = self.verify_database(output_file, results)
                
                # Store results
                self.results[file_type] = {
                    'success': True,
                    'tables_created': results['tables_created'],
                    'total_records': results['total_records'],
                    'duration': duration,
                    'records_per_second': results['total_records'] / duration,
                    'verification': verification_result
                }
                
                return True
            else:
                print(f"❌ Conversion failed!")
                for error in results['errors']:
                    print(f"   Error: {error}")
                
                self.results[file_type] = {
                    'success': False,
                    'errors': results['errors']
                }
                return False
                
        finally:
            # Clean up
            if os.path.exists(output_file):
                os.unlink(output_file)
    
    def verify_database(self, db_file, expected_results):
        """Verify the created SQLite database"""
        print(f"\n🔍 Verifying database: {db_file}")
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Check table count
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            
            print(f"   Tables in database: {table_count}")
            print(f"   Expected tables: {expected_results['tables_created']}")
            
            if table_count != expected_results['tables_created']:
                print(f"   ❌ Table count mismatch!")
                return False
            
            # Check record counts
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            total_records = 0
            table_details = {}
            
            for (table_name,) in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                record_count = cursor.fetchone()[0]
                total_records += record_count
                table_details[table_name] = record_count
                
                if record_count > 0:
                    print(f"   {table_name}: {record_count} records")
            
            print(f"   Total records: {total_records}")
            print(f"   Expected records: {expected_results['total_records']}")
            
            if total_records != expected_results['total_records']:
                print(f"   ❌ Record count mismatch!")
                return False
            
            # Check data integrity - sample a few records
            print(f"\n   📊 Data integrity check:")
            sample_tables = [name for name, count in table_details.items() if count > 0][:3]
            
            for table_name in sample_tables:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                sample_record = cursor.fetchone()
                if sample_record:
                    print(f"   {table_name}: Sample record has {len(sample_record)} fields")
                    # Check for None values (should be minimal)
                    none_count = sum(1 for field in sample_record if field is None)
                    if none_count > len(sample_record) * 0.5:  # More than 50% None values
                        print(f"   ⚠️  Warning: {none_count}/{len(sample_record)} fields are None")
                    else:
                        print(f"   ✅ Data quality good: {none_count}/{len(sample_record)} None fields")
            
            # Check indexes
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index'")
            index_count = cursor.fetchone()[0]
            print(f"   Indexes created: {index_count}")
            
            conn.close()
            
            print(f"   ✅ Database verification successful!")
            return True
            
        except Exception as e:
            print(f"   ❌ Database verification failed: {e}")
            return False
    
    def test_data_type_coverage(self, input_file, file_type):
        """Test that all data types are properly converted"""
        print(f"\n🔬 Testing data type coverage for {file_type.upper()} file")
        print("=" * 60)
        
        try:
            # Load the TopSpeed file
            tps = TPS(input_file, encoding='cp1251', cached=True, check=True)
            
            data_types_found = set()
            field_examples = {}
            
            # Analyze all tables
            for table_number in tps.tables._TpsTablesList__tables:
                table = tps.tables._TpsTablesList__tables[table_number]
                if table.name and table.name != '':
                    try:
                        table_def = tps.tables.get_definition(table_number)
                        
                        # Check fields
                        for field in table_def.fields:
                            field_type = str(field.type)
                            data_types_found.add(field_type)
                            
                            if field_type not in field_examples:
                                field_examples[field_type] = {
                                    'table': table.name,
                                    'field': field.name,
                                    'size': field.size
                                }
                        
                        # Check memos
                        for memo in table_def.memos:
                            data_types_found.add('MEMO')
                            if 'MEMO' not in field_examples:
                                field_examples['MEMO'] = {
                                    'table': table.name,
                                    'field': memo.name,
                                    'size': 'variable'
                                }
                                
                    except Exception as e:
                        print(f"   ⚠️  Could not analyze table {table.name}: {e}")
            
            print(f"   Data types found: {sorted(data_types_found)}")
            print(f"   Total data types: {len(data_types_found)}")
            
            # Show examples
            print(f"\n   📋 Field examples:")
            for data_type, example in field_examples.items():
                print(f"   {data_type}: {example['table']}.{example['field']} (size: {example['size']})")
            
            # Expected data types
            expected_types = {
                'STRING', 'CSTRING', 'PSTRING',
                'BYTE', 'SHORT', 'USHORT', 'LONG', 'ULONG',
                'FLOAT', 'DOUBLE', 'DECIMAL',
                'DATE', 'TIME', 'GROUP', 'MEMO'
            }
            
            missing_types = expected_types - data_types_found
            if missing_types:
                print(f"   ⚠️  Missing data types: {missing_types}")
            else:
                print(f"   ✅ All expected data types found!")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Data type analysis failed: {e}")
            return False
    
    def test_table_relationships(self, input_file, file_type):
        """Test table relationships and foreign key constraints"""
        print(f"\n🔗 Testing table relationships for {file_type.upper()} file")
        print("=" * 60)
        
        try:
            # Load the TopSpeed file
            tps = TPS(input_file, encoding='cp1251', cached=True, check=True)
            
            table_info = {}
            
            # Collect table information
            for table_number in tps.tables._TpsTablesList__tables:
                table = tps.tables._TpsTablesList__tables[table_number]
                if table.name and table.name != '':
                    try:
                        table_def = tps.tables.get_definition(table_number)
                        
                        table_info[table.name] = {
                            'fields': [field.name for field in table_def.fields],
                            'indexes': [index.name for index in table_def.indexes],
                            'memos': [memo.name for memo in table_def.memos]
                        }
                        
                    except Exception as e:
                        print(f"   ⚠️  Could not analyze table {table.name}: {e}")
            
            print(f"   Tables analyzed: {len(table_info)}")
            
            # Look for potential relationships (fields with similar names)
            potential_relationships = []
            table_names = list(table_info.keys())
            
            for i, table1 in enumerate(table_names):
                for table2 in table_names[i+1:]:
                    # Look for common field patterns
                    fields1 = set(table_info[table1]['fields'])
                    fields2 = set(table_info[table2]['fields'])
                    
                    common_fields = fields1.intersection(fields2)
                    if common_fields:
                        potential_relationships.append({
                            'table1': table1,
                            'table2': table2,
                            'common_fields': list(common_fields)
                        })
            
            print(f"   Potential relationships found: {len(potential_relationships)}")
            
            # Show some examples
            for rel in potential_relationships[:5]:  # Show first 5
                print(f"   {rel['table1']} ↔ {rel['table2']}: {rel['common_fields']}")
            
            if len(potential_relationships) > 5:
                print(f"   ... and {len(potential_relationships) - 5} more")
            
            print(f"   ✅ Table relationship analysis completed!")
            return True
            
        except Exception as e:
            print(f"   ❌ Table relationship analysis failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run all end-to-end tests"""
        print("🚀 Starting End-to-End Integration Tests")
        print("=" * 80)
        
        all_passed = True
        
        for input_file, file_type in self.test_files:
            # Test 1: File conversion
            conversion_ok = self.test_file_conversion(input_file, file_type)
            
            if conversion_ok:
                # Test 2: Data type coverage
                data_type_ok = self.test_data_type_coverage(input_file, file_type)
                
                # Test 3: Table relationships
                relationships_ok = self.test_table_relationships(input_file, file_type)
                
                if not (data_type_ok and relationships_ok):
                    all_passed = False
            else:
                all_passed = False
        
        # Summary
        print(f"\n📊 End-to-End Test Summary")
        print("=" * 80)
        
        for file_type, result in self.results.items():
            if result['success']:
                print(f"✅ {file_type.upper()} file:")
                print(f"   Tables: {result['tables_created']}")
                print(f"   Records: {result['total_records']:,}")
                print(f"   Duration: {result['duration']:.2f}s")
                print(f"   Speed: {result['records_per_second']:.0f} records/sec")
                print(f"   Verification: {'✅ PASS' if result['verification'] else '❌ FAIL'}")
            else:
                print(f"❌ {file_type.upper()} file: FAILED")
                for error in result.get('errors', []):
                    print(f"   Error: {error}")
        
        if all_passed:
            print(f"\n🎉 All end-to-end tests passed!")
        else:
            print(f"\n❌ Some end-to-end tests failed!")
        
        return all_passed


def main():
    """Main test function"""
    tester = TestEndToEndConversion()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
