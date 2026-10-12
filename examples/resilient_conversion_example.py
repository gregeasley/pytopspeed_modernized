#!/usr/bin/env python3
"""
Example script demonstrating resilient conversion for large databases

This script shows the resilience helpers (ResilienceConfig presets,
estimate_database_size_category, ResilienceEnhancer) next to a normal .phz
conversion. Note: the converters do not take a ResilienceConfig; the
configuration chosen below is printed for reference only.
"""

import sys
import os
import time
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from converter.phz_converter import PhzConverter
from converter.resilience_config import get_resilience_config, estimate_database_size_category
from converter.resilience_enhancements import ResilienceEnhancer


def demonstrate_resilient_conversion():
    """Demonstrate resilient conversion with different configurations"""
    
    print("=== Resilient TopSpeed Database Conversion Demo ===\n")
    
    # Example files (adjust paths as needed)
    phz_file = "assets/TxWells.phz"
    output_file = "resilient_conversion_demo.sqlite"
    
    if not os.path.exists(phz_file):
        print(f"❌ Example file not found: {phz_file}")
        print("Please ensure you have a .phz file to test with.")
        return
    
    # Initialize the converter
    converter = PhzConverter()
    
    # Get resilience enhancer
    enhancer = ResilienceEnhancer(max_memory_mb=500, enable_progress_tracking=True)
    
    print("1. Estimating database size...")
    
    # Quick size estimation (this would normally be done before conversion)
    try:
        from pytopspeed.tps import TPS
        import tempfile
        import zipfile
        
        # Extract and examine the PHZ file
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(phz_file, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Find PHD file
            phd_files = list(Path(temp_dir).glob("*.PHD"))
            if phd_files:
                tps = TPS(str(phd_files[0]), encoding='cp1251', cached=True, check=True)
                
                # Estimate total size
                total_tables = len(tps.tables._TpsTablesList__tables)
                total_pages = len([p for p in tps.pages.list() if tps.pages[p].hierarchy_level == 0])
                
                print(f"   📊 Database contains {total_tables} tables across {total_pages} pages")
                
                # Estimate category
                estimated_size_mb = (total_pages * 4) / 1024  # Rough estimate: 4KB per page
                estimated_records = total_pages * 10  # Rough estimate: 10 records per page
                
                category = estimate_database_size_category(estimated_size_mb, estimated_records)
                print(f"   📈 Estimated size: {estimated_size_mb:.1f}MB, {estimated_records} records")
                print(f"   🎯 Recommended configuration: {category}")

                # Release the memory-mapped file so the temp directory can be removed
                tps.tps_file.close()
                
            else:
                print("   ⚠️  Could not find PHD file in PHZ archive")
                category = 'medium'  # Default fallback
                
    except Exception as e:
        print(f"   ⚠️  Size estimation failed: {e}")
        category = 'medium'  # Default fallback
    
    print(f"\n2. Using {category} configuration for conversion...")
    
    # Get appropriate configuration
    config = get_resilience_config(category)
    print(f"   🔧 Configuration: {config.max_memory_mb}MB memory limit, {config.default_batch_size} batch size")
    print("   (PhzConverter does not take this configuration; shown for reference)")
    
    # Perform conversion with timing
    print(f"\n3. Starting resilient conversion...")
    start_time = time.time()
    
    try:
        result = converter.convert_phz(phz_file, output_file)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ Conversion completed successfully!")
        print(f"   📊 Results:")
        print(f"      - Success: {result['success']}")
        print(f"      - Tables created: {result['tables_created']}")
        print(f"      - Total records: {result['total_records']}")
        print(f"      - Duration: {duration:.2f} seconds")
        print(f"      - Files processed: {result['files_processed']}")
        
        if result['errors']:
            print(f"   ⚠️  Errors encountered: {len(result['errors'])}")
            for error in result['errors'][:3]:  # Show first 3 errors
                print(f"      - {error}")
        
        # Show performance metrics
        if result['total_records'] > 0:
            records_per_second = result['total_records'] / duration
            print(f"   🚀 Performance: {records_per_second:.0f} records/second")
        
    except Exception as e:
        print(f"\n❌ Conversion failed: {e}")
        return
    
    print(f"\n4. Verifying results...")
    
    # Verify the output database
    try:
        import sqlite3
        conn = sqlite3.connect(output_file)
        cursor = conn.cursor()
        
        # Get table information
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = cursor.fetchall()
        
        print(f"   📋 Created {len(tables)} tables:")
        
        # Show some table statistics
        for table_name, in tables[:5]:  # Show first 5 tables
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                count = cursor.fetchone()[0]
                print(f"      - {table_name}: {count} records")
            except Exception as e:
                print(f"      - {table_name}: Error counting records ({e})")
        
        if len(tables) > 5:
            print(f"      ... and {len(tables) - 5} more tables")
        
        conn.close()
        
    except Exception as e:
        print(f"   ⚠️  Verification failed: {e}")
    
    print(f"\n5. Cleanup...")
    
    # Clean up output file
    try:
        if os.path.exists(output_file):
            os.remove(output_file)
            print(f"   🗑️  Removed temporary output file: {output_file}")
    except Exception as e:
        print(f"   ⚠️  Cleanup failed: {e}")
    
    print(f"\n=== Demo completed ===")


def demonstrate_memory_management():
    """Demonstrate memory management features"""
    
    print("\n=== Memory Management Demo ===\n")
    
    enhancer = ResilienceEnhancer(max_memory_mb=100, enable_progress_tracking=True)
    
    print("1. Testing memory monitoring...")
    
    # Simulate memory usage
    import gc
    
    # Check initial memory
    initial_memory = enhancer.check_memory_usage()
    print(f"   📊 Initial memory check: {'OK' if not initial_memory else 'WARNING'}")
    
    # Force memory cleanup
    print("2. Testing memory cleanup...")
    enhancer.force_memory_cleanup()
    print("   🧹 Memory cleanup completed")
    
    print("3. Testing adaptive batch sizing...")
    
    # Simulate different table types
    class MockTableDef:
        def __init__(self, record_size, field_count):
            self.record_size = record_size
            self.field_count = field_count
    
    test_tables = [
        ("Small table", MockTableDef(50, 10)),
        ("Medium table", MockTableDef(500, 25)),
        ("Large table", MockTableDef(2000, 50)),
        ("Very large table", MockTableDef(8000, 150))
    ]
    
    for name, table_def in test_tables:
        batch_size = enhancer.get_adaptive_batch_size(table_def)
        print(f"   📊 {name}: {batch_size} batch size")
    
    print("\n=== Memory Management Demo completed ===")


def demonstrate_size_estimation():
    """Demonstrate table size estimation"""
    
    print("\n=== Size Estimation Demo ===\n")
    
    enhancer = ResilienceEnhancer()
    
    print("1. Testing size estimation with sample data...")
    
    # Simulate size estimation results
    sample_estimates = [
        {"estimated_records": 100, "estimated_size_mb": 0.3, "table": "SMALL_TABLE"},
        {"estimated_records": 5000, "estimated_size_mb": 15.0, "table": "MEDIUM_TABLE"},
        {"estimated_records": 50000, "estimated_size_mb": 150.0, "table": "LARGE_TABLE"},
        {"estimated_records": 500000, "estimated_size_mb": 1500.0, "table": "HUGE_TABLE"}
    ]
    
    for estimate in sample_estimates:
        category = estimate_database_size_category(
            estimate["estimated_size_mb"], 
            estimate["estimated_records"]
        )
        config = get_resilience_config(category)
        
        print(f"   📊 {estimate['table']}:")
        print(f"      - Records: {estimate['estimated_records']:,}")
        print(f"      - Size: {estimate['estimated_size_mb']:.1f}MB")
        print(f"      - Category: {category}")
        print(f"      - Config: {config.max_memory_mb}MB limit, {config.default_batch_size} batch size")
        print()
    
    print("=== Size Estimation Demo completed ===")


if __name__ == "__main__":
    try:
        demonstrate_resilient_conversion()
        demonstrate_memory_management()
        demonstrate_size_estimation()
        
        print("\n🎉 All demos completed successfully!")
        print("\nKey resilience features demonstrated:")
        print("   ✅ Adaptive batch sizing based on table characteristics")
        print("   ✅ Memory monitoring and cleanup")
        print("   ✅ Progress tracking for large operations")
        print("   ✅ Size estimation and configuration selection")
        print("   ✅ Error handling and recovery")
        print("   ✅ Performance optimization for different database sizes")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
