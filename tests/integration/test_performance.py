#!/usr/bin/env python3
"""
Performance testing for TopSpeed to SQLite conversion
"""

import os
import sys
import sqlite3
import tempfile
import time
import psutil
import gc
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed import TPS
from converter.sqlite_converter import SqliteConverter


class PerformanceTester:
    """Performance testing for conversion process"""
    
    def __init__(self):
        self.test_files = [
            ('assets/TxWells.PHD', 'phd'),
            ('assets/TxWells.mod', 'mod')
        ]
        self.results = {}
    
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def test_conversion_performance(self, input_file, file_type, batch_sizes=[100, 500, 1000]):
        """Test conversion performance with different batch sizes"""
        print(f"\n⚡ Performance Testing: {file_type.upper()} file")
        print("=" * 60)
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False
        
        file_size = os.path.getsize(input_file)
        print(f"File: {input_file}")
        print(f"Size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        
        performance_results = {}
        
        for batch_size in batch_sizes:
            print(f"\n🔧 Testing batch size: {batch_size}")
            
            # Create temporary output file
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
                output_file = tmp.name
            
            try:
                # Force garbage collection
                gc.collect()
                
                # Record initial memory
                initial_memory = self.get_memory_usage()
                
                # Initialize converter
                converter = SqliteConverter(batch_size=batch_size)
                
                # Record start time
                start_time = time.time()
                
                # Convert the file
                results = converter.convert(input_file, output_file)
                
                # Record end time
                end_time = time.time()
                duration = end_time - start_time
                
                # Record peak memory
                peak_memory = self.get_memory_usage()
                
                if results['success']:
                    records_per_second = results['total_records'] / duration
                    mb_per_second = (file_size / 1024 / 1024) / duration
                    memory_used = peak_memory - initial_memory
                    
                    print(f"   ✅ Success!")
                    print(f"   Duration: {duration:.2f} seconds")
                    print(f"   Records/sec: {records_per_second:.0f}")
                    print(f"   MB/sec: {mb_per_second:.1f}")
                    print(f"   Memory used: {memory_used:.1f} MB")
                    print(f"   Tables: {results['tables_created']}")
                    print(f"   Records: {results['total_records']:,}")
                    
                    performance_results[batch_size] = {
                        'success': True,
                        'duration': duration,
                        'records_per_second': records_per_second,
                        'mb_per_second': mb_per_second,
                        'memory_used': memory_used,
                        'tables_created': results['tables_created'],
                        'total_records': results['total_records']
                    }
                else:
                    print(f"   ❌ Failed!")
                    for error in results['errors']:
                        print(f"   Error: {error}")
                    
                    performance_results[batch_size] = {
                        'success': False,
                        'errors': results['errors']
                    }
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.unlink(output_file)
        
        # Find optimal batch size
        successful_results = {k: v for k, v in performance_results.items() if v['success']}
        if successful_results:
            best_batch_size = max(successful_results.keys(), 
                                key=lambda k: successful_results[k]['records_per_second'])
            best_performance = successful_results[best_batch_size]
            
            print(f"\n🏆 Optimal batch size: {best_batch_size}")
            print(f"   Best performance: {best_performance['records_per_second']:.0f} records/sec")
            print(f"   Best speed: {best_performance['mb_per_second']:.1f} MB/sec")
        
        self.results[file_type] = performance_results
        return len(successful_results) > 0
    
    def test_memory_usage(self, input_file, file_type):
        """Test memory usage during conversion"""
        print(f"\n🧠 Memory Usage Testing: {file_type.upper()} file")
        print("=" * 60)
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False
        
        # Test with different batch sizes to see memory impact
        batch_sizes = [50, 100, 500, 1000, 2000]
        memory_results = {}
        
        for batch_size in batch_sizes:
            print(f"\n🔧 Testing batch size: {batch_size}")
            
            # Create temporary output file
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
                output_file = tmp.name
            
            try:
                # Force garbage collection
                gc.collect()
                
                # Record initial memory
                initial_memory = self.get_memory_usage()
                
                # Initialize converter
                converter = SqliteConverter(batch_size=batch_size)
                
                # Convert the file
                results = converter.convert(input_file, output_file)
                
                # Record peak memory
                peak_memory = self.get_memory_usage()
                memory_used = peak_memory - initial_memory
                
                if results['success']:
                    print(f"   Memory used: {memory_used:.1f} MB")
                    print(f"   Peak memory: {peak_memory:.1f} MB")
                    print(f"   Records: {results['total_records']:,}")
                    
                    memory_results[batch_size] = {
                        'memory_used': memory_used,
                        'peak_memory': peak_memory,
                        'records': results['total_records']
                    }
                else:
                    print(f"   ❌ Conversion failed!")
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.unlink(output_file)
        
        # Analyze memory efficiency
        if memory_results:
            print(f"\n📊 Memory Analysis:")
            for batch_size, result in memory_results.items():
                memory_per_record = result['memory_used'] / result['records'] * 1000  # KB per record
                print(f"   Batch {batch_size}: {result['memory_used']:.1f} MB ({memory_per_record:.2f} KB/record)")
        
        return len(memory_results) > 0
    
    def test_large_file_handling(self, input_file, file_type):
        """Test handling of large files"""
        print(f"\n📁 Large File Handling Test: {file_type.upper()} file")
        print("=" * 60)
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False
        
        file_size = os.path.getsize(input_file)
        print(f"File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
        
        # Test with different batch sizes for large file handling
        batch_sizes = [100, 500, 1000, 2000]
        
        for batch_size in batch_sizes:
            print(f"\n🔧 Testing batch size: {batch_size}")
            
            # Create temporary output file
            with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
                output_file = tmp.name
            
            try:
                # Force garbage collection
                gc.collect()
                
                # Record initial memory
                initial_memory = self.get_memory_usage()
                
                # Initialize converter
                converter = SqliteConverter(batch_size=batch_size)
                
                # Record start time
                start_time = time.time()
                
                # Convert the file
                results = converter.convert(input_file, output_file)
                
                # Record end time
                end_time = time.time()
                duration = end_time - start_time
                
                # Record peak memory
                peak_memory = self.get_memory_usage()
                memory_used = peak_memory - initial_memory
                
                if results['success']:
                    print(f"   ✅ Success!")
                    print(f"   Duration: {duration:.2f} seconds")
                    print(f"   Memory used: {memory_used:.1f} MB")
                    print(f"   Records: {results['total_records']:,}")
                    print(f"   Records/sec: {results['total_records'] / duration:.0f}")
                else:
                    print(f"   ❌ Failed!")
                    for error in results['errors']:
                        print(f"   Error: {error}")
                
            finally:
                # Clean up
                if os.path.exists(output_file):
                    os.unlink(output_file)
        
        return True
    
    def test_progress_reporting(self, input_file, file_type):
        """Test progress reporting functionality"""
        print(f"\n📊 Progress Reporting Test: {file_type.upper()} file")
        print("=" * 60)
        
        if not os.path.exists(input_file):
            print(f"❌ File not found: {input_file}")
            return False
        
        # Create temporary output file
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            output_file = tmp.name
        
        try:
            # Track progress callbacks
            progress_updates = []
            
            def progress_callback(current, total, message):
                progress_updates.append({
                    'current': current,
                    'total': total,
                    'message': message,
                    'timestamp': time.time()
                })
                print(f"   Progress: {current}/{total} ({current/total*100:.1f}%) - {message}")
            
            # Initialize converter with progress callback
            converter = SqliteConverter(batch_size=100, progress_callback=progress_callback)
            
            # Convert the file
            results = converter.convert(input_file, output_file)
            
            if results['success']:
                print(f"\n📈 Progress Analysis:")
                print(f"   Total progress updates: {len(progress_updates)}")
                
                if progress_updates:
                    # Analyze progress timing
                    first_update = progress_updates[0]
                    last_update = progress_updates[-1]
                    total_time = last_update['timestamp'] - first_update['timestamp']
                    
                    print(f"   Progress duration: {total_time:.2f} seconds")
                    print(f"   Updates per second: {len(progress_updates) / total_time:.1f}")
                    
                    # Show sample progress messages
                    print(f"\n   Sample progress messages:")
                    for update in progress_updates[:5]:  # First 5
                        print(f"   {update['current']}/{update['total']}: {update['message']}")
                    
                    if len(progress_updates) > 5:
                        print(f"   ... and {len(progress_updates) - 5} more")
                
                print(f"   ✅ Progress reporting working correctly!")
                return True
            else:
                print(f"   ❌ Conversion failed!")
                return False
                
        finally:
            # Clean up
            if os.path.exists(output_file):
                os.unlink(output_file)
    
    def run_all_tests(self):
        """Run all performance tests"""
        print("🚀 Starting Performance Tests")
        print("=" * 80)
        
        all_passed = True
        
        for input_file, file_type in self.test_files:
            # Test 1: Conversion performance
            perf_ok = self.test_conversion_performance(input_file, file_type)
            
            if perf_ok:
                # Test 2: Memory usage
                memory_ok = self.test_memory_usage(input_file, file_type)
                
                # Test 3: Large file handling
                large_file_ok = self.test_large_file_handling(input_file, file_type)
                
                # Test 4: Progress reporting
                progress_ok = self.test_progress_reporting(input_file, file_type)
                
                if not (memory_ok and large_file_ok and progress_ok):
                    all_passed = False
            else:
                all_passed = False
        
        # Summary
        print(f"\n📊 Performance Test Summary")
        print("=" * 80)
        
        for file_type, results in self.results.items():
            print(f"\n{file_type.upper()} file results:")
            
            successful_results = {k: v for k, v in results.items() if v['success']}
            if successful_results:
                best_batch = max(successful_results.keys(), 
                               key=lambda k: successful_results[k]['records_per_second'])
                best_perf = successful_results[best_batch]
                
                print(f"   Best batch size: {best_batch}")
                print(f"   Best performance: {best_perf['records_per_second']:.0f} records/sec")
                print(f"   Best speed: {best_perf['mb_per_second']:.1f} MB/sec")
                print(f"   Memory efficiency: {best_perf['memory_used']:.1f} MB")
            else:
                print(f"   ❌ No successful conversions")
        
        if all_passed:
            print(f"\n🎉 All performance tests passed!")
        else:
            print(f"\n❌ Some performance tests failed!")
        
        return all_passed


def main():
    """Main test function"""
    tester = PerformanceTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
