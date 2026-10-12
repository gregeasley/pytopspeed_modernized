#!/usr/bin/env python3
"""
Example: Performance Optimization

Demonstrates advanced performance optimization capabilities including:
- Parallel processing for multiple files
- Memory-efficient streaming
- Intelligent caching
- Performance monitoring and reporting
"""

import sys
import argparse
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.performance_optimizer import PerformanceOptimizer


def progress_callback(current, total, message=""):
    """Progress callback function.

    Defined at module level so it can be pickled: parallel processing
    runs files in separate processes.
    """
    if total > 0:
        percentage = (current / total) * 100
        print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)
    else:
        print(f"\r{message}", end='', flush=True)


def main():
    """Main example function."""
    parser = argparse.ArgumentParser(description='Performance Optimization Example')
    parser.add_argument('--input-files', nargs='+', 
                       default=['assets/TxWells.PHD', 'assets/TxWells.mod'],
                       help='Input TopSpeed files to process')
    parser.add_argument('--output', default='performance_optimized_output.sqlite',
                       help='Output SQLite database file')
    parser.add_argument('--strategy', choices=['memory', 'speed', 'balanced'],
                       default='balanced', help='Optimization strategy')
    parser.add_argument('--parallel', action='store_true', default=True,
                       help='Enable parallel processing')
    parser.add_argument('--streaming', action='store_true', default=True,
                       help='Enable memory-efficient streaming')
    parser.add_argument('--caching', action='store_true', default=True,
                       help='Enable intelligent caching')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Maximum number of worker processes')
    parser.add_argument('--memory-limit', type=int, default=1024,
                       help='Memory limit in MB')
    parser.add_argument('--cache-size', type=int, default=1000,
                       help='Cache size for frequently accessed data')
    parser.add_argument('--report', default='performance_report.txt',
                       help='Output file for performance report')
    
    args = parser.parse_args()
    
    try:
        print("⚡ Performance Optimization Example")
        print("=" * 50)
        
        # Initialize performance optimizer
        optimizer = PerformanceOptimizer(
            max_workers=args.max_workers,
            memory_limit_mb=args.memory_limit,
            cache_size=args.cache_size,
            progress_callback=progress_callback
        )
        
        print(f"📁 Processing {len(args.input_files)} files:")
        for file_path in args.input_files:
            print(f"   - {file_path}")
        
        print(f"🎯 Optimization strategy: {args.strategy}")
        print(f"⚡ Parallel processing: {'Enabled' if args.parallel else 'Disabled'}")
        print(f"💾 Memory streaming: {'Enabled' if args.streaming else 'Disabled'}")
        print(f"🗄️  Intelligent caching: {'Enabled' if args.caching else 'Disabled'}")
        print(f"👥 Max workers: {args.max_workers}")
        print(f"💾 Memory limit: {args.memory_limit} MB")
        print(f"🗄️  Cache size: {args.cache_size}")
        print()
        
        # Perform optimized conversion
        print("🚀 Starting optimized conversion...")
        results = optimizer.optimize_conversion(
            input_files=args.input_files,
            output_file=args.output,
            optimization_strategy=args.strategy,
            enable_parallel=args.parallel,
            enable_streaming=args.streaming,
            enable_caching=args.caching
        )
        
        print()  # New line after progress
        print()
        
        # Display conversion results
        print("📋 CONVERSION RESULTS")
        print("=" * 50)
        print(f"✅ Success: {results['success']}")
        print(f"📁 Files Processed: {results['files_processed']}")
        print(f"🗃️  Tables Created: {results['tables_created']}")
        print(f"📊 Total Records: {results['total_records']}")
        print(f"⏱️  Duration: {results['duration']:.2f} seconds")
        
        if results['files_processed'] > 0 and results['duration'] > 0:
            throughput = results['total_records'] / results['duration']
            print(f"📈 Throughput: {throughput:.0f} records/second")
        
        print()
        
        # Display performance metrics
        if results['performance_metrics']:
            metrics = results['performance_metrics']
            print("⚡ PERFORMANCE METRICS")
            print("-" * 30)
            print(f"📊 Throughput: {metrics.get('throughput_records_per_sec', 0):.0f} records/sec")
            print(f"💾 Avg Memory Usage: {metrics.get('avg_memory_usage', 0):.1f}%")
            print(f"🖥️  Avg CPU Usage: {metrics.get('avg_cpu_usage', 0):.1f}%")
            print(f"🗄️  Cache Hit Rate: {metrics.get('cache_hit_rate', 0):.2%}")
            print()
        
        # Display optimization configuration
        if results['optimization_config']:
            config = results['optimization_config']
            print("⚙️  OPTIMIZATION CONFIGURATION")
            print("-" * 30)
            print(f"🔄 Parallel Processing: {'Enabled' if config['parallel_processing'] else 'Disabled'}")
            print(f"💾 Streaming: {'Enabled' if config['streaming'] else 'Disabled'}")
            print(f"🗄️  Caching: {'Enabled' if config['caching'] else 'Disabled'}")
            print(f"📦 Batch Size: {config['batch_size']}")
            print(f"💾 Buffer Size: {config['memory_buffer_size']} bytes")
            print(f"🗄️  Cache TTL: {config['cache_ttl']} seconds")
            print(f"📥 Prefetch Size: {config['prefetch_size']}")
            print(f"🗜️  Compression: {'Enabled' if config['compression'] else 'Disabled'}")
            print()
        
        # Display errors
        if results['errors']:
            print("❌ ERRORS")
            print("-" * 30)
            for error in results['errors']:
                print(f"   {error}")
            print()
        
        # Generate performance report
        print("📝 Generating performance report...")
        report_content = optimizer.get_performance_report()
        with open(args.report, 'w') as f:
            f.write(report_content)
        print(f"✅ Performance report written to: {args.report}")
        print()
        
        # Display summary
        print("🎯 OPTIMIZATION SUMMARY")
        print("=" * 50)
        if results['success']:
            print("✅ Optimized conversion completed successfully!")
            print(f"📊 Processed {results['total_records']} records from {results['files_processed']} files")
            print(f"🗃️  Created {results['tables_created']} tables in {results['output']}")
            
            if results['duration'] > 0:
                throughput = results['total_records'] / results['duration']
                print(f"📈 Achieved {throughput:.0f} records/second throughput")
            
            if results['performance_metrics']:
                metrics = results['performance_metrics']
                if metrics.get('avg_memory_usage', 0) > 0:
                    print(f"💾 Average memory usage: {metrics['avg_memory_usage']:.1f}%")
                
                if metrics.get('avg_cpu_usage', 0) > 0:
                    print(f"🖥️  Average CPU usage: {metrics['avg_cpu_usage']:.1f}%")
                
                if metrics.get('cache_hit_rate', 0) > 0:
                    print(f"🗄️  Cache hit rate: {metrics['cache_hit_rate']:.2%}")
            
            print(f"⏱️  Total processing time: {results['duration']:.2f} seconds")
            
            # Performance recommendations
            print()
            print("💡 PERFORMANCE RECOMMENDATIONS")
            print("-" * 30)
            if results['performance_metrics']:
                metrics = results['performance_metrics']
                
                if metrics.get('avg_memory_usage', 0) > 80:
                    print("⚠️  High memory usage detected - consider reducing batch size")
                
                if metrics.get('avg_cpu_usage', 0) < 50:
                    print("💡 Low CPU usage - consider increasing parallel workers")
                
                if metrics.get('cache_hit_rate', 0) < 0.5:
                    print("💡 Low cache hit rate - consider increasing cache size")
                
                if results['duration'] > 0:
                    throughput = results['total_records'] / results['duration']
                    if throughput < 1000:
                        print("💡 Low throughput - consider enabling parallel processing")
        else:
            print("❌ Optimized conversion failed!")
            print(f"   Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   - {error}")
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
