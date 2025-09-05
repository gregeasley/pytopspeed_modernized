#!/usr/bin/env python3
"""
Example: Advanced Batch Processing

Demonstrates the advanced batch processing capabilities including:
- Multiple file processing with relationship analysis
- Parallel processing for improved performance
- Cross-file relationship detection
- Comprehensive batch processing reports
"""

import sys
import argparse
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.batch_processor import BatchProcessor


def main():
    """Main example function."""
    parser = argparse.ArgumentParser(description='Advanced Batch Processing Example')
    parser.add_argument('--input-files', nargs='+', 
                       default=['assets/TxWells.PHD', 'assets/TxWells.mod'],
                       help='Input TopSpeed files to process')
    parser.add_argument('--output', default='batch_processing_output.sqlite',
                       help='Output SQLite database file')
    parser.add_argument('--merge-strategy', choices=['prefix', 'namespace', 'separate'],
                       default='prefix', help='Strategy for merging files')
    parser.add_argument('--parallel', action='store_true', default=True,
                       help='Enable parallel processing')
    parser.add_argument('--analyze-relationships', action='store_true', default=True,
                       help='Analyze cross-file relationships')
    parser.add_argument('--max-workers', type=int, default=4,
                       help='Maximum number of worker processes')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for processing')
    parser.add_argument('--report', default='batch_processing_report.txt',
                       help='Output file for batch processing report')
    
    args = parser.parse_args()
    
    def progress_callback(current, total, message=""):
        """Progress callback function."""
        if total > 0:
            percentage = (current / total) * 100
            print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)
        else:
            print(f"\r{message}", end='', flush=True)
    
    try:
        print("🚀 Advanced Batch Processing Example")
        print("=" * 50)
        
        # Initialize batch processor
        processor = BatchProcessor(
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            progress_callback=progress_callback
        )
        
        print(f"📁 Processing {len(args.input_files)} files:")
        for file_path in args.input_files:
            print(f"   - {file_path}")
        
        print(f"📊 Merge strategy: {args.merge_strategy}")
        print(f"⚡ Parallel processing: {'Enabled' if args.parallel else 'Disabled'}")
        print(f"🔍 Relationship analysis: {'Enabled' if args.analyze_relationships else 'Disabled'}")
        print()
        
        # Process files
        print("🔄 Starting batch processing...")
        results = processor.process_batch(
            input_files=args.input_files,
            output_file=args.output,
            merge_strategy=args.merge_strategy,
            relationship_analysis=args.analyze_relationships,
            parallel_processing=args.parallel
        )
        
        print()  # New line after progress
        print()
        
        # Display results
        print("📋 BATCH PROCESSING RESULTS")
        print("=" * 50)
        print(f"✅ Success: {results['success']}")
        print(f"📁 Files Processed: {results['files_processed']}")
        print(f"🗃️  Tables Created: {results['tables_created']}")
        print(f"📊 Total Records: {results['total_records']}")
        print(f"🔗 Relationships Found: {results['relationships_found']}")
        print(f"⏱️  Duration: {results['duration']:.2f} seconds")
        
        if results['files_processed'] > 0:
            print(f"📈 Average Records/Second: {results['total_records'] / results['duration']:.0f}")
        
        print()
        
        # Display file details
        if results['file_details']:
            print("📄 FILE DETAILS")
            print("-" * 30)
            for file_path, details in results['file_details'].items():
                print(f"📁 {Path(file_path).name}")
                print(f"   Tables: {details.get('tables_created', 0)}")
                print(f"   Records: {details.get('total_records', 0)}")
                print(f"   Duration: {details.get('duration', 0):.2f}s")
                print()
        
        # Display relationships
        if results['relationship_map']:
            relationships = results['relationship_map']
            
            if relationships.get('table_overlaps'):
                print("🔗 TABLE OVERLAPS")
                print("-" * 30)
                for table, files in relationships['table_overlaps'].items():
                    file_names = [Path(f).name for f in files]
                    print(f"📋 {table}: {', '.join(file_names)}")
                print()
            
            if relationships.get('schema_similarities'):
                print("📊 SCHEMA SIMILARITIES")
                print("-" * 30)
                for comparison, similarity in relationships['schema_similarities'].items():
                    print(f"🔍 {comparison}: {similarity:.2%}")
                print()
        
        # Display errors
        if results['errors']:
            print("❌ ERRORS")
            print("-" * 30)
            for error in results['errors']:
                print(f"   {error}")
            print()
        
        # Generate report
        print("📝 Generating batch processing report...")
        report_content = processor.generate_batch_report(results, args.report)
        print(f"✅ Report written to: {args.report}")
        
        # Display summary
        print()
        print("🎯 SUMMARY")
        print("=" * 50)
        if results['success']:
            print("✅ Batch processing completed successfully!")
            print(f"📊 Processed {results['total_records']} records from {results['files_processed']} files")
            print(f"🗃️  Created {results['tables_created']} tables in {results['output']}")
            
            if results['relationships_found'] > 0:
                print(f"🔗 Found {results['relationships_found']} cross-file relationships")
            
            print(f"⏱️  Total processing time: {results['duration']:.2f} seconds")
        else:
            print("❌ Batch processing failed!")
            print(f"   Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   - {error}")
        
        return 0 if results['success'] else 1
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
