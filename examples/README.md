# Examples

This directory contains comprehensive examples demonstrating how to use the Pytopspeed Modernized library.

## 📚 Available Examples

### 1. Basic Conversion Examples

- **`file_type_usage_guide.py`** - **START HERE** - Complete guide to file types and converter usage
- **`convert_phd_to_sqlite.py`** - Convert a single .phd file to SQLite
- **`convert_combined_to_sqlite.py`** - Convert multiple TopSpeed files to a combined SQLite database
- **`convert_phz_to_sqlite.py`** - Convert a .phz (zip) file to SQLite
- **`convert_sqlite_to_topspeed.py`** - Convert SQLite back to TopSpeed files
- **`round_trip_conversion.py`** - Complete round-trip conversion example

### 2. Advanced Examples

- **`multidimensional_arrays.py`** - Handle TopSpeed multidimensional arrays and JSON conversion
- **`resilient_conversion_example.py`** - **NEW** - Enterprise resilience features for large databases
- **`robust_conversion_example.py`** - Robust error handling and recovery
- **`cli_usage_examples.py`** - Demonstrate CLI usage patterns
- **`performance_optimization_example.py`** - Performance optimization techniques

### 3. Integration Examples

- **`database_analysis.py`** - Analyze converted SQLite databases
- **`data_validation.py`** - Validate conversion results
- **`performance_benchmarking.py`** - Performance testing and benchmarking

## 🔧 File Type Usage Guide

| File Type | Extension | Example File | Converter Class | Method |
|-----------|-----------|--------------|-----------------|--------|
| **Single TopSpeed** | `.phd`, `.mod`, `.tps` | `convert_phd_to_sqlite.py` | `SqliteConverter` | `convert()` |
| **Multiple TopSpeed** | `.phd`, `.mod`, `.tps` | `convert_combined_to_sqlite.py` | `SqliteConverter` | `convert_multiple()` |
| **PHZ Archive** | `.phz` | `convert_phz_to_sqlite.py` | `PhzConverter` | `convert_phz()` |
| **Reverse Conversion** | `.sqlite` | `convert_sqlite_to_topspeed.py` | `ReverseConverter` | `convert_sqlite_to_topspeed()` |

## 🚀 Quick Start

### Start with the Usage Guide

```bash
# First, understand which converter to use for which file type
python examples/file_type_usage_guide.py
```

### Run Basic Examples

```bash
# Convert a single .phd/.mod/.tps file
python examples/convert_phd_to_sqlite.py

# Convert multiple TopSpeed files to combined database
python examples/convert_combined_to_sqlite.py

# Convert .phz (zip archive) file
python examples/convert_phz_to_sqlite.py

# Convert SQLite back to TopSpeed files
python examples/convert_sqlite_to_topspeed.py
```

### Run with Custom Files

```bash
# Use your own files
python examples/convert_phd_to_sqlite.py --input your_file.phd --output your_output.sqlite
```

## 📖 Example Descriptions

### Basic Conversion Examples

#### convert_phd_to_sqlite.py
Demonstrates the simplest conversion workflow:
- Load a TopSpeed file
- Convert to SQLite
- Display results

#### convert_combined_to_sqlite.py
Shows how to combine multiple TopSpeed files:
- Load multiple files (.phd, .mod)
- Apply automatic prefixing
- Create combined database

#### convert_phz_to_sqlite.py
Handles .phz (zip) files:
- Extract TopSpeed files from zip
- Convert extracted files
- Clean up temporary files

#### convert_sqlite_to_topspeed.py
Reverse conversion from SQLite to TopSpeed:
- Read SQLite database
- Generate TopSpeed files
- Handle table separation

#### round_trip_conversion.py
Complete round-trip conversion:
- TopSpeed → SQLite → TopSpeed
- Verify data integrity
- Compare file sizes

### Advanced Examples

#### resilient_conversion_example.py
**Enterprise resilience features for large databases:**
- Memory management with configurable limits
- Adaptive batch sizing based on table characteristics
- Progress tracking for long-running conversions
- Error recovery with partial conversion support
- Predefined configurations for different database sizes
- Performance optimization for enterprise-scale databases

#### robust_conversion_example.py
**Robust error handling and recovery:**
- Graceful handling of individual record failures
- Continuation of processing despite parsing errors
- Detailed error logging for troubleshooting
- Partial conversion support for interrupted operations

#### cli_usage_examples.py
**CLI usage patterns:**
- Help system
- Command variations
- Error handling

#### performance_optimization_example.py
**Performance optimization techniques:**
- SQLite tuning with WAL mode
- Memory optimization strategies
- Batch size optimization
- Parallel processing configuration

### Integration Examples

#### database_analysis.py
Analyze converted databases:
- Table structure analysis
- Data statistics
- Relationship mapping

#### data_validation.py
Validate conversion results:
- Record count verification
- Data type validation
- Integrity checks

#### performance_benchmarking.py
Performance testing:
- Speed benchmarks
- Memory usage analysis
- Optimization recommendations

## 🔧 Customization

### Modify Examples

All examples are designed to be easily customizable:

1. **Change input files**: Modify the file paths in the examples
2. **Adjust settings**: Change batch sizes, output formats, etc.
3. **Add features**: Extend examples with additional functionality
4. **Error handling**: Add your own error handling logic

### Example Template

```python
#!/usr/bin/env python3
"""
Example: Your Custom Example

Description of what this example demonstrates.
"""

import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.sqlite_converter import SqliteConverter

def main():
    """Main example function."""
    # Your code here
    pass

if __name__ == '__main__':
    main()
```

## 📊 Expected Outputs

### Conversion Results

All conversion examples provide detailed output:

```
✅ Conversion completed successfully!
   Tables created: 52
   Records migrated: 5027
   Duration: 3.90 seconds
   Output file: output.sqlite
```

### Error Handling

Examples demonstrate proper error handling:

```
❌ Conversion failed!
   Error: Input file not found: missing_file.phd
   Error: Permission denied: output.sqlite
```

### Progress Tracking

Examples show progress tracking:

```
[   0.0%] Migrating table: CLASS
[   1.9%] Migrating table: COMMENT
[   3.8%] Migrating table: CONVENTIONS
...
[ 100.0%] Conversion completed
```

## 🧪 Testing Examples

### Run All Examples

```bash
# Run all examples (requires sample data)
python examples/run_all_examples.py
```

### Test Specific Examples

```bash
# Test basic conversion
python examples/convert_phd_to_sqlite.py --test

# Test resilience features
python examples/resilient_conversion_example.py

# Test with validation
python examples/data_validation.py --input output.sqlite
```

### Resilience Examples

```bash
# Test enterprise resilience features
python examples/resilient_conversion_example.py

# Test robust error handling
python examples/robust_conversion_example.py

# Test performance optimization
python examples/performance_optimization_example.py
```

## 📝 Adding New Examples

### Create New Example

1. **Create new file** in `examples/` directory
2. **Follow naming convention**: `descriptive_name.py`
3. **Add docstring** with description
4. **Include error handling**
5. **Add to this README**

### Example Structure

```python
#!/usr/bin/env python3
"""
Example: Descriptive Name

Brief description of what this example demonstrates.
"""

import sys
import argparse
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from converter.sqlite_converter import SqliteConverter

def main():
    """Main example function."""
    parser = argparse.ArgumentParser(description='Example description')
    parser.add_argument('--input', default='assets/TxWells.PHD', help='Input file')
    parser.add_argument('--output', default='example_output.sqlite', help='Output file')
    args = parser.parse_args()
    
    try:
        # Example implementation
        converter = SqliteConverter()
        results = converter.convert(args.input, args.output)
        
        if results['success']:
            print(f"✅ Example completed successfully!")
            print(f"   Records: {results['total_records']}")
        else:
            print(f"❌ Example failed: {results['errors']}")
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
```

## 🔍 Troubleshooting Examples

### Common Issues

1. **File not found**: Check file paths in examples
2. **Permission errors**: Ensure write permissions for output files
3. **Memory issues**: Reduce batch sizes in examples
4. **Import errors**: Ensure you're running from the correct directory

### Debug Mode

Run examples with debug information:

```bash
# Enable verbose output
python examples/convert_phd_to_sqlite.py --verbose

# Enable debug logging
python examples/convert_phd_to_sqlite.py --debug
```

## 📚 Learning Path

### Beginner

1. Start with `convert_phd_to_sqlite.py`
2. Try `convert_combined_to_sqlite.py`
3. Explore `cli_usage_examples.py`

### Intermediate

1. Study `custom_progress_tracking.py`
2. Learn from `error_handling.py`
3. Experiment with `batch_processing.py`

### Advanced

1. Analyze `database_analysis.py`
2. Implement `data_validation.py`
3. Optimize with `performance_benchmarking.py`

## 🤝 Contributing Examples

### Submit New Examples

1. **Create example file** following the structure above
2. **Test thoroughly** with sample data
3. **Document clearly** with comments and docstrings
4. **Update this README** with your example
5. **Submit pull request**

### Example Guidelines

- **Clear purpose**: Each example should have a clear, specific purpose
- **Well documented**: Include comprehensive comments and docstrings
- **Error handling**: Include proper error handling
- **Configurable**: Allow customization through command-line arguments
- **Testable**: Include test mode or validation
