# Examples

This directory contains examples demonstrating how to use the Pytopspeed Modernized library.

All examples add `src/` to `sys.path` themselves, so they run from a source checkout without installing the package. Run them from the repository root: several examples use the default sample paths `assets/TxWells.PHD`, `assets/TxWells.mod` and `assets/TxWells.phz`, and the examples write their output files (SQLite databases, reports, logs) into the current directory unless you pass an output path. The sample files are not shipped with the package; substitute your own TopSpeed files where needed.

## 📚 Available Examples

### 1. Basic Conversion Examples

- **`file_type_usage_guide.py`** - **START HERE** - Prints which converter and method to use for each file type (no files needed)
- **`convert_phd_to_sqlite.py`** - Convert a single .phd/.mod/.tps file to SQLite
- **`convert_combined_to_sqlite.py`** - Convert multiple TopSpeed files to one combined SQLite database
- **`convert_phz_to_sqlite.py`** - Convert a .phz (zip) file to SQLite
- **`convert_sqlite_to_topspeed.py`** - Convert SQLite back to TopSpeed files
- **`round_trip_conversion.py`** - TopSpeed → SQLite → TopSpeed in one run

### 2. Advanced Examples

- **`multidimensional_arrays.py`** - How TopSpeed arrays (DIM fields) are stored as JSON arrays and queried in SQLite
- **`resilient_conversion_example.py`** - Resilience helpers (configuration presets, size categories, memory checks, adaptive batch sizes)
- **`robust_conversion_example.py`** - Error handler, recovery strategies, backups and checkpoints
- **`cli_usage_examples.py`** - Runs the command-line interface (`src/cli/main.py`) help and `list` commands
- **`batch_processing_example.py`** - `BatchProcessor` with merge strategies and relationship analysis (see Known Issues)
- **`data_validation_example.py`** - `DataValidator` comparing a TopSpeed file with its SQLite conversion (see Known Issues)
- **`performance_optimization_example.py`** - `PerformanceOptimizer` with parallel/streaming/caching options (see Known Issues)

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
python examples/convert_phd_to_sqlite.py assets/TxWells.PHD output.sqlite

# Convert multiple TopSpeed files to a combined database (tables prefixed phd_, mod_, tps_)
python examples/convert_combined_to_sqlite.py assets/TxWells.PHD assets/TxWells.mod combined.sqlite

# Convert a .phz (zip archive) file
python examples/convert_phz_to_sqlite.py assets/TxWells.phz output.sqlite

# Convert SQLite back to TopSpeed files (writes TxWells.PHD / TxWells.mod into output_dir/)
python examples/convert_sqlite_to_topspeed.py combined.sqlite output_dir/

# Complete round trip (single file, several files, or one .phz)
python examples/round_trip_conversion.py assets/TxWells.PHD assets/TxWells.mod round_trip/
```

The three forward-conversion examples accept `--batch-size N` (default 1000).

## 📖 Example Descriptions

### Basic Conversion Examples

#### convert_phd_to_sqlite.py
Demonstrates the simplest conversion workflow:
- Convert one TopSpeed file with `SqliteConverter.convert()`
- Display results (tables, records, duration)
- List the data tables and show sample rows

#### convert_combined_to_sqlite.py
Shows how to combine multiple TopSpeed files:
- Convert several files with `SqliteConverter.convert_multiple()`
- Tables are prefixed by source file type (`phd_`, `mod_`, `tps_`)
- Per-file results and a summary of the combined database

#### convert_phz_to_sqlite.py
Handles .phz (zip) files:
- List the archive contents with `PhzConverter.list_phz_contents()`
- Extract and convert the contained .phd/.mod files into one database (`phd_`/`mod_` prefixes)
- Note: on Windows the converter may log a warning that it could not delete its temporary extraction folder; the conversion itself succeeds

#### convert_sqlite_to_topspeed.py
Reverse conversion from SQLite to TopSpeed:
- The database must have been created by this version's converter (it contains hidden `_topspeed_*` metadata tables); other databases are rejected with a "no TopSpeed metadata" error
- Writes the TopSpeed files under their original names (e.g. `TxWells.PHD`, `TxWells.mod`); a database made from a .phz yields the individual files, not a new .phz
- Unchanged rows are written back byte for byte; edited, inserted and deleted rows are applied and indexes are rebuilt

#### round_trip_conversion.py
Complete round-trip conversion:
- TopSpeed → SQLite (`intermediate.sqlite`) → TopSpeed (`reverse_output/`)
- Compares record counts and file sizes
- Accepts one file, several files, or a single .phz

### Advanced Examples

#### multidimensional_arrays.py
Uses `assets/TxWells.PHD`:
- Lists DIM (array) fields found in the table definitions
- Converts the file and finds JSON array columns (e.g. `FORCAST.VALUEREAL`, `CUMVOL.PROD`)
- Queries arrays with SQLite `json_array_length()` / `json_extract()`
- Shows that integer (BYTE/SHORT/LONG) arrays are stored as JSON numbers

#### resilient_conversion_example.py
Uses `assets/TxWells.phz`:
- Estimates a size category with `estimate_database_size_category()` and picks a preset with `get_resilience_config()`
- Converts the .phz with `PhzConverter` (the converters do not take a `ResilienceConfig`; the preset is printed for reference)
- Demonstrates `ResilienceEnhancer` memory checks, garbage collection and adaptive batch sizes

#### robust_conversion_example.py
Demonstrates the error-handling building blocks (no full conversion is run):
- Logging errors by category and severity with `ErrorHandler`, and writing `error_demo_report.json`
- Input file validation with `RobustConverter`
- Built-in and custom recovery strategies
- Backup/restore and checkpoint/restore

#### cli_usage_examples.py
Runs `src/cli/main.py` (installed as the `pytopspeed` command):
- `--help` for the main command and for `convert`, `reverse` and `list`
- `list assets/TxWells.phz`

#### batch_processing_example.py
`BatchProcessor.process_batch()` with `--merge-strategy {prefix,namespace,separate}`, relationship analysis and a text report (`batch_processing_report.txt`).

#### data_validation_example.py
Converts `assets/TxWells.PHD` to `validation_test.sqlite` and runs `DataValidator` at `--validation-level {basic,standard,comprehensive}`.

#### performance_optimization_example.py
`PerformanceOptimizer.optimize_conversion()` with `--strategy {memory,speed,balanced}` and a performance report (`performance_report.txt`).

## ⚠️ Known Issues

The following examples run but currently report failure with the TxWells sample files because of problems in the library modules they demonstrate (not in the examples):

- `batch_processing_example.py` - `BatchProcessor` fails while merging (`no such table: file_0_CLASS` with `prefix`, `db_0_CLASS` with `namespace`, `table _topspeed_file already exists` with `separate`)
- `data_validation_example.py` - the conversion succeeds, but `DataValidator` fails with `'TpsTablesList' object is not iterable`
- `performance_optimization_example.py` - `PerformanceOptimizer` fails in both parallel mode (`cannot pickle '_thread.lock' object`) and sequential mode (`'TpsTablesList' object is not iterable`)

For production conversions use `SqliteConverter`, `PhzConverter`, `ReverseConverter` or the `pytopspeed` CLI.

## 📊 Expected Outputs

### Conversion Results

Conversion examples print a summary like this (`convert_phd_to_sqlite.py` with `assets/TxWells.PHD`):

```
Success: True
Tables created: 55
Total records migrated: 9492
Duration: 6.37 seconds

✅ Conversion completed successfully!
```

The combined TxWells.PHD + TxWells.mod conversion creates 14,759 records (9,492 + 5,267). On the development machine each forward conversion took under 10 seconds and each reverse conversion under 20 seconds; your timings will vary.

### Error Handling

```
❌ Error: Input file not found: missing_file.phd
```

### Progress Tracking

Examples show progress through their progress callbacks:

```
Progress: 0/55 (0.0%) - Migrating table: CLASS
Progress: 1/55 (1.8%) - Migrating table: COMMENT
Progress: 2/55 (3.6%) - Migrating table: CONVENTIONS
...
```

## 🧪 Testing Examples

There is no runner script for the examples; run them individually. The automated tests live in `tests/` (see the main README).

```bash
# Test resilience helpers
python examples/resilient_conversion_example.py

# Test robust error handling
python examples/robust_conversion_example.py

# Test array handling
python examples/multidimensional_arrays.py
```

## 🔧 Customization

### Modify Examples

All examples are designed to be easily customizable:

1. **Change input files**: Pass your own paths on the command line, or edit the default paths
2. **Adjust settings**: Change batch sizes, output paths, etc.
3. **Add features**: Extend examples with additional functionality
4. **Error handling**: Add your own error handling logic

### Example Template

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
        converter = SqliteConverter()
        results = converter.convert(args.input, args.output)
        
        if results['success']:
            print(f"✅ Example completed successfully!")
            print(f"   Records: {results['total_records']}")
        else:
            print(f"❌ Example failed: {results['errors']}")
            return 1
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
```

## 🔍 Troubleshooting Examples

### Common Issues

1. **File not found**: Run from the repository root, or pass explicit file paths
2. **Permission errors**: Ensure write permissions for output files
3. **Reverse conversion fails with "no TopSpeed metadata"**: Re-create the SQLite database with this version's converter
4. **Import errors**: Run the scripts from a source checkout (they add `../src` to the path)

## 📚 Learning Path

### Beginner

1. Start with `file_type_usage_guide.py`
2. Run `convert_phd_to_sqlite.py`
3. Try `convert_combined_to_sqlite.py` and `convert_phz_to_sqlite.py`

### Intermediate

1. Run `round_trip_conversion.py`
2. Explore `multidimensional_arrays.py`
3. Try `cli_usage_examples.py`

### Advanced

1. Study `resilient_conversion_example.py`
2. Study `robust_conversion_example.py`

## 🤝 Contributing Examples

### Submit New Examples

1. **Create example file** following the template above
2. **Test it** with your own sample data (the TxWells sample files are not distributed)
3. **Document clearly** with comments and docstrings
4. **Update this README** with your example
5. **Submit pull request**

### Example Guidelines

- **Clear purpose**: Each example should have a clear, specific purpose
- **Well documented**: Include comments and docstrings
- **Error handling**: Include proper error handling
- **Configurable**: Allow customization through command-line arguments
