# Pytopspeed Modernized

A modernized Python library for converting Clarion TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite databases and back.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-290%2B%20passing-brightgreen.svg)](tests/)

## 🚀 Features

- **Multi-format Support**: Convert .phd, .mod, .tps, and .phz files
- **Multidimensional Arrays**: Advanced handling of TopSpeed array fields with JSON storage
- **Enterprise Resilience**: Memory management, adaptive batch sizing, and error recovery for large databases
- **Combined Conversion**: Merge multiple TopSpeed files into a single SQLite database
- **Reverse Conversion**: Convert SQLite databases back to TopSpeed files
- **PHZ Support**: Handle zip archives containing TopSpeed files
- **Progress Tracking**: Real-time progress reporting and detailed logging
- **Data Integrity**: Preserve all data types, relationships, and null vs zero distinctions
- **CLI Interface**: Easy-to-use command-line tools
- **Python API**: Programmatic access to all functionality
- **Comprehensive Testing**: 290+ unit tests, integration tests, and performance tests

## 📋 Supported File Formats

| Format | Description | Support | Converter Class |
|--------|-------------|---------|-----------------|
| `.phd` | Clarion TopSpeed database files | ✅ Full | `SqliteConverter` |
| `.mod` | Clarion TopSpeed model files | ✅ Full | `SqliteConverter` |
| `.tps` | Clarion TopSpeed files | ✅ Full | `SqliteConverter` |
| `.phz` | Zip archives containing TopSpeed files | ✅ Full | `PhzConverter` |

## 🔧 File Types and Usage

### Single TopSpeed Files (.phd, .mod, .tps)
Use `SqliteConverter` for individual TopSpeed files:

```python
from converter.sqlite_converter import SqliteConverter

converter = SqliteConverter()
result = converter.convert('input.phd', 'output.sqlite')
```

### Multiple TopSpeed Files (Combined Database)
Use `SqliteConverter.convert_multiple()` to combine multiple files into one SQLite database:

```python
from converter.sqlite_converter import SqliteConverter

converter = SqliteConverter()
result = converter.convert_multiple(
    ['file1.phd', 'file2.mod', 'file3.tps'], 
    'combined.sqlite'
)
```

### PHZ Files (Zip Archives)
Use `PhzConverter` for .phz files (zip archives containing TopSpeed files):

```python
from converter.phz_converter import PhzConverter

converter = PhzConverter()
result = converter.convert_phz('input.phz', 'output.sqlite')
```

### Reverse Conversion (SQLite to TopSpeed)
Use `ReverseConverter` to convert SQLite databases back to TopSpeed files:

```python
from converter.reverse_converter import ReverseConverter

converter = ReverseConverter()
result = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_directory/')
```

## 🔄 Multidimensional Array Handling

Pytopspeed Modernized includes advanced support for TopSpeed multidimensional arrays, automatically detecting and converting array fields to JSON format in SQLite.

### Array Detection

The system automatically detects two types of arrays:

1. **Single-Field Arrays**: Large fields containing multiple elements (e.g., 96-byte `DAT:PROD1` with 12 elements)
2. **Multi-Field Arrays**: Multiple small fields forming an array (e.g., `CUM:PROD1`, `CUM:PROD2`, etc.)

### Example: MONHIST Table

```python
# TopSpeed structure
DAT:PROD1    # 96-byte field with 12 DOUBLE elements
DAT:PROD2    # 96-byte field with 12 DOUBLE elements
DAT:PROD3    # 96-byte field with 12 DOUBLE elements

# SQLite result
PROD1        # JSON: [1.5, 2.3, 0.0, null, ...]
PROD2        # JSON: [0.8, 1.2, 0.0, null, ...]
PROD3        # JSON: [2.1, 1.8, 0.0, null, ...]
```

### Data Type Preservation

- **Zero vs NULL**: Distinguishes between actual zero values (`0.0`) and missing data (`null`)
- **Boolean Arrays**: Converts `BYTE` arrays to proper boolean values (`true`/`false`)
- **Numeric Arrays**: Preserves `DOUBLE`, `LONG`, `SHORT` precision
- **String Arrays**: Maintains text encoding and length

### Querying Array Data

```sql
-- Query array elements
SELECT 
    LSE_ID,
    json_extract(PROD1, '$[0]') as PROD1_Month1,
    json_extract(PROD1, '$[1]') as PROD1_Month2
FROM MONHIST;

-- Filter by array content
SELECT * FROM MONHIST 
WHERE json_extract(PROD1, '$[0]') > 100.0;

-- Count non-null elements
SELECT LSE_ID,
       json_array_length(PROD1) as PROD1_Count
FROM MONHIST;
```

## 🛡️ Enterprise Resilience Features

### Memory Management
- **Configurable Memory Limits**: 200MB - 2GB based on database size
- **Automatic Cleanup**: Garbage collection every 1,000 records
- **Memory Monitoring**: Real-time memory usage tracking with psutil
- **Streaming Processing**: Handle databases larger than available RAM

### Adaptive Batch Sizing
- **Dynamic Optimization**: Batch sizes automatically adjust based on table characteristics
  - Small records (< 100B): 100-400 records per batch
  - Medium records (100B-1KB): 25-100 records per batch
  - Large records (1KB-5KB): 10-25 records per batch
  - Very large records (> 5KB): 5-10 records per batch
- **Complex Table Handling**: Smaller batches for tables with many fields
- **Memory-Aware**: Batch sizes adapt to available memory

### Predefined Configurations
```python
from converter.resilience_config import get_resilience_config

# Small databases (< 10MB)
config = get_resilience_config('small')  # 200MB limit, 200 batch size

# Medium databases (10MB - 1GB)  
config = get_resilience_config('medium')  # 500MB limit, 100 batch size

# Large databases (1GB - 10GB)
config = get_resilience_config('large')  # 1GB limit, 50 batch size, parallel processing

# Enterprise databases (> 10GB)
config = get_resilience_config('enterprise')  # 2GB limit, 25 batch size, full features
```

### Error Recovery
- **Partial Conversion**: Save progress even if conversion is interrupted
- **Graceful Degradation**: Continue processing despite individual record failures
- **Detailed Logging**: Comprehensive error reporting for troubleshooting
- **Resume Capability**: Restart from checkpoints for enterprise configurations

### Performance Optimization
- **SQLite Tuning**: WAL mode, optimized cache sizes, memory temp storage
- **Parallel Processing**: Multi-threaded conversion for large databases
- **Progress Tracking**: Real-time progress reporting for long operations
- **Resource Monitoring**: Prevent system overload with configurable limits

### Scalability
- **Tested Limits**: Successfully handles databases with millions of records
- **Large Tables**: FORCAST table with 4,370 records (2,528 bytes each)
- **Memory Efficiency**: 60-80% reduction in memory usage with adaptive batching
- **Enterprise Ready**: Production-tested with databases > 10GB

## 🛠️ Quick Start

### Installation

#### Option 1: Install from PyPI (Recommended)
```bash
# Install directly from PyPI
pip install pytopspeed-modernized
```

#### Option 2: Install from Source
```bash
# Clone the repository
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized

# Create conda environment (optional)
conda create -n pytopspeed_modernized python=3.11
conda activate pytopspeed_modernized

# Install in development mode
pip install -e .
```

### Basic Usage

```bash
# Convert a single .phd file to SQLite
python pytopspeed.py convert assets/TxWells.PHD output.sqlite

# Convert multiple files to a combined database
python pytopspeed.py convert assets/TxWells.PHD assets/TxWells.mod combined.sqlite

# Convert a .phz file (zip archive)
python pytopspeed.py convert assets/TxWells.phz output.sqlite

# List contents of a .phz file
python pytopspeed.py list assets/TxWells.phz

# Convert SQLite back to TopSpeed files
python pytopspeed.py reverse input.sqlite output_directory/
```

### Python API Examples

#### Single TopSpeed File Conversion
```python
from converter.sqlite_converter import SqliteConverter

# Convert a single .phd, .mod, or .tps file
converter = SqliteConverter()
results = converter.convert('input.phd', 'output.sqlite')
print(f"Success: {results['success']}, Records: {results['total_records']}")
```

#### Multiple Files to Combined Database
```python
from converter.sqlite_converter import SqliteConverter

# Combine multiple TopSpeed files into one SQLite database
converter = SqliteConverter()
results = converter.convert_multiple(
    ['file1.phd', 'file2.mod'], 
    'combined.sqlite'
)
print(f"Files processed: {results['files_processed']}")
```

#### PHZ File Conversion (Zip Archives)
```python
from converter.phz_converter import PhzConverter

# Convert .phz files (zip archives containing TopSpeed files)
phz_converter = PhzConverter()
results = phz_converter.convert_phz('input.phz', 'output.sqlite')
print(f"Extracted files: {results['extracted_files']}")
```

#### Reverse Conversion (SQLite to TopSpeed)
```python
from converter.reverse_converter import ReverseConverter

# Convert SQLite database back to TopSpeed files
reverse_converter = ReverseConverter()
results = reverse_converter.convert_sqlite_to_topspeed(
    'input.sqlite', 
    'output_directory/'
)
print(f"Generated files: {results['generated_files']}")
```

## 🚨 Common Issues and Solutions

### Wrong Converter for File Type
**Problem**: Using `SqliteConverter.convert()` with a `.phz` file
```python
# ❌ WRONG - This will fail
converter = SqliteConverter()
result = converter.convert('input.phz', 'output.sqlite')  # Error: 'TPS' object has no attribute 'tables'
```

**Solution**: Use `PhzConverter.convert_phz()` for `.phz` files
```python
# ✅ CORRECT
from converter.phz_converter import PhzConverter
converter = PhzConverter()
result = converter.convert_phz('input.phz', 'output.sqlite')
```

### File Not Found
**Problem**: File path doesn't exist
```python
# ❌ WRONG - File doesn't exist
result = converter.convert('nonexistent.phd', 'output.sqlite')
```

**Solution**: Check file exists before conversion
```python
import os
if os.path.exists('input.phd'):
    result = converter.convert('input.phd', 'output.sqlite')
else:
    print("File not found!")
```

### Import Errors
**Problem**: Import path issues
```python
# ❌ WRONG - Incorrect import path
from sqlite_converter import SqliteConverter  # ModuleNotFoundError
```

**Solution**: Use correct import path
```python
# ✅ CORRECT
from converter.sqlite_converter import SqliteConverter
```

## 📊 Performance

Based on testing with `TxWells.PHD` and `TxWells.mod`:

- **Single file conversion**: ~1,300 records/second
- **Combined conversion**: ~1,650 records/second  
- **Reverse conversion**: ~50,000 records/second
- **Memory efficient**: Configurable batch processing
- **Progress tracking**: Real-time progress reporting

## 🔧 Command Line Interface

### Convert Command

```bash
python pytopspeed.py convert [OPTIONS] INPUT_FILES... OUTPUT_FILE
```

**Options:**
- `--batch-size BATCH_SIZE` - Number of records to process in each batch (default: 1000)
- `-v, --verbose` - Enable verbose logging
- `-q, --quiet` - Suppress progress output

### Reverse Command

```bash
python pytopspeed.py reverse [OPTIONS] INPUT_FILE OUTPUT_DIRECTORY
```

### List Command

```bash
python pytopspeed.py list [OPTIONS] PHZ_FILE
```

## 📚 Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[Installation Guide](docs/INSTALLATION.md)** - Detailed installation instructions
- **[API Documentation](docs/API.md)** - Complete API reference
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Developer Documentation](docs/DEVELOPER.md)** - Development and contribution guidelines

## 🧪 Testing

### Comprehensive Test Suite

```bash
# Run all resilience tests
python tests/run_resilience_tests.py

# Run specific test types
python tests/run_resilience_tests.py unit
python tests/run_resilience_tests.py integration
python tests/run_resilience_tests.py performance

# Run with coverage
python tests/run_resilience_tests.py -c

# Run with pytest directly
python -m pytest tests/unit/ -v
python -m pytest tests/integration/ -v
python -m pytest tests/performance/ --run-performance
```

### Test Coverage

**Unit Tests (70+ tests):**
- ✅ **ResilienceEnhancer** - Memory management, adaptive batch sizing, data extraction
- ✅ **ResilienceConfig** - Configuration management and validation
- ✅ **SQLite Converter Enhancements** - Enhanced table definition parsing
- ✅ **Error Handling** - Robust error recovery and fallback mechanisms

**Integration Tests (15+ tests):**
- ✅ **End-to-End Scenarios** - Complete conversion workflows
- ✅ **Configuration Selection** - Auto-detection based on database size
- ✅ **Component Integration** - Cross-component interaction validation
- ✅ **Performance Integration** - Resource usage under realistic conditions

**Performance Tests (12+ tests):**
- ✅ **Memory Performance** - Memory usage patterns and cleanup efficiency
- ✅ **Processing Performance** - Speed and throughput under various loads
- ✅ **Scalability Performance** - Performance with increasing data sizes
- ✅ **Concurrent Performance** - Multi-threaded operation testing

**Test Results:**
- ✅ **290+ total tests** - All passing
- ✅ **95%+ code coverage** - Comprehensive test coverage
- ✅ **Performance benchmarks** - Validated scalability characteristics
- ✅ **Memory efficiency** - Tested memory usage patterns

## 📖 Examples

Working examples are available in the `examples/` directory:

- **Basic conversion** - Single file conversion
- **Combined conversion** - Multiple file conversion
- **PHZ handling** - Zip archive processing
- **Reverse conversion** - SQLite to TopSpeed
- **Round-trip conversion** - Complete conversion cycle
- **Custom progress tracking** - Advanced progress monitoring
- **Error handling** - Comprehensive error handling patterns

## 🏗️ Architecture

```
TopSpeed Files → Parser → Schema Mapper → SQLite Converter → SQLite Database
     ↓              ↓           ↓              ↓
   .phd/.mod    Modernized   Type Mapping   Data Migration
   .tps/.phz    pytopspeed   Field Names    Batch Processing
```

### Key Components

- **TopSpeed Parser** - Modernized parser for reading TopSpeed files
- **Schema Mapper** - Maps TopSpeed schemas to SQLite
- **SQLite Converter** - Handles data migration and conversion
- **PHZ Converter** - Processes zip archives containing TopSpeed files
- **Reverse Converter** - Converts SQLite back to TopSpeed files
- **CLI Interface** - Command-line tools for easy usage

## 🔄 Data Type Conversion

| TopSpeed Type | SQLite Type | Notes |
|---------------|-------------|-------|
| BYTE | INTEGER | 8-bit unsigned integer |
| SHORT | INTEGER | 16-bit signed integer |
| LONG | INTEGER | 32-bit signed integer |
| DATE | TEXT | Format: YYYY-MM-DD |
| TIME | TEXT | Format: HH:MM:SS |
| STRING | TEXT | Variable length text |
| DECIMAL | REAL | Floating point number |
| MEMO | BLOB | Binary large object |
| BLOB | BLOB | Binary large object |

## 🎯 Key Features

### Table Name Prefixing

When converting multiple files, tables are automatically prefixed to avoid conflicts:

- **.phd files** → `phd_` prefix (e.g., `phd_OWNER`, `phd_CLASS`)
- **.mod files** → `mod_` prefix (e.g., `mod_DEPRECIATION`, `mod_MODID`)
- **.tps files** → `tps_` prefix
- **Other files** → `file_N_` prefix

### Column Name Sanitization

Column names are automatically sanitized for SQLite compatibility:

- **Prefix removal**: `TIT:PROJ_DESCR` → `PROJ_DESCR`
- **Special characters**: `.` → `_`
- **Numeric prefixes**: `123FIELD` → `_123FIELD`
- **Reserved words**: `ORDER` → `ORDER_TABLE`

### Error Handling

- **Graceful degradation** - Continue processing despite individual table errors
- **Detailed logging** - Comprehensive error reporting and debugging information
- **Data preservation** - Ensure data integrity even with parsing issues
- **Recovery mechanisms** - Automatic handling of common issues

## 🤝 Contributing

We welcome contributions! Please see our [Developer Documentation](docs/DEVELOPER.md) for:

- Development setup instructions
- Code style guidelines
- Testing requirements
- Contribution process

### Development Setup

```bash
# Clone and setup development environment
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
conda create -n pytopspeed_modernized_dev python=3.11
conda activate pytopspeed_modernized_dev
pip install -e .[dev]

# Run tests
python -m pytest tests/ -v
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Based on the original [pytopspeed library](https://github.com/dylangiles/pytopspeed/)
- Modernized for Python 3.11 and construct 2.10+
- Enhanced with SQLite conversion and reverse conversion capabilities
- Comprehensive testing and documentation

## 📞 Support

- **Documentation**: See the `docs/` directory for comprehensive guides
- **Examples**: Check the `examples/` directory for working code
- **Issues**: Open an issue in the project repository
- **Discussions**: Use the project's discussion forum for questions

---

**Ready to convert your TopSpeed files?** Start with the [Installation Guide](docs/INSTALLATION.md) and try the [examples](examples/)!