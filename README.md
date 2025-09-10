# Pytopspeed Modernized

A modernized Python library for converting Clarion TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite databases and back.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-88%20passing-brightgreen.svg)](tests/)

## 🚀 Features

- **Multi-format Support**: Convert .phd, .mod, .tps, and .phz files
- **Combined Conversion**: Merge multiple TopSpeed files into a single SQLite database
- **Reverse Conversion**: Convert SQLite databases back to TopSpeed files
- **PHZ Support**: Handle zip archives containing TopSpeed files
- **Progress Tracking**: Real-time progress reporting and detailed logging
- **Data Integrity**: Preserve all data types and relationships
- **CLI Interface**: Easy-to-use command-line tools
- **Python API**: Programmatic access to all functionality
- **Comprehensive Testing**: 88+ unit tests and integration tests

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

```bash
# Run all unit tests
python -m pytest tests/unit/ -v

# Run integration tests
python -m pytest tests/integration/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

**Test Results:**
- ✅ **88 unit tests** - All passing
- ✅ **Integration tests** - End-to-end conversion testing
- ✅ **Performance tests** - Benchmarking and optimization
- ✅ **Error handling tests** - Robust error handling validation

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