# Pytopspeed Modernized - TopSpeed to SQLite Converter

A modernized Python library for converting Clarion TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite databases and back.

## 🚀 Quick Start

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

# Create a conda environment (optional)
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

## 📋 Supported File Formats

- **.phd** - Clarion TopSpeed database files
- **.mod** - Clarion TopSpeed model files  
- **.tps** - Clarion TopSpeed files
- **.phz** - Zip archives containing .phd and .mod files

## 🛠️ Command Line Interface

### Convert Command

Convert TopSpeed files to SQLite databases:

```bash
python pytopspeed.py convert [OPTIONS] INPUT_FILES... OUTPUT_FILE
```

**Options:**
- `--batch-size BATCH_SIZE` - Number of records to process in each batch (default: 1000)
- `-v, --verbose` - Enable verbose logging
- `-q, --quiet` - Suppress progress output

**Examples:**
```bash
# Single file conversion
python pytopspeed.py convert assets/TxWells.PHD output.sqlite

# Multiple file conversion with custom batch size
python pytopspeed.py convert assets/TxWells.PHD assets/TxWells.mod combined.sqlite --batch-size 500

# PHZ file conversion
python pytopspeed.py convert assets/TxWells.phz output.sqlite
```

### Reverse Command

Convert SQLite databases back to TopSpeed files:

```bash
python pytopspeed.py reverse [OPTIONS] INPUT_FILE OUTPUT_DIRECTORY
```

**Examples:**
```bash
# Convert SQLite back to TopSpeed files
python pytopspeed.py reverse input.sqlite output_directory/

# With verbose output
python pytopspeed.py reverse input.sqlite output_directory/ --verbose
```

### List Command

List contents of .phz files:

```bash
python pytopspeed.py list [OPTIONS] PHZ_FILE
```

**Examples:**
```bash
# List PHZ file contents
python pytopspeed.py list assets/TxWells.phz
```

## 📊 Conversion Features

### Table Name Prefixing

When converting multiple files, tables are automatically prefixed to avoid conflicts:

- **.phd files** → `phd_` prefix (e.g., `phd_OWNER`, `phd_CLASS`)
- **.mod files** → `mod_` prefix (e.g., `mod_DEPRECIATION`, `mod_MODID`)
- **.tps files** → `tps_` prefix
- **Other files** → `file_N_` prefix

### Data Type Conversion

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

### Column Name Sanitization

Column names are automatically sanitized for SQLite compatibility:

- **Prefix removal**: `TIT:PROJ_DESCR` → `PROJ_DESCR`
- **Special characters**: `.` → `_`
- **Numeric prefixes**: `123FIELD` → `_123FIELD`
- **Reserved words**: `ORDER` → `ORDER_TABLE`

## 🔧 Python API Usage

### Basic Conversion

```python
from converter.sqlite_converter import SqliteConverter

# Single file conversion
converter = SqliteConverter()
results = converter.convert('input.phd', 'output.sqlite')

# Multiple file conversion
results = converter.convert_multiple(['file1.phd', 'file2.mod'], 'combined.sqlite')
```

### PHZ File Conversion

```python
from converter.phz_converter import PhzConverter

converter = PhzConverter()
results = converter.convert_phz('input.phz', 'output.sqlite')
```

### Reverse Conversion

```python
from converter.reverse_converter import ReverseConverter

converter = ReverseConverter()
results = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_directory/')
```

### Progress Tracking

```python
def progress_callback(current, total, message=""):
    if total > 0:
        percentage = (current / total) * 100
        print(f"[{percentage:6.1f}%] {message}")

converter = SqliteConverter(progress_callback=progress_callback)
results = converter.convert('input.phd', 'output.sqlite')
```

## 📈 Performance

### Benchmarks

Based on testing with `TxWells.PHD` and `TxWells.mod`:

- **Single file conversion**: ~1,300 records/second
- **Combined conversion**: ~1,650 records/second  
- **Reverse conversion**: ~50,000 records/second

### Optimization Tips

1. **Batch size**: Adjust `--batch-size` for your system (default: 1000)
2. **Memory usage**: Larger batch sizes use more memory but may be faster
3. **SSD storage**: Use SSD storage for better I/O performance
4. **Multiple files**: Combined conversion is more efficient than separate conversions

## 🐛 Troubleshooting

### Common Issues

**1. "stream read less than specified amount" errors**
- These are parsing warnings for corrupted or incomplete table definitions
- The conversion will continue and skip problematic tables
- Check the log output for details

**2. "charmap codec can't decode" errors**
- Character encoding issues in text data
- Non-ASCII characters are handled gracefully
- Data is preserved but may have encoding warnings

**3. "Table name collision" warnings**
- Multiple files have tables with the same name
- Automatic prefixing resolves conflicts
- Check the log for specific collision details

**4. "File not found" errors**
- Verify file paths are correct
- Check file permissions
- Ensure files are not locked by other applications

### Debug Mode

Enable verbose logging for detailed information:

```bash
python pytopspeed.py convert input.phd output.sqlite --verbose
```

### Log Files

The converter provides detailed logging including:
- File processing progress
- Table creation details
- Data migration statistics
- Error messages and warnings

## 📚 Examples

See the `examples/` directory for complete working examples:

- `convert_phd_to_sqlite.py` - Basic single file conversion
- `convert_combined_to_sqlite.py` - Multiple file conversion
- `convert_phz_to_sqlite.py` - PHZ file conversion
- `convert_sqlite_to_topspeed.py` - Reverse conversion
- `round_trip_conversion.py` - Complete round-trip example

## 🤝 Contributing

See [Developer Documentation](DEVELOPER.md) for contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Based on the original [pytopspeed library](https://github.com/dylangiles/pytopspeed/)
- Modernized for Python 3.11 and construct 2.10+
- Enhanced with SQLite conversion and reverse conversion capabilities
