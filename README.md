# Pytopspeed Modernized

A modernized Python library for converting Clarion TopSpeed database files (.phd, .mod, .tps, .phz) to SQLite databases and back.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-376%20passing-brightgreen.svg)](tests/)

## 🚀 Features

- **Multi-format Support**: Convert .phd, .mod, .tps, and .phz files
- **Array Fields**: `DIM(n)` fields, members of dimensioned `GROUP`s and numbered fields (`PROD1`..`PROD5`) are stored as JSON arrays
- **Combined Conversion**: Merge multiple TopSpeed files into a single SQLite database
- **Reverse Conversion**: Write a converted (and optionally edited) SQLite database back to TopSpeed files that PHDWin can open
- **PHZ Support**: Handle zip archives containing TopSpeed files
- **Progress Tracking**: Progress callbacks and logging
- **Data Integrity**: Unchanged records round-trip byte for byte; `NULL` is kept distinct from zero
- **CLI Interface**: `pytopspeed convert | reverse | list`
- **Python API**: Programmatic access to all functionality
- **Tested**: 376 unit and integration tests, built on synthetic TopSpeed files

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
Use `ReverseConverter` to turn a converted database (including any edits) back into the original TopSpeed file(s):

```python
from converter.reverse_converter import ReverseConverter

converter = ReverseConverter()
result = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_directory/')
```

How it works:
- The forward conversion keeps what SQLite can't hold (table definitions, table numbers, file-level records and the original bytes of every row) in hidden `_topspeed_*` tables. Reverse conversion needs them, so it only works on databases created by this version; other databases fail with a "no TopSpeed metadata" error.
- Rows you didn't change are written back byte for byte. Edited rows keep their record number and only the changed columns are re-encoded. Inserted rows get new record numbers; deleted rows and their memos are dropped.
- Index entries and record counts are rebuilt from the final rows. A change that would put two rows on the same unique key is rejected with an error naming the table and key, and no file is written. A value that doesn't fit its field (too many array elements, a `DECIMAL` overflow) is rejected with an error naming the column.
- Output files keep their original names (e.g. `MyProject.PHD`, `MyProject.mod`). A database made from a `.phz` produces both files; zip them yourself if you need a `.phz` again.
- Rows are linked to their source records by SQLite `rowid`. If rowids change (`VACUUM` after deletes, or dropping and recreating a table), unchanged rows are still matched by content, but edited rows are written as new records with new record numbers.

Limits:
- Tables whose definition could not be parsed during forward conversion are written back unchanged; edits to them are ignored with a warning.
- Index keys are encoded for `BYTE`, `SHORT`, `LONG`, `DOUBLE` and `STRING` fields (including `NOCASE` and descending keys). If you edit a table with an index on another field type, you get a warning.
- Round-tripped and edited files have been opened in PHDWin 2.11 and read with the SoftVelocity TopSpeed ODBC driver. Keep a copy of the original file anyway.

The result is a dict with `success`, `files_created`, `tables_processed`, `records_processed`, `duration`, `errors` and `warnings`.

## 🔄 Array Handling

Array-like TopSpeed fields are stored as JSON arrays in SQLite `TEXT` columns:

1. **`DIM(n)` fields**: one JSON array column per field (e.g. `MONHIST` `PROD1`, 12 monthly `DOUBLE` values)
2. **Dimensioned `GROUP`s**: each member becomes its own JSON array with one element per group entry; the `GROUP` itself gets no column (e.g. `FORCAST` `NUMFORM` → `VALUETYPE`, `PCODE`, `PRODVALTYPE`, `ACTION`, `VALUEREAL`, `DATEVAL`)
3. **Numbered fields**: scalars named `NAME1`..`NAMEn` of the same type become one array column `NAME` (e.g. `CUMVOL` `PROD1`..`PROD5` → `PROD`). Fields such as `VALUE` and `VALUE2` stay separate.

### Example: MONHIST Table

```
# TopSpeed structure
PROD1        # DOUBLE, DIM(12)
PROD2        # DOUBLE, DIM(12)

# SQLite result
PROD1        # JSON: [0.0, 17226.0, 20428.0, ...]
PROD2        # JSON: [0.0, 0.0, 0.0, ...]
```

### Data Type Preservation

- **Zero vs NULL**: Zero values stay `0.0`; NaN and infinity become `NULL` (`null` inside arrays)
- **Byte Arrays**: `BYTE` arrays hold their numeric values (`[0, 1, 13]`); flags read as `0`/`1`, not `true`/`false`
- **Numeric Arrays**: Preserves `DOUBLE`, `LONG`, `SHORT` values
- **String Arrays**: Right-trimmed text

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

-- Number of elements in an array
SELECT LSE_ID,
       json_array_length(PROD1) as PROD1_Count
FROM MONHIST;
```

## 🛡️ Resilience Helpers

The `converter` package also contains standalone helper modules for large databases. They are not used by `SqliteConverter` or the CLI; call them yourself if you need them. See [docs/RESILIENCE_FEATURES.md](docs/RESILIENCE_FEATURES.md).

- **`ResilienceEnhancer`** (`converter.resilience_enhancements`): memory checks with psutil, forced garbage collection, adaptive batch sizes based on record size and field count, table size estimates
- **`ResilienceConfig`** (`converter.resilience_config`): predefined settings objects

```python
from converter.resilience_config import get_resilience_config

config = get_resilience_config('small')       # 200MB limit, batch size 200
config = get_resilience_config('medium')      # 500MB limit, batch size 100
config = get_resilience_config('large')       # 1GB limit, batch size 50
config = get_resilience_config('enterprise')  # 2GB limit, batch size 25
```

The configurations also carry flags such as `enable_parallel_processing` and `enable_checkpointing`; nothing in the converters acts on them yet.

The converter itself opens SQLite in WAL mode with `synchronous=NORMAL`, inserts in batches (`--batch-size`, default 1000), and logs and skips tables that fail to convert instead of stopping.

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

Installing the package provides the `pytopspeed` command. From a source checkout without installing, use `python src/cli/main.py` in its place.

```bash
# Convert a single .phd file to SQLite
pytopspeed convert MyProject.PHD output.sqlite

# Convert multiple files to a combined database
pytopspeed convert MyProject.PHD MyProject.mod combined.sqlite

# Convert a .phz file (zip archive)
pytopspeed convert MyProject.phz output.sqlite

# List contents of a .phz file
pytopspeed list MyProject.phz

# Convert SQLite back to TopSpeed files
pytopspeed reverse input.sqlite output_directory/
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
print(f"Files created: {results['files_created']}")
```

## 🚨 Common Issues and Solutions

### Wrong Converter for File Type
**Problem**: Using `SqliteConverter.convert()` with a `.phz` file
```python
# ❌ WRONG - This will fail: a .phz is a zip archive, not a TopSpeed file
converter = SqliteConverter()
result = converter.convert('input.phz', 'output.sqlite')
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

### Reverse Conversion Says "no TopSpeed metadata"
The database was not created by this version of the converter (or the `_topspeed_*` tables were dropped). Convert the original TopSpeed file again and redo your edits in the new database.

### PHDWin Keeps Reporting "File System Error"
After PHDWin fails to open one database, it can keep reporting "Version file is UNAVAILABLE ... File System Error" for every database it opens afterwards. Restart PHDWin before trying another file.

## 📊 Performance

The converted database is larger than the source file, because it also stores the original bytes of every record for reverse conversion (a ~7 MB `.PHD` became a ~37 MB SQLite database in testing). Use `--batch-size` to trade memory for fewer, larger inserts.

## 🔧 Command Line Interface

```bash
pytopspeed [-v | --verbose] [-q | --quiet] [--version] {convert,reverse,list} ...
```

**Global options** (put them before the command):
- `-v, --verbose` - Enable verbose logging
- `-q, --quiet` - Suppress progress output (log messages are still printed)
- `--version` - Print the version

### Convert Command

```bash
pytopspeed convert [--batch-size N] INPUT_FILES... OUTPUT_FILE
```

- `--batch-size N` - Number of records to insert per batch (default: 1000)
- Several `.phd`/`.mod`/`.tps` inputs are combined into one database with prefixed table names.
- A `.phz` must be converted on its own: only one per run, and it can't be mixed with other files.

### Reverse Command

```bash
pytopspeed reverse INPUT_FILE OUTPUT_DIRECTORY
```

### List Command

```bash
pytopspeed list PHZ_FILE
```

## 📚 Documentation

More documentation is in the `docs/` directory:

- **[Installation Guide](docs/INSTALLATION.md)** - Detailed installation instructions
- **[API Documentation](docs/API.md)** - API reference
- **[Troubleshooting Guide](docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[Developer Documentation](docs/DEVELOPER.md)** - Development and contribution guidelines
- **[Resilience Features](docs/RESILIENCE_FEATURES.md)** - The resilience helper modules
- **[PHD Format Analysis](docs/phd_format_analysis.md)** - Notes on the TopSpeed file format

## 🧪 Testing

```bash
# Unit and integration tests (376 tests)
python -m pytest tests/unit tests/integration

# Unit tests with coverage (what CI runs)
python -m pytest tests/unit --cov=src

# Performance tests are skipped unless enabled
python -m pytest tests/performance --run-performance

# Resilience-module tests only
python tests/run_resilience_tests.py [unit|integration|performance|all] [-c]
```

### Test Coverage

- **Unit tests (358)**: TopSpeed reader, TopSpeed writer, record codec, SQLite/PHZ/reverse converters, schema mapping, array handling, and the helper modules (batch processing, validation, error handling, resilience)
- **Integration tests (18)**: `.mod` file conversion and the resilience helpers
- **Performance tests (14)**: memory and throughput checks for the resilience helpers; run only with `--run-performance`

The tests build their TopSpeed input files synthetically (`tests/unit/tps_builder.py`), so no sample database is needed.

### Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) runs on pushes to `master`/`develop`, pull requests to `master`, and releases. It tests Python 3.8 to 3.12 on Ubuntu (flake8 syntax checks, mypy as a non-blocking check, unit tests with coverage), builds and checks the package, uploads pull-request builds to TestPyPI, and publishes releases to PyPI.

## 📖 Examples

Working examples are available in the [`examples/`](examples/) directory:

- **Basic conversion** - `convert_phd_to_sqlite.py`
- **Combined conversion** - `convert_combined_to_sqlite.py`
- **PHZ handling** - `convert_phz_to_sqlite.py`
- **Reverse conversion** - `convert_sqlite_to_topspeed.py`
- **Round-trip conversion** - `round_trip_conversion.py`
- **Array fields** - `multidimensional_arrays.py`
- **CLI usage** - `cli_usage_examples.py`

## 🏗️ Architecture

```
TopSpeed Files → Parser → Record Codec → SQLite Converter → SQLite Database
     ↓              ↓           ↓              ↓
   .phd/.mod    Modernized   Column layout  Data migration +
   .tps/.phz    pytopspeed   Type mapping   _topspeed_* metadata

SQLite Database → Reverse Converter → Record Codec (encode) → TopSpeed Writer → TopSpeed Files
```

### Key Components

- **TopSpeed Parser** (`pytopspeed`) - Modernized parser for reading TopSpeed files
- **TopSpeed Writer** (`pytopspeed/tpswriter.py`) - Builds TopSpeed files: pages, B-tree, compression, indexes
- **Record Codec** (`converter/record_codec.py`) - Maps a table definition to SQLite columns and decodes/encodes records
- **Schema Mapper** - Table and column name sanitization; type mapping for the fallback path
- **SQLite Converter** - Handles data migration and conversion
- **PHZ Converter** - Processes zip archives containing TopSpeed files
- **Reverse Converter** - Converts SQLite back to TopSpeed files
- **CLI Interface** (`cli/main.py`) - The `pytopspeed` command

## 🔄 Data Type Conversion

| TopSpeed Type | SQLite Type | Notes |
|---------------|-------------|-------|
| BYTE | INTEGER | 8-bit unsigned integer |
| SHORT, USHORT | INTEGER | 16-bit integer |
| LONG, ULONG | INTEGER | 32-bit integer |
| FLOAT, DOUBLE | REAL | NaN and infinity become `NULL` |
| DECIMAL | REAL | Floating point number |
| DATE | TEXT | Format: `YYYY-MM-DD`; `NULL` if the date is zero |
| TIME | TEXT | Format: `HH:MM:SS.cc` |
| STRING, CSTRING, PSTRING | TEXT | Right-trimmed; blank becomes `''` |
| MEMO | BLOB column | Holds the memo's full text |
| Arrays (`DIM`, dimensioned `GROUP` members, numbered fields) | TEXT | JSON array |

Columns follow the order of the table definition. SQLite indexes are created for TopSpeed keys whose fields map to plain (non-array) columns.

## 🎯 Key Features

### Table Name Prefixing

When converting multiple files, tables are automatically prefixed to avoid conflicts:

- **.phd files** → `phd_` prefix (e.g., `phd_OWNER`, `phd_CLASS`)
- **.mod files** → `mod_` prefix (e.g., `mod_DEPRECIATION`, `mod_MODID`)
- **.tps files** → `tps_` prefix
- **Other files** → `file_N_` prefix
- A second file of the same type gets the file's position in the list added (e.g. `phd2_`)

### Name Sanitization

Column and table names are sanitized for SQLite compatibility:

- **Prefix removal**: `TIT:PROJ_DESCR` → `PROJ_DESCR`
- **Special characters**: spaces, `-`, `.`, `/`, `\` → `_`
- **Numeric prefixes**: `123FIELD` → `_123FIELD`
- **Reserved words** (table names): `ORDER` → `ORDER_TABLE`

### Error Handling

- **Graceful degradation** - A table that fails to convert is logged and skipped; the rest of the file is still converted
- **Fallback parsing** - Tables whose definition can't be parsed are converted by an older heuristic path
- **Detailed logging** - Use `-v` for debug output

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
python -m pytest tests/unit tests/integration
```

Tests must use synthetic data; don't add tests that depend on real client databases.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Based on the original [pytopspeed library](https://github.com/dylangiles/pytopspeed/)
- Modernized for Python 3.8+ and construct 2.10+
- Enhanced with SQLite conversion and reverse conversion capabilities

## 📞 Support

- **Documentation**: See the `docs/` directory
- **Examples**: Check the `examples/` directory for working code
- **Issues**: Open an issue in the project repository

---

**Ready to convert your TopSpeed files?** Start with the [Installation Guide](docs/INSTALLATION.md) and try the [examples](examples/)!
