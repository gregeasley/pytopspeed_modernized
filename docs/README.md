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

## 📋 Supported File Formats

- **.phd** - Clarion TopSpeed database files
- **.mod** - Clarion TopSpeed model files  
- **.tps** - Clarion TopSpeed files
- **.phz** - Zip archives containing .phd and .mod files

## 🛠️ Command Line Interface

```bash
pytopspeed [-v | --verbose] [-q | --quiet] [--version] {convert,reverse,list} ...
```

**Global options** (put them before the command):
- `-v, --verbose` - Enable verbose logging
- `-q, --quiet` - Suppress progress output (log messages are still printed)
- `--version` - Print the version

### Convert Command

Convert TopSpeed files to SQLite databases:

```bash
pytopspeed convert [--batch-size N] INPUT_FILES... OUTPUT_FILE
```

**Options:**
- `--batch-size N` - Number of records to insert per batch (default: 1000)

Several `.phd`/`.mod`/`.tps` inputs are combined into one database. A `.phz` must be converted on its own: only one per run, and it can't be mixed with other files.

**Examples:**
```bash
# Single file conversion
pytopspeed convert MyProject.PHD output.sqlite

# Multiple file conversion with custom batch size
pytopspeed convert --batch-size 500 MyProject.PHD MyProject.mod combined.sqlite

# PHZ file conversion
pytopspeed convert MyProject.phz output.sqlite
```

### Reverse Command

Convert a database created by `pytopspeed convert` back to TopSpeed files:

```bash
pytopspeed reverse INPUT_FILE OUTPUT_DIRECTORY
```

**Examples:**
```bash
# Convert SQLite back to TopSpeed files
pytopspeed reverse input.sqlite output_directory/

# With verbose output
pytopspeed -v reverse input.sqlite output_directory/
```

See [Reverse Conversion](#reverse-conversion) below for what is written back and the limits.

### List Command

List contents of .phz files:

```bash
pytopspeed list PHZ_FILE
```

**Examples:**
```bash
# List PHZ file contents
pytopspeed list MyProject.phz
```

## 📊 Conversion Features

### Table Name Prefixing

When converting multiple files, tables are automatically prefixed to avoid conflicts:

- **.phd files** → `phd_` prefix (e.g., `phd_OWNER`, `phd_CLASS`)
- **.mod files** → `mod_` prefix (e.g., `mod_DEPRECIATION`, `mod_MODID`)
- **.tps files** → `tps_` prefix
- **Other files** → `file_N_` prefix
- A second file of the same type gets the file's position in the list added (e.g. `phd2_`)

### Data Type Conversion

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
| Arrays | TEXT | JSON array |

Columns follow the order of the table definition. SQLite indexes are created for TopSpeed keys whose fields map to plain (non-array) columns.

### Array Fields

Array-like fields are stored as JSON arrays in SQLite TEXT columns:

- **`DIM(n)` fields**: one JSON array column per field
- **Dimensioned `GROUP`s**: each member becomes its own JSON array with one element per group entry; the `GROUP` itself gets no column (e.g. `FORCAST` `NUMFORM` → `VALUETYPE`, `PCODE`, `PRODVALTYPE`, `ACTION`, `VALUEREAL`, `DATEVAL`)
- **Numbered fields**: scalars `NAME1`..`NAMEn` of the same type become one array column `NAME` (e.g. `CUMVOL` `PROD1`..`PROD5` → `PROD`); fields like `VALUE` and `VALUE2` stay separate
- **Values**: `BYTE` elements are integers (`0`/`1` for flags, not `true`/`false`); NaN becomes `null`; zero stays zero

Tables whose definition can't be parsed fall back to an older heuristic conversion path. Those tables can't be edited and written back (see below).

### Column Name Sanitization

Column and table names are sanitized for SQLite compatibility:

- **Prefix removal**: `TIT:PROJ_DESCR` → `PROJ_DESCR`
- **Special characters**: spaces, `-`, `.`, `/`, `\` → `_`
- **Numeric prefixes**: `123FIELD` → `_123FIELD`
- **Reserved words** (table names): `ORDER` → `ORDER_TABLE`

### Reverse Conversion

- Forward conversion stores what SQLite can't hold (table definitions, file-level records, the original bytes of each row) in hidden `_topspeed_*` tables. Reverse conversion only works on databases that have them; otherwise it fails with "no TopSpeed metadata".
- Unchanged rows are written back byte for byte. Edited rows keep their record number and only the changed columns are re-encoded. Inserted rows get new record numbers; deleted rows and their memos are dropped. Index entries and record counts are rebuilt.
- A duplicate value in a unique key is an error naming the table and key, and no file is written. A value that doesn't fit its field (too many array elements, `DECIMAL` overflow) is an error naming the column.
- Output files keep their original names. A database made from a `.phz` produces the `.phd` and `.mod` files, not a new `.phz`.
- Rows are matched to their source records by SQLite `rowid`. If rowids change (`VACUUM` after deletes, recreating a table), unchanged rows are still matched by content, but edited rows become new records.
- Tables converted by the fallback path are written back unchanged, and edits to them are ignored with a warning. Index keys on field types other than `BYTE`, `SHORT`, `LONG`, `DOUBLE` and `STRING` produce a warning when their table was edited.
- Output has been opened in PHDWin 2.11 and read with the SoftVelocity TopSpeed ODBC driver. Keep a copy of the original file.

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
# results: success, files_created, tables_processed, records_processed, duration, errors, warnings
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

The SQLite database is larger than the source file because it also stores the original bytes of every record for reverse conversion (in testing, a ~7 MB `.PHD` became a ~37 MB database).

### Optimization Tips

1. **Batch size**: Adjust `--batch-size` for your system (default: 1000)
2. **Memory usage**: Larger batch sizes use more memory but mean fewer insert calls
3. **Storage**: Write the output database to a local disk rather than a network share

## 🐛 Troubleshooting

### Common Issues

**1. "stream read less than specified amount" errors**
- These are parsing warnings for corrupted or incomplete table definitions
- The conversion continues; such tables go through the fallback path or are skipped
- Check the log output for details

**2. "charmap codec can't decode" errors**
- Character encoding issues in text data
- Text is decoded as cp1251; undecodable bytes are replaced
- Check the log for the affected table

**3. Table name collisions**
- Multiple files have tables with the same name
- Automatic prefixing resolves conflicts

**4. "File not found" errors**
- Verify file paths are correct
- Check file permissions
- Ensure files are not locked by other applications

**5. Reverse conversion fails with "no TopSpeed metadata"**
- The database wasn't created by this version of the converter, or the `_topspeed_*` tables were removed
- Convert the original TopSpeed file again

**6. PHDWin reports "Version file is UNAVAILABLE ... File System Error" for every file**
- After PHDWin fails to open one database it keeps showing this error for later opens
- Restart PHDWin

### Known Quirks

- `-q` does not silence log output, and log lines can be printed twice.
- On Windows, the PHZ converter can't delete its temporary extraction folder (the extracted files are still open) and logs a warning.

### Debug Mode

Enable verbose logging for detailed information:

```bash
pytopspeed -v convert input.phd output.sqlite
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
- `multidimensional_arrays.py` - Array fields

## 🤝 Contributing

See [Developer Documentation](DEVELOPER.md) for contribution guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## 🙏 Acknowledgments

- Based on the original [pytopspeed library](https://github.com/dylangiles/pytopspeed/)
- Modernized for Python 3.8+ and construct 2.10+
- Enhanced with SQLite conversion and reverse conversion capabilities
