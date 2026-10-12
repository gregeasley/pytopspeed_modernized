# API Documentation

This document provides comprehensive API documentation for the Pytopspeed Modernized library.

## 📚 Table of Contents

- [Core Classes](#core-classes)
- [Resilience Features](#resilience-features)
- [SQLite Converter](#sqlite-converter)
- [Multidimensional Handler](#multidimensional-handler)
- [PHZ Converter](#phz-converter)
- [Reverse Converter](#reverseconverter)
- [Record Codec](#record-codec)
- [TopSpeed Metadata Tables](#topspeed-metadata-tables)
- [TPS Writer](#tps-writer)
- [Schema Mapper](#schema-mapper)
- [TopSpeed Parser](#topspeed-parser)
- [Utility Functions](#utility-functions)

## 🛡️ Resilience Features

### ResilienceEnhancer

Core resilience engine providing memory management, adaptive batch sizing, and error recovery for large database conversions.

```python
from converter.resilience_enhancements import ResilienceEnhancer
```

#### Constructor

```python
ResilienceEnhancer(max_memory_mb: int = 500, enable_progress_tracking: bool = True)
```

**Parameters:**
- `max_memory_mb` (int): Maximum memory usage in MB before cleanup (default: 500)
- `enable_progress_tracking` (bool): Enable detailed progress tracking (default: True)

#### Methods

##### `get_adaptive_batch_size(table_def) -> int`

Calculate optimal batch size based on table characteristics.

**Parameters:**
- `table_def`: Table definition object with `record_size` and `field_count` attributes

**Returns:**
- `int`: Optimal batch size (5-400 records based on table complexity)

**Example:**
```python
enhancer = ResilienceEnhancer()
batch_size = enhancer.get_adaptive_batch_size(table_def)
```

##### `check_memory_usage() -> bool`

Check if current memory usage exceeds the configured limit.

**Returns:**
- `bool`: True if memory limit exceeded, False otherwise

##### `force_memory_cleanup()`

Force garbage collection to free memory.

##### `extract_raw_data_safe(record) -> Optional[bytes]`

Safely extract raw data from a TPS record with multiple fallback methods.

**Parameters:**
- `record`: TPS record object

**Returns:**
- `Optional[bytes]`: Raw data bytes or None if extraction fails

##### `create_compact_json(raw_data: bytes, table_name: str) -> str`

Create a compact JSON representation of raw binary data.

**Parameters:**
- `raw_data`: Raw binary data
- `table_name`: Name of the table for context

**Returns:**
- `str`: JSON string representation with base64-encoded data

##### `estimate_table_size(tps, table_name: str) -> Dict[str, Any]`

Estimate table size and characteristics for optimization.

**Parameters:**
- `tps`: TopSpeed file object
- `table_name`: Name of the table to estimate

**Returns:**
- `Dict[str, Any]`: Dictionary with size estimates and recommendations

##### `should_use_streaming(table_def, estimated_size: Dict[str, Any]) -> bool`

Decide whether a table should be processed in streaming mode, given the result of `estimate_table_size()`.

##### `log_progress(current: int, total: int, table_name: str, operation: str = 'processing')`

Log progress for a table operation.

### ResilienceConfig

Configuration management for resilience features with predefined settings for different database sizes.

```python
from converter.resilience_config import ResilienceConfig, get_resilience_config
```

#### Predefined Configurations

```python
# get_resilience_config(config_name: str = 'default') -> ResilienceConfig
config = get_resilience_config('small')       # ResilienceConfig.for_small_databases()
config = get_resilience_config('medium')      # ResilienceConfig.for_medium_databases()
config = get_resilience_config('large')       # ResilienceConfig.for_large_databases()
config = get_resilience_config('enterprise')  # ResilienceConfig.for_enterprise_databases()
config = get_resilience_config()              # 'default': ResilienceConfig()
```

An unknown name raises `ValueError`.

#### Configuration Attributes

`ResilienceConfig` is a dataclass; the most commonly used attributes are listed below (see
`src/converter/resilience_config.py` for the full list).

| Attribute | Type | Description | Default |
|-----------|------|-------------|---------|
| `max_memory_mb` | int | Maximum memory usage in MB | 500 |
| `default_batch_size` | int | Default batch size for processing | 100 |
| `enable_streaming` | bool | Enable streaming for large tables | True |
| `enable_parallel_processing` | bool | Enable parallel processing | False |
| `enable_checkpointing` | bool | Enable checkpointing for resumability | False |
| `sqlite_journal_mode` | str | SQLite journal mode | "WAL" |
| `sqlite_cache_size` | int | SQLite cache size in KB | -2000 |

#### Methods

##### `to_dict() -> Dict[str, Any]`

Convert configuration to dictionary.

##### `from_dict(config_dict: Dict[str, Any]) -> ResilienceConfig` (classmethod)

Create configuration from dictionary.

##### `for_small_databases() -> ResilienceConfig` (classmethod)

`max_memory_mb=200`, `default_batch_size=200`.

##### `for_medium_databases() -> ResilienceConfig` (classmethod)

`max_memory_mb=500`, `default_batch_size=100`.

##### `for_large_databases() -> ResilienceConfig` (classmethod)

`max_memory_mb=1000`, `default_batch_size=50`, parallel processing and checkpointing enabled.

##### `for_enterprise_databases() -> ResilienceConfig` (classmethod)

`max_memory_mb=2000`, `default_batch_size=25`, parallel processing and checkpointing enabled.

### Database Size Category Estimation

```python
from converter.resilience_config import estimate_database_size_category

category = estimate_database_size_category(estimated_size_mb, estimated_records)
```

**Parameters:**
- `estimated_size_mb` (float): Estimated database size in MB
- `estimated_records` (int): Estimated total number of records

**Returns:**
- `str`: Configuration category: `'enterprise'` if > 10,000 MB or > 1,000,000 records,
  `'large'` if > 1,000 MB or > 100,000 records, `'medium'` if > 100 MB or > 10,000 records,
  otherwise `'small'`

## 🔧 Core Classes

### File Type Usage Guide

| File Type | Extension | Converter Class | Method | Description |
|-----------|-----------|-----------------|--------|-------------|
| **Single TopSpeed** | `.phd`, `.mod`, `.tps` | `SqliteConverter` | `convert()` | Convert individual TopSpeed files |
| **Multiple TopSpeed** | `.phd`, `.mod`, `.tps` | `SqliteConverter` | `convert_multiple()` | Combine multiple files into one database |
| **PHZ Archive** | `.phz` | `PhzConverter` | `convert_phz()` | Convert zip archives containing TopSpeed files |
| **Reverse Conversion** | `.sqlite` | `ReverseConverter` | `convert_sqlite_to_topspeed()` | Convert SQLite back to TopSpeed files |

### SqliteConverter

Main class for converting TopSpeed files to SQLite databases.

**Use for**: Single `.phd`, `.mod`, `.tps` files or combining multiple files

```python
from converter.sqlite_converter import SqliteConverter
```

#### Constructor

```python
SqliteConverter(batch_size: int = 1000, progress_callback=None)
```

**Parameters:**
- `batch_size` (int): Number of records to process in each batch (default: 1000)
- `progress_callback` (Callable): Optional `callback(current, total, message)` for progress updates

**Example:**
```python
def progress_callback(current, total, message=""):
    print(f"Progress: {current}/{total} - {message}")

converter = SqliteConverter(batch_size=500, progress_callback=progress_callback)
```

#### Methods

##### convert()

Convert a single TopSpeed file to SQLite.

```python
convert(phd_file: str, output_file: str) -> Dict[str, Any]
```

**Parameters:**
- `phd_file` (str): Path to input TopSpeed file (.phd, .mod, .tps)
- `output_file` (str): Path to output SQLite database

**Returns:**
- `Dict[str, Any]`: `success`, `tables_created`, `total_records`, `errors`, `duration`

Tables whose definition parses are converted with `RecordCodec` (see [Record Codec](#record-codec));
definitions that fail to parse fall back to the `MultidimensionalHandler` path. The database also
gets the hidden `_topspeed_*` tables that reverse conversion needs (see
[TopSpeed Metadata Tables](#topspeed-metadata-tables)).

**Example:**
```python
results = converter.convert('input.phd', 'output.sqlite')
if results['success']:
    print(f"Converted {results['total_records']} records")
else:
    print(f"Conversion failed: {results['errors']}")
```

##### convert_multiple()

Convert multiple TopSpeed files to a single SQLite database.

```python
convert_multiple(input_files: List[str], output_file: str) -> Dict[str, Any]
```

**Parameters:**
- `input_files` (List[str]): List of input TopSpeed files
- `output_file` (str): Path to output SQLite database

**Returns:**
- `Dict[str, Any]`: `success`, `tables_created`, `total_records`, `duration`, `errors`,
  `files_processed`, `file_results` (per-file results keyed by input path)

Table names are prefixed by file type to avoid collisions: `phd_`, `mod_`, `tps_` (other extensions
get `file_N_`; a second file of the same type gets its position appended, e.g. `phd2_`).

**Example:**
```python
files = ['file1.phd', 'file2.mod']
results = converter.convert_multiple(files, 'combined.sqlite')
```

### MultidimensionalHandler

Array detection and JSON conversion for TopSpeed array fields. `SqliteConverter` now converts tables
with `RecordCodec` and uses this handler only as a fallback for table definitions that fail to parse;
`RecordCodec` reuses its numbered-field grouping rule (`NAME1..NAMEn` -> one array `NAME`).

```python
from converter.multidimensional_handler import MultidimensionalHandler
```

#### Constructor

```python
MultidimensionalHandler()
```

#### Key Methods

##### analyze_table_structure()

Analyzes a TopSpeed table definition to detect multidimensional arrays.

```python
analysis = handler.analyze_table_structure(table_def)
```

**Parameters:**
- `table_def`: TopSpeed table definition object

**Returns:**
- `Dict[str, Any]`: Analysis results containing:
  - `has_arrays` (bool): Whether the table contains arrays
  - `array_fields` (List[ArrayFieldInfo]): Detected array fields
  - `regular_fields` (List): Non-array fields
  - `total_record_size` (int): Calculated record size

##### create_sqlite_schema()

Creates SQLite schema for tables with multidimensional fields.

```python
sql = handler.create_sqlite_schema("TABLE_NAME", analysis, table_def=None)
```

**Parameters:**
- `table_name` (str): Name of the SQLite table
- `analysis` (Dict): Analysis results from `analyze_table_structure()`
- `table_def` (optional): TopSpeed table definition

**Returns:**
- `str`: CREATE TABLE SQL statement

##### parse_record_data() / prepare_record_for_sqlite()

```python
parse_record_data(data, analysis: Dict[str, Any]) -> Dict[str, Any]
prepare_record_for_sqlite(parsed_data: Dict[str, Any]) -> Tuple[List[str], List[Any]]
```

Parse one record's bytes into a field dict, then turn it into (column names, values) for an INSERT.

#### ArrayFieldInfo

Data class representing information about a detected array field.

```python
@dataclass
class ArrayFieldInfo:
    base_name: str              # Base field name (e.g., "PROD1")
    element_type: str           # Data type of array elements
    element_size: int           # Size of each element in bytes
    array_size: int             # Number of elements in array
    start_offset: int           # Starting offset in record
    element_offsets: List[int]  # Offset of each element
    is_single_field_array: bool = False  # Whether this is a single large field
    double_79_extra1: int = None
    double_79_extra2: int = None
    element_names: List[str] = None
```

#### Array Detection Types

1. **Single-Field Arrays**: Large fields containing multiple elements
   - Example: 96-byte `DAT:PROD1` field with 12 DOUBLE elements
   - Detected using `array_element_count` attribute from TopSpeed schema

2. **Multi-Field Arrays**: Multiple small fields forming an array
   - Example: `CUM:PROD1`, `CUM:PROD2`, `CUM:PROD3` fields
   - Detected by analyzing field name patterns and offsets

#### Data Type Handling

Array values are JSON arrays of the element values (numbers, strings, `YYYY-MM-DD` dates).
For tables converted with `RecordCodec` (the normal path), BYTE values, including BYTE arrays,
are integers. The handler's own parser (fallback path only) still returns BYTE elements as
booleans.

#### Enhanced Parsing Features

- **Comprehensive Type Mapping**: Supports all TopSpeed field types including type 79 (DOUBLE_79)
- **Generic Binary Data Detection**: Automatically detects and parses numeric binary data in STRING fields
- **Dynamic Field Offset Calculation**: Correctly calculates field offsets for complex table structures
- **Large Table Support**: Handles tables with 100+ fields without artificial limits
- **Robust Error Recovery**: Continues parsing even when individual fields fail

#### Enhanced Parsing Capabilities

The multidimensional handler now includes advanced parsing features:

##### Comprehensive Type Support

```python
# All TopSpeed field types are now supported:
# - Type 79 (DOUBLE_79): Special DOUBLE fields with extended metadata
# - Type 83/82 (SHORT): 16-bit signed integers
# - Type 0 (DOUBLE): Standard 8-byte floating point
# - Type 84/67/14 (STRING): Variable length strings
# - Type 248 (DECIMAL): Decimal numbers
# - Type 65 (BYTE): 8-bit unsigned integers
# - Type 44 (DATE): Date fields
# - Type 69 (LONG): 32-bit signed integers
```

##### Generic Binary Data Detection

```python
# Automatically detects when STRING fields contain binary data
# and parses them as appropriate numeric types:
# - 4-byte fields → 32-bit unsigned integers
# - 2-byte fields → 16-bit unsigned integers  
# - 1-byte fields → 8-bit unsigned integers
```

##### Large Table Support

```python
# No artificial limits on table size:
# - Tables with 100+ fields are fully supported
# - Dynamic field offset calculation
# - Robust error recovery for individual field failures
```

#### Example Usage

```python
from converter.multidimensional_handler import MultidimensionalHandler

# Create handler
handler = MultidimensionalHandler()

# Analyze table structure
analysis = handler.analyze_table_structure(table_def)

if analysis['has_arrays']:
    print(f"Found {len(analysis['array_fields'])} array fields")
    
    # Create SQLite schema
    sql = handler.create_sqlite_schema("MONHIST", analysis)
    print(sql)
    
    # Process record data
    parsed_data = handler.parse_record_data(raw_data, analysis)
    print(parsed_data)
```

### PhzConverter

Class for handling .phz files (zip archives containing TopSpeed files).

**Use for**: `.phz` files (zip archives containing TopSpeed files)

```python
from converter.phz_converter import PhzConverter
```

#### Constructor

```python
PhzConverter(batch_size: int = 1000, progress_callback=None)
```

**Parameters:**
- `batch_size` (int): Number of records to process in each batch
- `progress_callback` (Callable): Optional callback function for progress updates

#### Methods

##### convert_phz()

Convert a .phz file to SQLite database.

```python
convert_phz(phz_file: str, output_file: str) -> Dict[str, Any]
```

**Parameters:**
- `phz_file` (str): Path to input .phz file
- `output_file` (str): Path to output SQLite database

**Returns:**
- `Dict[str, Any]`: `success`, `tables_created`, `total_records`, `duration`, `errors`,
  `extracted_files` (names of the .phd/.mod files converted), `phz_contents`, plus the
  `convert_multiple()` keys (`files_processed`, `file_results`)

The .phd and .mod files in the archive are converted with `SqliteConverter.convert_multiple()`
into one database (tables prefixed `phd_` / `mod_`). On Windows the temporary extraction folder
can't always be deleted because the input files are still open; a warning is logged.

**Example:**
```python
converter = PhzConverter()
results = converter.convert_phz('input.phz', 'output.sqlite')
```

##### list_phz_contents()

List contents of a .phz file without extracting.

```python
list_phz_contents(phz_file: str) -> Dict[str, Any]
```

**Parameters:**
- `phz_file` (str): Path to input .phz file

**Returns:**
- `Dict[str, Any]`: `success`, `phz_contents`, `phd_files`, `mod_files`, `other_files`, `errors`

**Example:**
```python
contents = converter.list_phz_contents('input.phz')
print(f"PHD files: {contents['phd_files']}")
print(f"MOD files: {contents['mod_files']}")
```

### ReverseConverter

Class for converting SQLite databases back to TopSpeed files.

```python
from converter.reverse_converter import ReverseConverter
```

#### Constructor

```python
ReverseConverter(progress_callback=None)
```

**Parameters:**
- `progress_callback` (Callable): Optional `callback(current, total, message)`, called once per table

#### Methods

##### convert_sqlite_to_topspeed()

Convert SQLite database back to TopSpeed files.

```python
convert_sqlite_to_topspeed(sqlite_file: str, output_dir: str) -> Dict[str, Any]
```

**Parameters:**
- `sqlite_file` (str): Path to a SQLite database created by `SqliteConverter` or `PhzConverter`
  (it must contain the `_topspeed_*` metadata tables)
- `output_dir` (str): Directory for output TopSpeed files; each file keeps its original name

**Returns:**
- `Dict[str, Any]`: `success` (at least one file written and no errors), `files_created`
  (list of output paths), `tables_processed`, `records_processed`, `duration`, `errors`, `warnings`

The database must have been created by this version (it needs the `_topspeed_*` tables);
otherwise the result contains a "no TopSpeed metadata" error. One file is written per
converted source file, under its original name (a .phz-derived database yields the .phd and
.mod files; they are not re-zipped).

- Rows are matched to their original records by SQLite rowid. Unchanged rows are written back
  byte for byte; edited rows keep their record number and only the changed columns are re-encoded;
  inserted rows get new record numbers; deleted rows and their memos are dropped.
- If rowids changed (e.g. `VACUUM` after deletes, or a recreated table), unchanged rows are still
  matched by content; edited rows then become new records.
- Index entries and record-count metadata are regenerated from the resulting rows.
- No file is written for a source file whose data can't be stored: a duplicate value in a unique
  key (error names the table and key) or a value that doesn't fit its field, such as too many
  array elements or a DECIMAL overflow (error names the column).
- Tables converted on the fallback path (no codec) are written back unchanged; edits to them are
  ignored with a warning. Index keys are verified for BYTE, SHORT, LONG, DOUBLE and STRING fields;
  other key types produce a warning when their table was edited.

**Example:**
```python
converter = ReverseConverter()
results = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_dir/')
```

## Record Codec

### RecordCodec

Maps one TopSpeed table definition to SQLite columns and converts DATA record payloads to
column values and back. Forward conversion, schema creation and reverse conversion all use it.

```python
from converter.record_codec import RecordCodec

RecordCodec(table_def, encoding: str = 'cp1251', sanitize=None)
```

**Parameters:**
- `table_def`: Parsed table definition (`tps.tables.get_definition(number)`)
- `encoding` (str): String encoding (default: `'cp1251'`)
- `sanitize` (Callable): Field-name sanitizer (default: the `MultidimensionalHandler` one, which strips the `PRE:` prefix)

**Column layout** (`codec.columns`, a list of `Column` dataclasses with `name`, `sqlite_type`,
`field_type`, `element_size`, `offsets`, `is_array`, `decimal_count`, `source_fields`,
`memo_index` and the `is_memo` property), in definition order:
- scalar field -> one column named after the field without its `PRE:` prefix
- `DIM(n)` field -> one JSON array column (declared `TEXT`)
- members of a dimensioned GROUP -> one JSON array column per member; the GROUP itself has no column
- numbered scalars `NAME1..NAMEn` (suffixes exactly 1..n, same type, evenly spaced) -> one JSON
  array column `NAME`; fields such as `VALUE` and `VALUE2` stay separate
- each memo -> one column (declared `BLOB`, holds the memo text)

`data_columns` and `memo_columns` are the non-memo and memo subsets.

#### Methods

```python
decode(payload: bytes, memos: Optional[Dict[int, bytes]] = None) -> tuple
encode(values: Sequence[Any], template: Optional[bytes] = None,
       columns: Optional[Sequence[str]] = None) -> bytes
encode_memo(value: Any) -> Optional[bytes]
decode_memo(raw: Optional[bytes]) -> Optional[str]
index_columns(index_def) -> Optional[List[str]]
```

- `decode()`: payload bytes (plus raw memo bytes keyed by memo index) -> values in column order.
  Arrays are returned as JSON strings; NaN/inf become `None` (`null` inside arrays).
- `encode()`: values in column order -> payload bytes. Bytes not covered by an encoded column are
  kept from `template` (zeros if omitted); `columns` limits encoding to the named columns. Memo
  columns are ignored. Raises `ValueError` naming the column when a value doesn't fit (e.g. more
  array elements than the field has).
- `encode_memo()`: memo value -> raw memo bytes; `None` means no memo, an empty string is stored
  as a single zero byte.
- `index_columns()`: column names for a TopSpeed index, or `None` if a key field isn't a
  standalone (non-array) column.

**Example:**
```python
from pytopspeed.tps import TPS
from converter.record_codec import RecordCodec

tps = TPS('input.phd', encoding='cp1251')
tps.set_current_table('WELLS')
table_def = tps.tables.get_definition(tps.current_table_number)
codec = RecordCodec(table_def)
print([(c.name, c.sqlite_type, c.is_array) for c in codec.columns])

values = codec.decode(payload)                        # payload: DATA record bytes
assert codec.encode(values, template=payload) == payload
```

## TopSpeed Metadata Tables

Forward conversion stores what SQLite has no place for in tables prefixed `_topspeed_`
(`converter.topspeed_metadata`; `is_metadata_table(name)` tests for the prefix). Reverse
conversion needs them, so don't drop them if you want to write the data back. They make the
database noticeably larger than the table data alone.

| Table | Contents |
|-------|----------|
| `_topspeed_file` | One row per converted file: `file_prefix`, `source_file`, `last_issued_row`, `change_count`, `encoding`, `format_version` |
| `_topspeed_table` | `file_prefix`, `table_number`, `table_name`, `sqlite_table`, `uses_codec` |
| `_topspeed_record` | Records copied back verbatim (`file_prefix`, `header_size`, `record`): table names, definitions, file-level records, and all records of fallback-path tables |
| `_topspeed_row` | Original `record_number` and `payload` for each SQLite row (`file_prefix`, `table_number`, `row_id`) |
| `_topspeed_memo` | Original memo bytes (`file_prefix`, `table_number`, `record_number`, `memo_index`, `data`) |

## TPS Writer

`pytopspeed.tpswriter` builds TopSpeed files from raw records. A record is a
`(header_size, record_bytes)` pair; `record_bytes[:header_size]` is its key.

```python
from pytopspeed.tpswriter import (build_tps, read_raw_records, data_record, memo_records,
                                  metadata_record, index_records)

build_tps(records, last_issued_row: int = 0, change_count: int = 1) -> bytes
read_raw_records(tps) -> List[Tuple[int, bytes]]
data_record(table_number: int, record_number: int, payload: bytes) -> Tuple[int, bytes]
memo_records(table_number: int, record_number: int, memo_index: int,
             chunks: Sequence[bytes]) -> List[Tuple[int, bytes]]
metadata_record(table_number: int, metadata_type: int, count: int,
                last_access: int = 0) -> Tuple[int, bytes]
index_records(table_number: int, definition, payload: bytes, record_number: int,
              encoding: str = 'cp1251') -> List[Tuple[int, bytes]]
```

- `build_tps()` returns the complete file contents: it sorts the records by key (input order
  doesn't matter), packs them into prefix- and RLE-compressed pages, builds the page tree and
  writes the header.
- `read_raw_records()` returns every record of an open `TPS` file in the same form.
- `data_record()`, `memo_records()` (one record per chunk; `converter.record_codec.memo_chunks()`
  splits memo bytes into 256-byte chunks), `metadata_record()` (`metadata_type` 0xF3 for the row
  count, or an index number for that index's entry count) and `index_records()` (one entry per
  index of the definition) build individual records.

**Example (copy a file record for record):**
```python
from pytopspeed.tps import TPS
from pytopspeed.tpswriter import build_tps, read_raw_records

tps = TPS('input.phd', encoding='cp1251')
with open('copy.phd', 'wb') as f:
    f.write(build_tps(read_raw_records(tps)))
```

## 🗺️ Schema Mapper

### TopSpeedToSQLiteMapper

Class for mapping TopSpeed table definitions to SQLite schema.

```python
from converter.schema_mapper import TopSpeedToSQLiteMapper
```

#### Constructor

```python
TopSpeedToSQLiteMapper()
```

#### Methods

##### sanitize_field_name()

Sanitize TopSpeed field names for SQLite compatibility.

```python
sanitize_field_name(field_name: str) -> str
```

**Parameters:**
- `field_name` (str): Original TopSpeed field name

**Returns:**
- `str`: Sanitized field name

**Example:**
```python
mapper = TopSpeedToSQLiteMapper()
clean_name = mapper.sanitize_field_name("TIT:PROJ_DESCR")  # Returns "PROJ_DESCR"
```

##### sanitize_table_name()

Sanitize TopSpeed table names for SQLite compatibility.

```python
sanitize_table_name(table_name: str) -> str
```

**Parameters:**
- `table_name` (str): Original TopSpeed table name

**Returns:**
- `str`: Sanitized table name

**Example:**
```python
clean_name = mapper.sanitize_table_name("ORDER")  # Returns "ORDER_TABLE"
```

##### generate_create_table_sql()

Generate SQLite CREATE TABLE statement.

```python
generate_create_table_sql(table_name: str, table_def: Any) -> str
```

**Parameters:**
- `table_name` (str): Sanitized table name
- `table_def` (Any): TopSpeed table definition object

**Returns:**
- `str`: SQLite CREATE TABLE statement

##### generate_create_index_sql()

Generate SQLite CREATE INDEX statement.

```python
generate_create_index_sql(table_name: str, index: Any, table_def: Any) -> str
```

**Parameters:**
- `table_name` (str): Sanitized table name
- `index` (Any): TopSpeed index definition object
- `table_def` (Any): TopSpeed table definition object

**Returns:**
- `str`: SQLite CREATE INDEX statement

## 🔍 TopSpeed Parser

### TPS

Main class for reading TopSpeed files.

```python
from pytopspeed.tps import TPS
```

#### Constructor

```python
TPS(filename, encoding=None, password=None, cached=True, check=False, current_tablename=None,
    date_fieldname=None, time_fieldname=None, decryptor_class=TpsDecryptor)
```

**Parameters:**
- `filename` (str): Path to TopSpeed file
- `encoding` (str): Encoding for STRING fields; `None` means `'cp1251'`, the encoding Clarion
  TopSpeed files use
- `password` (str): Password for encrypted files
- `cached`, `check`: Page caching and structure checks

**Example:**
```python
tps = TPS('input.phd', encoding='cp1251')
```

#### Properties

- `header`: File header information
- `pages`: List of pages in the file
- `tables`: Table list; `tables.get_number(name)` and `tables.get_definition(number)`
- `current_table_number`: Currently selected table number

#### Methods

##### set_current_table()

Set the current table for iteration.

```python
set_current_table(tablename) -> None
```

**Parameters:**
- `tablename` (str): Name of the table to select

##### __iter__()

Iterate through records in the current table.

```python
for record in tps:
    # Process record
    pass
```

**Returns:**
- Iterator of dicts, one per DATA record (see [Record Objects](#record-objects))

**Example:**
```python
tps.set_current_table('OWNER')
for record in tps:
    print(record)
```

## 📊 Data Types

### Type Mappings

Column types used by `RecordCodec` (the normal conversion path):

| TopSpeed Type | SQLite Type | Python Type | Notes |
|---------------|-------------|-------------|-------|
| BYTE, SHORT, USHORT, LONG, ULONG | INTEGER | int | BYTE values are integers, not booleans |
| FLOAT, DOUBLE, DECIMAL | REAL | float | NaN/inf stored as NULL |
| STRING, CSTRING, PSTRING | TEXT | str | Right-trimmed; blank -> `''` |
| DATE | TEXT | str | `YYYY-MM-DD`; NULL if zero |
| TIME | TEXT | str | `HH:MM:SS.cc` |
| Array (DIM, GROUP member, NAME1..n) | TEXT | str | JSON array; NaN/inf -> `null` |
| MEMO | BLOB (declared) | str | Full memo text |

### Record Objects

Iterating a `TPS` object yields plain dicts keyed by the full TopSpeed field name (including the
`PRE:` prefix), plus the record number under the literal key `"b':RecNo'"`:

```python
record = {
    "b':RecNo'": 1,          # Record number
    'WEL:ID': 1,             # Field values
    'WEL:NAME': 'Alpha 1',
    'WEL:NOTES': ...,        # Memo fields
}
```

This low-level reader returns only the first element of a DIM field; use `RecordCodec` for full
array and GROUP handling.

## 🔧 Utility Functions

### Progress Callback

Standard progress callback function signature:

```python
def progress_callback(current: int, total: int, message: str = "") -> None:
    """
    Progress callback function.
    
    Args:
        current: Current progress value
        total: Total progress value
        message: Optional progress message
    """
    if total > 0:
        percentage = (current / total) * 100
        print(f"[{percentage:6.1f}%] {message}")
```

### Error Handling

Converter methods catch their own exceptions and report them in the result dictionary
instead of raising. Every result has `success`, `duration` and `errors`; the other keys
depend on the method:

```python
{
    'success': bool,           # Whether the operation succeeded
    'duration': float,         # Operation duration in seconds
    'errors': List[str],       # List of error messages
    'tables_created': int,     # convert / convert_multiple / convert_phz
    'total_records': int,      # convert / convert_multiple / convert_phz
    'files_processed': int,    # convert_multiple / convert_phz
    'file_results': Dict,      # convert_multiple / convert_phz
    'extracted_files': List[str],  # convert_phz
    'files_created': List[str],    # convert_sqlite_to_topspeed (output paths)
    'tables_processed': int,       # convert_sqlite_to_topspeed
    'records_processed': int,      # convert_sqlite_to_topspeed
    'warnings': List[str],         # convert_sqlite_to_topspeed
}
```

## 📝 Examples

### Basic Single File Conversion

```python
from converter.sqlite_converter import SqliteConverter

# Create converter
converter = SqliteConverter(batch_size=1000)

# Convert single file
results = converter.convert('input.phd', 'output.sqlite')

# Check results
if results['success']:
    print(f"Success! Converted {results['total_records']} records")
    print(f"Created {results['tables_created']} tables")
    print(f"Duration: {results['duration']:.2f} seconds")
else:
    print(f"Conversion failed: {results['errors']}")
```

### Multiple File Conversion with Progress

```python
from converter.sqlite_converter import SqliteConverter

def progress_callback(current, total, message=""):
    if total > 0:
        percentage = (current / total) * 100
        print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)

# Create converter with progress tracking
converter = SqliteConverter(
    batch_size=500,
    progress_callback=progress_callback
)

# Convert multiple files
files = ['file1.phd', 'file2.mod', 'file3.tps']
results = converter.convert_multiple(files, 'combined.sqlite')

print()  # New line after progress
if results['success']:
    print(f"Converted {results['total_records']} records from {results['files_processed']} files")
```

### PHZ File Processing

```python
from converter.phz_converter import PhzConverter

# Create PHZ converter
converter = PhzConverter()

# List contents first
contents = converter.list_phz_contents('input.phz')
print(f"PHZ contains: {len(contents['phd_files'])} PHD files, {len(contents['mod_files'])} MOD files")

# Convert PHZ file
results = converter.convert_phz('input.phz', 'output.sqlite')
if results['success']:
    print(f"Extracted and converted {len(results['extracted_files'])} files")
```

### Reverse Conversion

```python
from converter.reverse_converter import ReverseConverter

# Create reverse converter
converter = ReverseConverter()

# Convert SQLite back to TopSpeed
results = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_dir/')
if results['success']:
    print(f"Created {len(results['files_created'])} TopSpeed files")
    print(f"Processed {results['records_processed']} records")
for warning in results['warnings']:
    print(f"Warning: {warning}")
for error in results['errors']:
    print(f"Error: {error}")
```

### Custom Schema Mapping

```python
from converter.schema_mapper import TopSpeedToSQLiteMapper

# Create mapper
mapper = TopSpeedToSQLiteMapper()

# Sanitize names
clean_field = mapper.sanitize_field_name("TIT:PROJ_DESCR")  # "PROJ_DESCR"
clean_table = mapper.sanitize_table_name("ORDER")           # "ORDER_TABLE"

# Generate SQL (table_def from tps.tables.get_definition(...))
sql = mapper.generate_create_table_sql(clean_table, table_def)
print(sql)
```

## 🚨 Error Handling Best Practices

### Always Check Results

```python
results = converter.convert('input.phd', 'output.sqlite')
if not results['success']:
    print("Conversion failed!")
    for error in results['errors']:
        print(f"  Error: {error}")
```

Missing input files and other failures are reported in `errors` rather than raised.

### Use Progress Callbacks

```python
def progress_callback(current, total, message=""):
    if total > 0:
        percentage = (current / total) * 100
        print(f"\r[{percentage:6.1f}%] {message}", end='', flush=True)
    else:
        print(f"\r{message}", end='', flush=True)

converter = SqliteConverter(progress_callback=progress_callback)
```

## 🔍 Debugging

### Enable Logging

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Or enable for specific modules
logging.getLogger('converter.sqlite_converter').setLevel(logging.DEBUG)
```

### Inspect Results

```python
results = converter.convert('input.phd', 'output.sqlite')

# Print all result details
for key, value in results.items():
    print(f"{key}: {value}")
```
