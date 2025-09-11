# API Documentation

This document provides comprehensive API documentation for the Pytopspeed Modernized library.

## 📚 Table of Contents

- [Core Classes](#core-classes)
- [Resilience Features](#resilience-features)
- [SQLite Converter](#sqlite-converter)
- [Multidimensional Handler](#multidimensional-handler)
- [PHZ Converter](#phz-converter)
- [Reverse Converter](#reverse-converter)
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

### ResilienceConfig

Configuration management for resilience features with predefined settings for different database sizes.

```python
from converter.resilience_config import ResilienceConfig, get_resilience_config
```

#### Predefined Configurations

```python
# Get predefined configuration
config = get_resilience_config('small')    # < 10MB databases
config = get_resilience_config('medium')   # 10MB - 1GB databases  
config = get_resilience_config('large')    # 1GB - 10GB databases
config = get_resilience_config('enterprise') # > 10GB databases
```

#### Configuration Attributes

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

##### `from_dict(config_dict: Dict[str, Any]) -> ResilienceConfig`

Create configuration from dictionary.

##### `for_small_databases() -> ResilienceConfig`

Get configuration optimized for small databases (< 10MB).

##### `for_medium_databases() -> ResilienceConfig`

Get configuration optimized for medium databases (10MB - 1GB).

##### `for_large_databases() -> ResilienceConfig`

Get configuration optimized for large databases (1GB - 10GB).

##### `for_enterprise_databases() -> ResilienceConfig`

Get configuration optimized for enterprise databases (> 10GB).

### Database Size Category Estimation

```python
from converter.resilience_config import estimate_database_size_category

category = estimate_database_size_category(estimated_size_mb, estimated_records)
```

**Parameters:**
- `estimated_size_mb` (float): Estimated database size in MB
- `estimated_records` (int): Estimated total number of records

**Returns:**
- `str`: Configuration category ('small', 'medium', 'large', 'enterprise')

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
SqliteConverter(batch_size: int = 1000, progress_callback: Optional[Callable] = None)
```

**Parameters:**
- `batch_size` (int): Number of records to process in each batch (default: 1000)
- `progress_callback` (Callable): Optional callback function for progress updates

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
convert(input_file: str, output_file: str) -> Dict[str, Any]
```

**Parameters:**
- `input_file` (str): Path to input TopSpeed file (.phd, .mod, .tps)
- `output_file` (str): Path to output SQLite database

**Returns:**
- `Dict[str, Any]`: Conversion results with success status, statistics, and errors

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
- `Dict[str, Any]`: Conversion results with success status, statistics, and errors

**Example:**
```python
files = ['file1.phd', 'file2.mod']
results = converter.convert_multiple(files, 'combined.sqlite')
```

### MultidimensionalHandler

Advanced class for handling TopSpeed multidimensional arrays and converting them to JSON format in SQLite.

**Use for**: Automatic detection and conversion of array fields in TopSpeed tables

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
sql = handler.create_sqlite_schema("TABLE_NAME", analysis)
```

**Parameters:**
- `table_name` (str): Name of the SQLite table
- `analysis` (Dict): Analysis results from `analyze_table_structure()`

**Returns:**
- `str`: CREATE TABLE SQL statement

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
    is_single_field_array: bool # Whether this is a single large field
```

#### Array Detection Types

1. **Single-Field Arrays**: Large fields containing multiple elements
   - Example: 96-byte `DAT:PROD1` field with 12 DOUBLE elements
   - Detected using `array_element_count` attribute from TopSpeed schema

2. **Multi-Field Arrays**: Multiple small fields forming an array
   - Example: `CUM:PROD1`, `CUM:PROD2`, `CUM:PROD3` fields
   - Detected by analyzing field name patterns and offsets

#### Data Type Handling

- **DOUBLE Arrays**: Preserved as JSON arrays of numbers
- **BYTE Arrays**: Converted to JSON arrays of booleans (`true`/`false`)
- **STRING Arrays**: Maintained as JSON arrays of strings
- **Null vs Zero**: Distinguishes between `null` (missing data) and `0.0` (actual zero)

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
PhzConverter(batch_size: int = 1000, progress_callback: Optional[Callable] = None)
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
- `Dict[str, Any]`: Conversion results with success status, statistics, and errors

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
- `Dict[str, Any]`: File contents information with categorized file lists

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
ReverseConverter(progress_callback: Optional[Callable] = None)
```

**Parameters:**
- `progress_callback` (Callable): Optional callback function for progress updates

#### Methods

##### convert_sqlite_to_topspeed()

Convert SQLite database back to TopSpeed files.

```python
convert_sqlite_to_topspeed(sqlite_file: str, output_dir: str) -> Dict[str, Any]
```

**Parameters:**
- `sqlite_file` (str): Path to input SQLite database
- `output_dir` (str): Directory for output TopSpeed files

**Returns:**
- `Dict[str, Any]`: Conversion results with success status, statistics, and errors

**Example:**
```python
converter = ReverseConverter()
results = converter.convert_sqlite_to_topspeed('input.sqlite', 'output_dir/')
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
TPS(filename: str)
```

**Parameters:**
- `filename` (str): Path to TopSpeed file

**Example:**
```python
tps = TPS('input.phd')
```

#### Properties

- `header`: File header information
- `pages`: List of pages in the file
- `tables`: Table definitions and metadata
- `current_table_number`: Currently selected table number

#### Methods

##### set_current_table()

Set the current table for iteration.

```python
set_current_table(table_name: str) -> None
```

**Parameters:**
- `table_name` (str): Name of the table to select

##### __iter__()

Iterate through records in the current table.

```python
for record in tps:
    # Process record
    pass
```

**Returns:**
- Iterator of record objects

**Example:**
```python
tps.set_current_table('OWNER')
for record in tps:
    print(record.data)
```

## 📊 Data Types

### Type Mappings

The library automatically maps TopSpeed data types to SQLite types:

| TopSpeed Type | SQLite Type | Python Type | Notes |
|---------------|-------------|-------------|-------|
| BYTE | INTEGER | int | 8-bit unsigned integer |
| SHORT | INTEGER | int | 16-bit signed integer |
| LONG | INTEGER | int | 32-bit signed integer |
| DATE | TEXT | str | Format: YYYY-MM-DD |
| TIME | TEXT | str | Format: HH:MM:SS |
| STRING | TEXT | str | Variable length text |
| DECIMAL | REAL | float | Floating point number |
| MEMO | BLOB | bytes | Binary large object |
| BLOB | BLOB | bytes | Binary large object |

### Record Objects

Records are returned as dictionary-like objects with the following structure:

```python
record = {
    'field_name': value,  # Field values
    'record_number': int,  # Record number
    # Additional metadata
}
```

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

All converter methods return a standardized result dictionary:

```python
{
    'success': bool,           # Whether the operation succeeded
    'tables_created': int,     # Number of tables created
    'total_records': int,      # Total records processed
    'duration': float,         # Operation duration in seconds
    'errors': List[str],       # List of error messages
    'files_processed': int,    # Number of files processed (multi-file operations)
    'extracted_files': List[str],  # List of extracted files (PHZ operations)
    'files_created': List[str],    # List of created files (reverse operations)
    'tables_processed': int,   # Number of tables processed (reverse operations)
    'records_processed': int,  # Number of records processed (reverse operations)
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
```

### Custom Schema Mapping

```python
from converter.schema_mapper import TopSpeedToSQLiteMapper

# Create mapper
mapper = TopSpeedToSQLiteMapper()

# Sanitize names
clean_field = mapper.sanitize_field_name("TIT:PROJ_DESCR")  # "PROJ_DESCR"
clean_table = mapper.sanitize_table_name("ORDER")           # "ORDER_TABLE"

# Generate SQL
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
    return
```

### Handle Specific Errors

```python
try:
    results = converter.convert('input.phd', 'output.sqlite')
except FileNotFoundError:
    print("Input file not found")
except PermissionError:
    print("Permission denied - check file permissions")
except Exception as e:
    print(f"Unexpected error: {e}")
```

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
