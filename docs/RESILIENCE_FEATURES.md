# Resilience Features for Large Database Conversions

This document describes the resilience helpers in `src/converter/` intended for large TopSpeed databases, and how they relate to the converters.

> **Important:** `ResilienceConfig` and `ResilienceEnhancer` are standalone helper classes. `SqliteConverter`, `PhzConverter` and `ReverseConverter` do **not** accept a `ResilienceConfig` and do not call `ResilienceEnhancer`; choosing a configuration does not change how the converters run. The settings that do affect a conversion are listed under [What the converters do](#3-what-the-converters-do).

## Overview

The resilience helpers provide:

- **Memory Checks**: Compare the process memory (via `psutil`) with a limit and force garbage collection
- **Adaptive Batch Sizes**: Suggest batch sizes from a table's record size and field count
- **Size Estimation**: Estimate a table's record count by sampling leaf pages
- **Configuration Presets**: Predefined settings for small, medium, large and enterprise databases
- **Progress Logging**: Log progress messages for long operations

## Key Components

### 1. ResilienceEnhancer (`src/converter/resilience_enhancements.py`)

`ResilienceEnhancer(max_memory_mb=500, enable_progress_tracking=True)`

#### Memory Management
- **`check_memory_usage()`**: Returns `True` (and logs a warning) when the process RSS exceeds `max_memory_mb`; returns `False` if `psutil` is unavailable or the check fails
- **`force_memory_cleanup()`**: Runs `gc.collect()`

#### Adaptive Batch Sizing
`get_adaptive_batch_size(table_def)` starts from 100 and checks `table_def.record_size` first, then `table_def.field_count`:

| Condition | Batch size |
|-----------|------------|
| record size > 10,000 bytes | 5 |
| record size > 5,000 bytes | 10 |
| record size > 1,000 bytes | 25 |
| record size < 100 bytes | 400 |
| field count > 200 | 5 |
| field count > 100 | 10 |
| field count > 50 | 50 |
| otherwise | 100 |

#### Safe Data Extraction
- **`extract_raw_data_safe(record)`**: Tries `record.data.data.data`, `record.data.data`, then `record.data`; returns `None` on failure
- **`create_compact_json(raw_data, table_name)`**: Stores raw bytes as base64 in compact JSON with the size and table name (plus leading/trailing bytes for larger records)

#### Size Estimation
- **`estimate_table_size(tps, table_name)`**: Counts the table's DATA records in up to 20 leaf pages, extrapolates to all leaf pages, and assumes ~3 KB per record for the size estimate. Returns `estimated_records`, `estimated_size_mb`, `sample_pages`, `total_pages`, `recommendation` (`skip`, `normal`, `streaming_low_memory`, `streaming_medium_memory`, `streaming_high_memory`) and `optimal_batch_size` (100, 50, 25 or 10)
- **`should_use_streaming(table_def, estimated_size)`**: `True` for more than 10,000 estimated records, record size over 2,000 bytes, or more than 100 fields

#### Progress Logging
- **`log_progress(current, total, table_name, operation="processing")`**: Logs `"<Operation> <table>: current/total (x%)"` when progress tracking is enabled

### 2. ResilienceConfig (`src/converter/resilience_config.py`)

A dataclass of settings with `to_dict()` / `from_dict()`, four preset constructors (`for_small_databases()`, `for_medium_databases()`, `for_large_databases()`, `for_enterprise_databases()`), `get_resilience_config(name)` for `'small'`, `'medium'`, `'large'`, `'enterprise'` or `'default'` (raises `ValueError` for other names), and `estimate_database_size_category(estimated_size_mb, estimated_records)`.

#### Size Categories

`estimate_database_size_category()` returns:

| Category | Condition |
|----------|-----------|
| `enterprise` | size > 10,000 MB or records > 1,000,000 |
| `large` | size > 1,000 MB or records > 100,000 |
| `medium` | size > 100 MB or records > 10,000 |
| `small` | otherwise |

(The preset docstrings describe small as "< 10MB" and medium as "10MB - 1GB"; the function above is what the code uses.)

#### Defaults (`ResilienceConfig()`)

`max_memory_mb=500`, `memory_cleanup_interval=1000`, `default_batch_size=100`, `adaptive_batch_sizing=True`, `max_batch_size=1000`, `min_batch_size=5`, `progress_log_interval=100`, `max_consecutive_errors=100`, `enable_partial_conversion=True`, `enable_streaming=True`, `streaming_threshold_records=10000`, `enable_parallel_processing=False`, `max_worker_threads=4`, `sqlite_journal_mode="WAL"`, `sqlite_synchronous="NORMAL"`, `sqlite_cache_size=-2000`, `sqlite_temp_store="MEMORY"`, `large_table_threshold_records=50000`, `large_table_threshold_mb=500.0`, `enable_checkpointing=False`, `checkpoint_interval=10000`, `enable_resume_capability=False`.

#### Predefined Configurations

**Small** (`for_small_databases()`)
- Memory limit: 200MB
- Batch size: 200 records
- Streaming: Disabled
- Parallel processing: Disabled

**Medium** (`for_medium_databases()`)
- Memory limit: 500MB
- Batch size: 100 records
- Streaming: Enabled (threshold: 5,000 records)
- Parallel processing: Disabled

**Large** (`for_large_databases()`)
- Memory limit: 1000MB
- Batch size: 50 records
- Streaming: Enabled (threshold: 1,000 records)
- Parallel processing: Enabled (2 threads)
- Checkpointing: Enabled (every 5,000 records)
- Detailed progress logging: Enabled

**Enterprise** (`for_enterprise_databases()`)
- Memory limit: 2000MB
- Batch size: 25 records
- Streaming: Enabled (threshold: 500 records)
- Parallel processing: Enabled (4 threads)
- Checkpointing: Enabled (every 1,000 records)
- Resume capability: Enabled
- SQLite cache: `sqlite_cache_size=-10000` (about 10MB)
- Large-table thresholds: 100,000 records / 1,000 MB

Flags such as streaming, parallel processing, checkpointing and resume are configuration values only; no converter currently implements behavior for them.

### 3. What the converters do

`SqliteConverter` (also used by `PhzConverter`):
- **Batch inserts**: rows are inserted with `executemany` in batches of `batch_size` (default 1000; CLI `--batch-size`)
- **Progress reporting**: `progress_callback(current, total, message)` per table, or a `Progress: ...` log line when no callback is given
- **Per-table error isolation**: if a table fails, its inserts are rolled back, the error is logged, and the conversion continues with the next table
- **Fallback path**: tables whose definition pytopspeed cannot parse are converted by an older multidimensional/enhanced path instead of the record codec

## Usage Examples

### Choosing a Configuration from a Size Estimate

```python
from converter.phz_converter import PhzConverter
from converter.resilience_config import get_resilience_config, estimate_database_size_category

category = estimate_database_size_category(estimated_size_mb=150, estimated_records=50000)  # 'medium'
config = get_resilience_config(category)
print(config.max_memory_mb, config.default_batch_size)  # 500 100

# The converter does not take the config; pass a batch size explicitly if wanted
converter = PhzConverter(batch_size=1000)
result = converter.convert_phz('large_database.phz', 'output.sqlite')
```

### Manual Configuration

```python
from converter.resilience_config import ResilienceConfig
from converter.resilience_enhancements import ResilienceEnhancer

# Create custom configuration
config = ResilienceConfig(
    max_memory_mb=1000,
    default_batch_size=50,
    enable_streaming=True,
    enable_parallel_processing=True
)

# Use the memory limit with the enhancer
enhancer = ResilienceEnhancer(max_memory_mb=config.max_memory_mb)
if enhancer.check_memory_usage():
    enhancer.force_memory_cleanup()
```

### Enterprise Preset

```python
from converter.resilience_config import get_resilience_config

config = get_resilience_config('enterprise')

# This configuration includes:
# - 2000MB memory limit
# - 25 record batches
# - Parallel processing flag (4 threads)
# - Checkpointing flag, interval 1,000 records
# - Resume capability flag
# - sqlite_cache_size=-10000 (about 10MB)
```

See `examples/resilient_conversion_example.py` for a runnable demonstration.

## Performance Characteristics

No benchmarks for large databases have been published for this project. As a reference point, converting the TxWells sample (TxWells.PHD: 55 tables, 9,492 rows; TxWells.mod: 21 tables, 5,267 rows; FORCAST is the largest table with 4,370 rows) takes a few seconds per file on a desktop machine. Memory use and throughput for larger files depend on table layout and hardware; measure with your own data.

## Monitoring and Troubleshooting

### Progress Monitoring
Without a progress callback, `SqliteConverter` logs progress per table:

```
2026-10-12 12:48:43,560 - SqliteConverter - INFO - Progress: 13/55 (23.6%) - Migrating table: FORCAST
2026-10-12 12:48:47,785 - SqliteConverter - INFO - Migrated 4370 records from FORCAST
```

### Memory Monitoring
`ResilienceEnhancer.check_memory_usage()` logs through the `converter.resilience_enhancements` logger:

```
Memory usage 512.3MB exceeds limit 500MB
```

### Error Handling
Table-level errors are logged and the conversion continues:

```
2026-10-12 12:38:02,202 - SqliteConverter - ERROR - Error migrating data for table FORCAST: <error>
```

## Best Practices

- For most files the default converter settings are sufficient
- Ensure enough free disk space: the SQLite database is considerably larger than the TopSpeed file because it also stores the original record bytes for reverse conversion (TxWells.PHD, about 0.8 MB, becomes a database of about 37 MB)
- For very large files, lower `--batch-size` if memory is a concern, and monitor system resources

## Configuration Tuning

These snippets set `ResilienceConfig` values; they matter only to code that reads the configuration.

### Memory Settings
```python
config = ResilienceConfig(
    max_memory_mb=1000,  # Increase for more memory
    memory_cleanup_interval=500,  # More frequent cleanup
)
```

### Batch Size Tuning
```python
config = ResilienceConfig(
    default_batch_size=25,  # Smaller batches for large records
    adaptive_batch_sizing=True,  # Enable automatic adjustment
)
```

### SQLite Optimization
```python
config = ResilienceConfig(
    sqlite_cache_size=-10000,  # about 10MB cache
    sqlite_journal_mode="WAL",  # Write-ahead logging
    sqlite_synchronous="NORMAL",  # Balance safety/speed
)
```

## Troubleshooting Common Issues

### Out of Memory Errors
- Reduce the converter batch size (`SqliteConverter(batch_size=...)` or `--batch-size`)
- Convert files one at a time instead of combining them

### Slow Performance
- Increase the batch size (if memory allows)
- Use local SSD storage for input and output files

### Conversion Failures
- Run the CLI with `-v` and check the log for table-level errors
- Check disk space availability
- Verify file permissions

## Future Enhancements

Possible improvements (configuration flags exist for some of these, but they are not implemented in the converters):

1. **Converter integration**: Let the converters accept a `ResilienceConfig`
2. **Resume Capability**: Ability to resume interrupted conversions
3. **Parallel Processing**: Multi-threaded conversion for large tables
4. **Compression**: Optional compression of stored binary data
5. **Incremental Conversion**: Convert only changed data

## Conclusion

The resilience helpers provide building blocks for large conversions:

- **Size categories and presets** for choosing settings
- **Memory checks and cleanup**
- **Adaptive batch-size and streaming suggestions**
- **Progress logging**

They are covered by unit and integration tests (see [TESTING_RESILIENCE_FEATURES.md](TESTING_RESILIENCE_FEATURES.md)) but are not yet wired into the converters.
