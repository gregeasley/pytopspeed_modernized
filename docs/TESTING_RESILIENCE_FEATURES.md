# Testing Resilience Features

This document describes the tests for the resilience helpers (`ResilienceEnhancer`, `ResilienceConfig`) and the related `SqliteConverter` fallback helper methods. See [RESILIENCE_FEATURES.md](RESILIENCE_FEATURES.md) for what these components do.

## Test Structure

The resilience tests are organized into three categories:

### 1. Unit Tests (`tests/unit/`)

**Purpose**: Test individual components in isolation

**Files**:
- `test_resilience_enhancer.py` - Tests for the ResilienceEnhancer class (42 tests)
- `test_resilience_config.py` - Tests for the ResilienceConfig class and helper functions (32 tests)
- `test_sqlite_converter_enhancements.py` - More ResilienceEnhancer checks plus the SqliteConverter fallback helpers `_create_enhanced_table_definition()` and `_get_table_definition_robust()` (28 tests)

**Coverage**:
- Memory checks and cleanup
- Adaptive batch sizing
- Safe data extraction
- Compact JSON creation
- Size estimation and recommendations
- Configuration presets and size categories
- Error handling paths

### 2. Integration Tests (`tests/integration/`)

**Purpose**: Test the helpers together on realistic scenarios

**Files**:
- `test_resilience_integration.py` - Integration tests for resilience features (16 tests)

**Coverage**:
- Configuration selection based on database size
- Adaptive batch sizing with different table types
- Memory monitoring
- Size estimation
- Streaming decision logic
- Progress tracking
- Error recovery scenarios

### 3. Performance Tests (`tests/performance/`)

**Purpose**: Measure the helpers' overhead and behavior with larger inputs

**Files**:
- `test_resilience_performance.py` - Performance tests for resilience features (14 tests, skipped unless `--run-performance` is given)

**Coverage**:
- Memory usage under different batch sizes
- Processing speed with various data sizes
- Scaling with increasing data volumes
- Concurrent operation
- Memory limit check overhead
- Batch size impact

All of these tests use mocks and synthetic data; none of them needs a real TopSpeed file.

## Running Tests

### Quick Start

`tests/run_resilience_tests.py` wraps pytest (it runs `python -m pytest`, so `python` must be on your PATH):

```bash
# Run the unit, integration and performance directories (all tests in them, not only resilience tests)
python tests/run_resilience_tests.py

# Run only the resilience test files of one type
python tests/run_resilience_tests.py unit
python tests/run_resilience_tests.py integration
python tests/run_resilience_tests.py performance   # collected but skipped: the runner does not pass --run-performance

# Run with verbose output
python tests/run_resilience_tests.py -v

# Run with coverage reporting
python tests/run_resilience_tests.py -c

# Run one file / list the available test files
python tests/run_resilience_tests.py -f tests/unit/test_resilience_config.py
python tests/run_resilience_tests.py -l
```

The runner prints emoji status lines; on a Windows console with a legacy code page set `PYTHONIOENCODING=utf-8` first.

### Using pytest directly

```bash
# Run the resilience unit tests
python -m pytest tests/unit/test_resilience_enhancer.py tests/unit/test_resilience_config.py tests/unit/test_sqlite_converter_enhancements.py -v

# Run a specific test method
python -m pytest tests/unit/test_resilience_enhancer.py::TestResilienceEnhancer::test_initialization -v

# Run all unit tests with coverage
python -m pytest tests/unit/ --cov=src/converter --cov-report=html
```

### Performance Tests

Tests under `tests/performance/` are skipped by default (`tests/conftest.py` skips any test with the `performance` keyword, which includes everything in that directory):

```bash
# Run performance tests
python -m pytest tests/performance/ --run-performance

# Default: collected and skipped
python -m pytest tests/performance/
```

## Test Categories

### Unit Test Categories

#### ResilienceEnhancer Tests
- **Initialization**: Constructor with default and custom parameters
- **Memory Management**: `check_memory_usage()` under/over the limit, without psutil, on exceptions; `force_memory_cleanup()`
- **Adaptive Batch Sizing**: Batch sizes for small, large, very large and complex tables
- **Data Extraction**: `extract_raw_data_safe()` with fallbacks
- **JSON Creation**: `create_compact_json()` with various data sizes
- **Size Estimation**: `estimate_table_size()`, recommendations and optimal batch sizes
- **Streaming Decisions**: `should_use_streaming()`
- **Progress Logging**: `log_progress()` enabled, disabled and without a total

#### ResilienceConfig Tests
- **Configuration Creation**: Defaults and custom values
- **Predefined Configs**: small/medium/large/enterprise presets
- **Configuration Consistency**: Relationships between presets
- **Size Category Estimation**: `estimate_database_size_category()`
- **Configuration Conversion**: `to_dict()` / `from_dict()`

#### SQLite Converter Enhancement Tests
- **ResilienceEnhancer methods**: Batch sizing, memory checks, data extraction, compact JSON and size estimation (overlapping the enhancer tests)
- **Enhanced Table Definitions**: `_create_enhanced_table_definition()` with normal, large-array, insufficient and failing input
- **Robust Definition Lookup**: `_get_table_definition_robust()` with normal, enhanced and minimal fallbacks

### Integration Test Categories

- **Configuration Selection**: Choosing presets from database size estimates and checking consistency across categories
- **Batch Sizing and Streaming**: Combined decisions for different table types
- **Memory and Progress**: Memory checks and progress logging together
- **Error Recovery**: Behavior when estimation or extraction fails

### Performance Test Categories

- **Memory**: Memory use with different batch sizes, cleanup and monitoring overhead
- **Processing**: Batch size impact, data size scaling, concurrent processing, JSON creation speed

## Test Data

### Mock Data Generation

`tests/conftest.py` provides fixtures with synthetic data, for example:

```python
# sample_binary_data
{
    'small': b"small data",
    'medium': b"x" * 1000,
    'large': b"x" * 10000,
    'very_large': b"x" * 100000
}

# sample_table_definitions
{
    'small': Mock(record_size=50, field_count=10),
    'medium': Mock(record_size=500, field_count=25),
    'large': Mock(record_size=2000, field_count=50),
    'very_large': Mock(record_size=8000, field_count=150)
}
```

### Size Scenarios

`sample_size_estimates` and the integration tests use the boundaries of `estimate_database_size_category()`:

- **Small**: up to 100 MB and up to 10,000 records
- **Medium**: more than 100 MB or more than 10,000 records
- **Large**: more than 1,000 MB or more than 100,000 records
- **Enterprise**: more than 10,000 MB or more than 1,000,000 records

## Test Configuration

### Pytest Configuration

`tests/conftest.py` registers markers and options:

```python
# Custom markers
@pytest.mark.performance    # Performance tests
@pytest.mark.slow           # Slow running tests
@pytest.mark.memory         # Memory intensive tests
@pytest.mark.real_topspeed  # Use the real TPS reader instead of the default mocks

# Custom options
--run-performance        # Enable performance tests
--memory-limit=500       # Defined, but not currently used by any test
```

`pyproject.toml` additionally declares the `slow`, `integration` and `unit` markers and runs pytest with `--strict-markers`.

### Test Fixtures

Common fixtures are provided for:

- **Temporary Directories**: `temp_dir`, `test_data_dir`, `temp_sqlite_db`
- **Mock TPS Objects**: `mock_tps`, `sample_tps`
- **Table Definitions**: `mock_table_def`, `sample_table_def`, `sample_table_definitions`
- **Binary Data**: `sample_binary_data`
- **Size Estimates**: `sample_size_estimates`
- **System Mocks**: `mock_psutil_process`, `mock_gc`
- **Data Generator**: `test_data_generator`

An autouse fixture replaces `TPS.__init__` and `TpsRecordsList` with mocks for every test that is not marked `real_topspeed`.

## Coverage

There are no enforced coverage thresholds for these tests. Generate a report with `--cov=src/converter --cov-report=html` (or `-c` with the runner) to check coverage locally.

## Continuous Integration

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs the unit tests only, which include the resilience unit tests:

```yaml
- name: Test with pytest
  run: |
    pytest tests/unit/ -v --cov=src --cov-report=xml --cov-report=html
```

The integration and performance tests are not run in CI; run them locally:

```bash
python -m pytest tests/integration/
python -m pytest tests/performance/ --run-performance
```

### Test Reporting

- **Coverage Reports**: HTML and XML coverage reports when run with `--cov`
- **Test Results**: Standard pytest output

## Debugging Tests

### Common Issues

1. **Import Errors**: Ensure `src/` is on the Python path (the test modules add it themselves)
2. **Mock Failures**: Check mock object configurations; remember the autouse TPS mock
3. **Performance Failures**: Verify system resources; timings depend on the machine
4. **Memory Issues**: Check memory limits and cleanup

### Debug Commands

```bash
# Run with debug output
python -m pytest tests/unit/ -v -s --tb=long

# Run a specific test
python -m pytest tests/unit/test_resilience_enhancer.py::TestResilienceEnhancer::test_check_memory_usage_over_limit -v -s

# Run with pdb debugger
python -m pytest tests/unit/ --pdb
```

## Test Maintenance

### Adding New Tests

1. **Unit Tests**: Add to the appropriate test file in `tests/unit/`
2. **Integration Tests**: Add to `tests/integration/test_resilience_integration.py`
3. **Performance Tests**: Add to `tests/performance/test_resilience_performance.py`
4. **Test data**: Use synthetic data only (mocks, or `tests/unit/tps_builder.py` for real TopSpeed files with `pytest.mark.real_topspeed`); the TxWells sample files are not available in CI

### Test Naming Conventions

- **Test Classes**: `Test{ComponentName}`
- **Test Methods**: `test_{functionality}_{scenario}`

### Test Documentation

- **Docstrings**: Describe test purpose and expected behavior
- **Comments**: Explain complex test logic
- **Assertions**: Use descriptive assertion messages

## Best Practices

### Test Design
- **Isolation**: Each test should be independent
- **Deterministic**: Tests should produce consistent results
- **Fast**: Unit tests should run quickly
- **Clear**: Test intent should be obvious

### Mock Usage
- **Minimal**: Mock only what's necessary
- **Realistic**: Use realistic mock data
- **Verification**: Verify mock interactions when important

### Performance Testing
- **Tolerance**: Use generous tolerances; timings vary by machine
- **Resources**: Consider system resource limitations
- **Reproducible**: Ensure tests are reproducible

## Conclusion

The resilience test suite covers:

- **Functionality**: The public methods of `ResilienceEnhancer` and `ResilienceConfig`
- **Integration**: The helpers used together on simulated database sizes
- **Performance**: Overhead of the helpers (opt-in with `--run-performance`)

Because the converters do not use these helpers yet, these tests do not exercise real conversions; conversion behavior is covered by the converter, record codec, writer and reverse converter tests.
