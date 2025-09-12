# Testing Resilience Features

This document describes the comprehensive test suite for the resilience enhancements added to the TopSpeed to SQLite converter.

## Test Structure

The test suite is organized into three main categories:

### 1. Unit Tests (`tests/unit/`)

**Purpose**: Test individual components in isolation

**Files**:
- `test_resilience_enhancer.py` - Tests for ResilienceEnhancer class
- `test_resilience_config.py` - Tests for ResilienceConfig class  
- `test_sqlite_converter_enhancements.py` - Tests for enhanced SQLite converter methods

**Coverage**:
- Memory management functions
- Adaptive batch sizing algorithms
- Safe data extraction methods
- Compact JSON creation
- Size estimation logic
- Configuration management
- Error handling and recovery

### 2. Integration Tests (`tests/integration/`)

**Purpose**: Test component interactions and end-to-end scenarios

**Files**:
- `test_resilience_integration.py` - Integration tests for resilience features

**Coverage**:
- Configuration selection based on database size
- Adaptive batch sizing with different table types
- Memory monitoring integration
- Size estimation with realistic data
- Streaming decision logic
- Progress tracking integration
- Error recovery scenarios

### 3. Performance Tests (`tests/performance/`)

**Purpose**: Test performance characteristics and scalability

**Files**:
- `test_resilience_performance.py` - Performance tests for resilience features

**Coverage**:
- Memory usage under different batch sizes
- Processing speed with various data sizes
- Scalability with increasing data volumes
- Concurrent operation performance
- Memory limit enforcement overhead
- Batch size optimization impact

## Running Tests

### Quick Start

```bash
# Run all resilience tests
python tests/run_resilience_tests.py

# Run specific test types
python tests/run_resilience_tests.py unit
python tests/run_resilience_tests.py integration
python tests/run_resilience_tests.py performance

# Run with verbose output
python tests/run_resilience_tests.py -v

# Run with coverage reporting
python tests/run_resilience_tests.py -c
```

### Using pytest directly

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run specific test file
pytest tests/unit/test_resilience_enhancer.py -v

# Run specific test method
pytest tests/unit/test_resilience_enhancer.py::TestResilienceEnhancer::test_initialization -v

# Run with coverage
pytest tests/unit/ --cov=src/converter --cov-report=html
```

### Performance Tests

Performance tests are marked and can be skipped by default:

```bash
# Run performance tests (requires --run-performance flag)
pytest tests/performance/ --run-performance

# Skip performance tests (default behavior)
pytest tests/performance/
```

## Test Categories

### Unit Test Categories

#### ResilienceEnhancer Tests
- **Initialization**: Test constructor with various parameters
- **Memory Management**: Test memory monitoring and cleanup
- **Adaptive Batch Sizing**: Test batch size calculation for different table types
- **Data Extraction**: Test safe raw data extraction with fallbacks
- **JSON Creation**: Test compact JSON creation with various data sizes
- **Size Estimation**: Test table size estimation algorithms
- **Streaming Decisions**: Test streaming recommendation logic
- **Progress Logging**: Test progress tracking functionality

#### ResilienceConfig Tests
- **Configuration Creation**: Test configuration initialization
- **Predefined Configs**: Test small/medium/large/enterprise configurations
- **Configuration Validation**: Test configuration consistency
- **Size Category Estimation**: Test database size category detection
- **Configuration Conversion**: Test dict conversion and serialization

#### SQLite Converter Enhancement Tests
- **Enhanced Table Definitions**: Test robust table definition parsing
- **Memory Management**: Test memory monitoring integration
- **Batch Processing**: Test adaptive batch sizing integration
- **Error Recovery**: Test error handling and recovery mechanisms

### Integration Test Categories

#### Configuration Integration
- **Auto-Detection**: Test automatic configuration selection
- **Size-Based Selection**: Test configuration selection based on database size
- **Consistency Validation**: Test configuration consistency across categories

#### Processing Integration
- **End-to-End Scenarios**: Test complete conversion workflows
- **Memory Management**: Test memory monitoring during conversion
- **Progress Tracking**: Test progress reporting during long operations
- **Error Recovery**: Test error handling during conversion

#### Performance Integration
- **Scalability Testing**: Test performance with various database sizes
- **Resource Management**: Test resource usage under different conditions
- **Concurrent Operations**: Test multi-threaded scenarios

### Performance Test Categories

#### Memory Performance
- **Memory Usage**: Test memory consumption with different batch sizes
- **Memory Cleanup**: Test garbage collection performance
- **Memory Monitoring**: Test memory monitoring overhead
- **Memory Limits**: Test memory limit enforcement

#### Processing Performance
- **Batch Size Optimization**: Test performance impact of different batch sizes
- **Data Size Scaling**: Test performance with increasing data sizes
- **Concurrent Processing**: Test multi-threaded performance
- **JSON Creation**: Test JSON serialization performance

#### Scalability Performance
- **Large Data Handling**: Test performance with very large datasets
- **Resource Scaling**: Test resource usage scaling
- **Throughput Testing**: Test records per second processing rates

## Test Data

### Mock Data Generation

The test suite includes comprehensive mock data generation:

```python
# Binary data of various sizes
test_data = {
    'small': b"small data",
    'medium': b"x" * 1000,
    'large': b"x" * 10000,
    'very_large': b"x" * 100000
}

# Table definitions with different characteristics
table_defs = {
    'small': Mock(record_size=50, field_count=10),
    'medium': Mock(record_size=500, field_count=25),
    'large': Mock(record_size=2000, field_count=50),
    'very_large': Mock(record_size=8000, field_count=150)
}
```

### Realistic Test Scenarios

Tests use realistic scenarios based on actual TopSpeed database characteristics:

- **Small Databases**: < 10MB, < 10,000 records
- **Medium Databases**: 10MB - 1GB, 10,000 - 100,000 records  
- **Large Databases**: 1GB - 10GB, 100,000 - 1,000,000 records
- **Enterprise Databases**: > 10GB, > 1,000,000 records

## Test Configuration

### Pytest Configuration

The test suite uses `conftest.py` for shared configuration:

```python
# Custom markers
@pytest.mark.performance  # Performance tests
@pytest.mark.slow        # Slow running tests
@pytest.mark.memory      # Memory intensive tests

# Custom options
--run-performance        # Enable performance tests
--memory-limit=500       # Set memory limit for tests
```

### Test Fixtures

Common fixtures are provided for:

- **Temporary Directories**: For test file creation
- **Mock TPS Objects**: For TopSpeed file simulation
- **Table Definitions**: For various table types
- **Binary Data**: For different data sizes
- **Size Estimates**: For database size simulation

## Coverage Goals

### Unit Test Coverage
- **Target**: 95%+ line coverage
- **Focus**: All public methods and edge cases
- **Exclusions**: External dependencies and error paths

### Integration Test Coverage
- **Target**: 90%+ scenario coverage
- **Focus**: Component interactions and workflows
- **Exclusions**: External system dependencies

### Performance Test Coverage
- **Target**: 80%+ performance scenario coverage
- **Focus**: Scalability and resource usage
- **Exclusions**: Hardware-specific optimizations

## Continuous Integration

### GitHub Actions Integration

The test suite is designed to work with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run Unit Tests
  run: pytest tests/unit/ --cov=src/converter --cov-report=xml

- name: Run Integration Tests  
  run: pytest tests/integration/

- name: Run Performance Tests
  run: pytest tests/performance/ --run-performance
```

### Test Reporting

Tests generate comprehensive reports:

- **Coverage Reports**: HTML and XML coverage reports
- **Performance Reports**: Timing and memory usage reports
- **Test Results**: Detailed test result summaries

## Debugging Tests

### Common Issues

1. **Import Errors**: Ensure `src/` is in Python path
2. **Mock Failures**: Check mock object configurations
3. **Performance Failures**: Verify system resources
4. **Memory Issues**: Check memory limits and cleanup

### Debug Commands

```bash
# Run with debug output
pytest tests/unit/ -v -s --tb=long

# Run specific test
pytest tests/unit/test_resilience_enhancer.py::TestResilienceEnhancer::test_memory_usage -v -s

# Run with pdb debugger
pytest tests/unit/ --pdb
```

## Test Maintenance

### Adding New Tests

1. **Unit Tests**: Add to appropriate test file in `tests/unit/`
2. **Integration Tests**: Add to `tests/integration/test_resilience_integration.py`
3. **Performance Tests**: Add to `tests/performance/test_resilience_performance.py`

### Test Naming Conventions

- **Test Classes**: `Test{ComponentName}`
- **Test Methods**: `test_{functionality}_{scenario}`
- **Fixtures**: `{purpose}_fixture`

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
- **Baseline**: Establish performance baselines
- **Tolerance**: Use reasonable performance tolerances
- **Resources**: Consider system resource limitations
- **Reproducible**: Ensure tests are reproducible

## Conclusion

The resilience feature test suite provides comprehensive coverage of:

- **Functionality**: All resilience features are thoroughly tested
- **Integration**: Component interactions are validated
- **Performance**: Scalability and resource usage are verified
- **Reliability**: Error handling and recovery are tested

This ensures that the resilience enhancements work correctly across all supported scenarios and database sizes, from small test databases to enterprise-scale production systems.
