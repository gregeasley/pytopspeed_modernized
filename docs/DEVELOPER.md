# Developer Documentation

This document provides comprehensive information for developers working on the Pytopspeed Modernized library.

## 📚 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Code Structure](#code-structure)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Code Style](#code-style)
- [Extension Guidelines](#extension-guidelines)
- [Contributing](#contributing)
- [Release Process](#release-process)

## 🏗️ Architecture Overview

### High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   TopSpeed      │    │   Pytopspeed    │    │   SQLite        │
│   Files         │───▶│   Parser        │───▶│   Database      │
│   (.phd, .mod)  │    │   (Modernized)  │    │   (.sqlite)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Converter     │
                       │   Components    │
                       └─────────────────┘
```

### Component Overview

1. **TopSpeed Parser** (`src/pytopspeed/`): Modernized parser for reading TopSpeed files
2. **Schema Mapper** (`src/converter/schema_mapper.py`): Maps TopSpeed schemas to SQLite
3. **SQLite Converter** (`src/converter/sqlite_converter.py`): Handles data migration
4. **PHZ Converter** (`src/converter/phz_converter.py`): Handles zip archives
5. **Reverse Converter** (`src/converter/reverse_converter.py`): Converts back to TopSpeed
6. **CLI Interface** (`src/cli/`): Command-line interface

### Data Flow

```
TopSpeed File → Parser → Schema Mapper → SQLite Converter → SQLite Database
                     ↓
               Table Definitions
                     ↓
               Field Mappings
                     ↓
               Data Conversion
```

## 📁 Code Structure

### Directory Layout

```
pytopspeed_modernized/
├── src/                          # Source code
│   ├── pytopspeed/              # Modernized TopSpeed parser
│   │   ├── __init__.py
│   │   ├── tps.py               # Main TPS class
│   │   ├── tpspage.py           # Page handling
│   │   ├── tpsrecord.py         # Record parsing
│   │   ├── tpstable.py          # Table definitions
│   │   ├── tpscrypt.py          # Encryption/decryption
│   │   └── utils.py             # Utility functions
│   ├── converter/               # Conversion components
│   │   ├── __init__.py
│   │   ├── schema_mapper.py     # Schema mapping
│   │   ├── sqlite_converter.py  # SQLite conversion
│   │   ├── phz_converter.py     # PHZ file handling
│   │   └── reverse_converter.py # Reverse conversion
│   └── cli/                     # Command-line interface
│       ├── __init__.py
│       └── main.py              # CLI entry point
├── tests/                       # Test suite
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── debug/                   # Debug scripts
├── examples/                    # Example scripts
├── docs/                        # Documentation
├── assets/                      # Sample data files
└── scripts/                     # Utility scripts
```

### Key Classes and Their Responsibilities

#### TPS (TopSpeed Parser)
- **File**: `src/pytopspeed/tps.py`
- **Purpose**: Main interface for reading TopSpeed files
- **Key Methods**: `__init__()`, `set_current_table()`, `__iter__()`

#### SqliteConverter
- **File**: `src/converter/sqlite_converter.py`
- **Purpose**: Converts TopSpeed data to SQLite format
- **Key Methods**: `convert()`, `convert_multiple()`, `_create_schema()`, `_migrate_table_data()`
- **Enhanced Features**: 
  - `_create_enhanced_table_definition()`: Advanced table definition parsing
  - `_parse_multidimensional_field_value_from_binary()`: Enhanced field parsing
  - `_parse_multidimensional_array_field_from_binary()`: Array field parsing
  - Comprehensive type mapping for all TopSpeed field types
  - Generic binary data detection for misidentified STRING fields
  - Dynamic field offset calculation for complex table structures
  - Large table support (100+ fields) without artificial limits

#### TopSpeedToSQLiteMapper
- **File**: `src/converter/schema_mapper.py`
- **Purpose**: Maps TopSpeed table definitions to SQLite schema
- **Key Methods**: `sanitize_field_name()`, `sanitize_table_name()`, `generate_create_table_sql()`

#### PhzConverter
- **File**: `src/converter/phz_converter.py`
- **Purpose**: Handles .phz (zip) files containing TopSpeed files
- **Key Methods**: `convert_phz()`, `list_phz_contents()`

#### ReverseConverter
- **File**: `src/converter/reverse_converter.py`
- **Purpose**: Converts SQLite databases back to TopSpeed files
- **Key Methods**: `convert_sqlite_to_topspeed()`, `_write_topspeed_file()`

## 🛠️ Development Setup

### Prerequisites

- Python 3.8+ (3.11 recommended)
- Git
- Conda or virtual environment
- Code editor (VS Code, PyCharm, etc.)

### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/gregeasley/pytopspeed_modernized
   cd pytopspeed_modernized
   ```

2. **Create development environment**:
   ```bash
   conda create -n pytopspeed_modernized_dev python=3.11
   conda activate pytopspeed_modernized_dev
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -e .  # Install in development mode
   ```

4. **Install development dependencies**:
   ```bash
   pip install pytest-cov black flake8 mypy
   ```

5. **Verify setup**:
   ```bash
   python -m pytest tests/unit/ -v
   python pytopspeed.py --help
   ```

### IDE Configuration

#### VS Code

Create `.vscode/settings.json`:
```json
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.flake8Enabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"]
}
```

#### PyCharm

1. Open project in PyCharm
2. Configure Python interpreter to use the conda environment
3. Enable pytest as test runner
4. Configure code style to use Black formatter

## 🧪 Testing

### Test Structure

```
tests/
├── unit/                        # Unit tests
│   ├── test_tps_parser.py      # Parser tests
│   ├── test_schema_mapper.py   # Schema mapper tests
│   ├── test_sqlite_converter.py # SQLite converter tests
│   ├── test_phz_converter.py   # PHZ converter tests
│   ├── test_reverse_converter.py # Reverse converter tests
│   └── test_combined_conversion.py # Combined conversion tests
├── integration/                 # Integration tests
│   ├── test_end_to_end_conversion.py
│   ├── test_performance.py
│   └── test_phd_parser.py
├── debug/                       # Debug scripts
└── conftest.py                  # Pytest configuration
```

### Running Tests

```bash
# Run all unit tests
python -m pytest tests/unit/ -v

# Run specific test file
python -m pytest tests/unit/test_tps_parser.py -v

# Run with coverage
python -m pytest tests/unit/ --cov=src --cov-report=html

# Run integration tests
python -m pytest tests/integration/ -v

# Run all tests
python -m pytest tests/ -v
```

### Writing Tests

#### Unit Test Example

```python
import pytest
from unittest.mock import Mock, patch
from converter.sqlite_converter import SqliteConverter

class TestSqliteConverter:
    def test_convert_single_file(self, sample_phd_file, temp_sqlite_db):
        """Test converting a single PHD file to SQLite."""
        converter = SqliteConverter()
        results = converter.convert(sample_phd_file, temp_sqlite_db)
        
        assert results['success'] is True
        assert results['tables_created'] > 0
        assert results['total_records'] > 0
        assert len(results['errors']) == 0
```

#### Integration Test Example

```python
import pytest
import sqlite3
from converter.sqlite_converter import SqliteConverter

def test_end_to_end_conversion():
    """Test complete conversion workflow."""
    converter = SqliteConverter()
    results = converter.convert('assets/TxWells.PHD', 'test_output.sqlite')
    
    # Verify conversion succeeded
    assert results['success'] is True
    
    # Verify database structure
    conn = sqlite3.connect('test_output.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    assert len(tables) > 0
    
    # Verify data
    cursor.execute("SELECT COUNT(*) FROM OWNER")
    count = cursor.fetchone()[0]
    assert count > 0
    
    conn.close()
```

### Test Fixtures

Common fixtures are defined in `tests/conftest.py`:

```python
@pytest.fixture
def sample_phd_file():
    """Path to sample PHD file."""
    return 'assets/TxWells.PHD'

@pytest.fixture
def temp_sqlite_db(tmp_path):
    """Temporary SQLite database path."""
    return str(tmp_path / 'test.db')

@pytest.fixture
def mock_tps():
    """Mock TPS object for testing."""
    tps = Mock()
    tps.tables = Mock()
    tps.tables.get_definition.return_value = Mock()
    return tps
```

## 🎨 Code Style

### Python Style Guide

We follow PEP 8 with some modifications:

- **Line length**: 100 characters (instead of 80)
- **Import order**: Standard library, third-party, local imports
- **Docstrings**: Google style
- **Type hints**: Required for all public methods

### Code Formatting

We use Black for code formatting:

```bash
# Format all Python files
black src/ tests/ examples/

# Check formatting without changing files
black --check src/ tests/ examples/
```

### Linting

We use flake8 for linting:

```bash
# Run linting
flake8 src/ tests/ examples/

# Ignore specific errors
flake8 --ignore=E203,W503 src/ tests/ examples/
```

### Type Checking

We use mypy for type checking:

```bash
# Run type checking
mypy src/

# With strict mode
mypy --strict src/
```

### Pre-commit Hooks

Create `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 22.3.0
    hooks:
      - id: black
        language_version: python3.11
  - repo: https://github.com/pycqa/flake8
    rev: 4.0.1
    hooks:
      - id: flake8
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v0.950
    hooks:
      - id: mypy
        additional_dependencies: [types-all]
```

Install pre-commit hooks:

```bash
pip install pre-commit
pre-commit install
```

## 🚀 Recent Technical Improvements

### Enhanced Multidimensional Parsing

The library has undergone significant improvements to handle complex TopSpeed table structures:

#### Comprehensive Type Mapping
- **All TopSpeed Types Supported**: Including type 79 (DOUBLE_79), type 83/82 (SHORT), type 0 (DOUBLE), etc.
- **Dynamic Type Detection**: Automatically maps raw type codes to appropriate Python types
- **Generic Binary Data Detection**: Identifies when STRING fields contain binary data and parses them as integers

#### Advanced Field Offset Calculation
- **Dynamic Offset Calculation**: Replaces fixed offset increments with calculated field definition sizes
- **Type-Specific Handling**: Special handling for DOUBLE_79 fields with extra metadata bytes
- **Robust Error Recovery**: Continues parsing even when individual fields fail

#### Large Table Support
- **No Artificial Limits**: Removed 50-field limit for enhanced table definition parsing
- **Scalable Architecture**: Handles tables with 100+ fields efficiently
- **Memory Optimization**: Efficient processing of large table structures

#### Technical Implementation Details

```python
# Enhanced field type mapping
FIELD_TYPE_STRUCT = Enum(Byte,
    BYTE=1, SHORT=2, DATE=3, TIME=4, LONG=5, STRING=6, DECIMAL=7, MEMO=8, BLOB=9,
    CSTRING=10, PSTRING=11, PICTURE=12, DOUBLE=13, DOUBLE_79=79, _default_='STRING'
)

# Generic binary data detection
if type_code in ['STRING', 'CSTRING', '6', '10']:
    if len(field_data) <= 4 and any(byte < 32 or byte > 126 for byte in field_data if byte != 0):
        # Parse as integer based on field size
        if len(field_data) >= 4:
            return struct.unpack('<I', field_data[:4])[0]  # 32-bit integer
        elif len(field_data) >= 2:
            return struct.unpack('<H', field_data[:2])[0]  # 16-bit integer
        elif len(field_data) >= 1:
            return field_data[0]  # 8-bit integer
```

## 🔧 Extension Guidelines

### Adding New File Formats

To add support for a new TopSpeed file format:

1. **Update the parser** (`src/pytopspeed/tps.py`):
   ```python
   def _detect_file_type(self, filename: str) -> str:
       ext = os.path.splitext(filename)[1].lower()
       if ext in ['.phd', '.mod', '.tps', '.newformat']:
           return 'topspeed'
       # ...
   ```

2. **Update the converter** (`src/converter/sqlite_converter.py`):
   ```python
   def _get_file_prefix(self, filename: str) -> str:
       ext = os.path.splitext(filename)[1].lower()
       if ext == '.newformat':
           return 'new_'
       # ...
   ```

3. **Add tests**:
   ```python
   def test_new_format_conversion(self):
       # Test new format conversion
       pass
   ```

4. **Update documentation**:
   - Add to supported formats list
   - Update examples
   - Add to API documentation

### Adding New Data Types

To add support for a new TopSpeed data type:

1. **Update the parser** (`src/pytopspeed/tpsrecord.py`):
   ```python
   # Add new data type parsing logic
   def _parse_new_type(self, data: bytes) -> Any:
       # Parse new data type
       pass
   ```

2. **Update the schema mapper** (`src/converter/schema_mapper.py`):
   ```python
   TYPE_MAPPING = {
       # ... existing mappings
       'NEW_TYPE': 'SQLITE_TYPE',
   }
   ```

3. **Update the converter** (`src/converter/sqlite_converter.py`):
   ```python
   def _convert_field_value(self, field: Any, value: Any) -> Any:
       if field.type == 'NEW_TYPE':
           return self._convert_new_type(value)
       # ...
   ```

4. **Add tests**:
   ```python
   def test_new_type_conversion(self):
       # Test new type conversion
       pass
   ```

### Adding New CLI Commands

To add a new CLI command:

1. **Update the CLI** (`src/cli/main.py`):
   ```python
   def new_command(args) -> int:
       """Handle new command."""
       # Implementation
       return 0
   
   def main():
       # Add subparser
       new_parser = subparsers.add_parser('new', help='New command')
       new_parser.add_argument('input', help='Input file')
       # ...
   ```

2. **Add tests**:
   ```python
   def test_new_command():
       # Test new command
       pass
   ```

3. **Update documentation**:
   - Add to README
   - Add to API documentation
   - Add examples

## 🤝 Contributing

### Contribution Process

1. **Fork the repository**
2. **Create a feature branch**:
   ```bash
   git checkout -b feature/new-feature
   ```
3. **Make changes**:
   - Write code following the style guide
   - Add tests for new functionality
   - Update documentation
4. **Run tests**:
   ```bash
   python -m pytest tests/ -v
   ```
5. **Commit changes**:
   ```bash
   git commit -m "Add new feature"
   ```
6. **Push to fork**:
   ```bash
   git push origin feature/new-feature
   ```
7. **Create pull request**

### Pull Request Guidelines

- **Title**: Clear, descriptive title
- **Description**: Detailed description of changes
- **Tests**: All tests must pass
- **Documentation**: Update relevant documentation
- **Examples**: Add examples for new features

### Code Review Process

1. **Automated checks**: All CI checks must pass
2. **Code review**: At least one reviewer approval required
3. **Testing**: Manual testing of new features
4. **Documentation**: Documentation must be updated

## 🚀 Release Process

### Version Numbering

We use semantic versioning (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

### Release Steps

1. **Update version** in `setup.py` and `__init__.py`
2. **Update changelog** with new features and fixes
3. **Run full test suite**:
   ```bash
   python -m pytest tests/ -v
   python -m pytest tests/integration/ -v
   ```
4. **Create release tag**:
   ```bash
   git tag -a v1.1.2 -m "Release version 1.1.2"
   git push origin v1.1.2
   ```
5. **Build and upload** to PyPI:
   ```bash
   python -m build
   python -m twine upload dist/*
   ```

### Release Checklist

- [ ] All tests pass
- [ ] Documentation is updated
- [ ] Version numbers are updated
- [ ] Changelog is updated
- [ ] Release notes are written
- [ ] Tag is created and pushed
- [ ] Package is uploaded to PyPI

## 📊 Performance Considerations

### Memory Usage

- **Batch processing**: Use appropriate batch sizes
- **Streaming**: Process large files in chunks
- **Cleanup**: Properly close file handles and database connections

### CPU Usage

- **Parallel processing**: Consider multiprocessing for large files
- **Optimization**: Profile code and optimize bottlenecks
- **Caching**: Cache frequently accessed data

### I/O Optimization

- **SSD storage**: Use SSD for better I/O performance
- **Buffer sizes**: Optimize buffer sizes for your system
- **Compression**: Consider compression for large datasets

## 🔍 Debugging

### Debug Tools

- **pdb**: Python debugger
- **logging**: Comprehensive logging
- **profiling**: cProfile for performance analysis
- **memory profiling**: memory_profiler for memory usage

### Debug Configuration

```python
import logging
import pdb

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Set breakpoint
pdb.set_trace()
```

### Common Debug Scenarios

1. **Parsing errors**: Check file format and structure
2. **Memory issues**: Monitor memory usage and batch sizes
3. **Performance issues**: Profile code and identify bottlenecks
4. **Data corruption**: Verify input files and conversion logic

## 📚 Additional Resources

- [Python Style Guide (PEP 8)](https://pep8.org/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [pytest Documentation](https://docs.pytest.org/)
- [Black Code Formatter](https://black.readthedocs.io/)
- [mypy Type Checker](https://mypy.readthedocs.io/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
