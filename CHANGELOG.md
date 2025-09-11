# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2025-01-11

### Added
- **Multidimensional Array Support**: Advanced handling of TopSpeed multidimensional arrays
  - Automatic detection of single-field arrays (e.g., 96-byte fields with multiple elements)
  - Automatic detection of multi-field arrays (e.g., PROD1, PROD2, PROD3 fields)
  - JSON storage in SQLite for array data
  - Support for all TopSpeed data types in arrays (DOUBLE, BYTE, STRING, etc.)
- **Data Type Preservation**: Enhanced data integrity features
  - Distinction between null values and zero values in DOUBLE fields
  - Boolean conversion for BYTE arrays (true/false instead of raw bytes)
  - Proper handling of missing vs. actual zero values
- **MultidimensionalHandler Class**: New dedicated class for array processing
  - `analyze_table_structure()` method for automatic array detection
  - `create_sqlite_schema()` method for JSON schema generation
  - `ArrayFieldInfo` dataclass for array metadata
- **Comprehensive Testing**: Added 23 new unit tests for multidimensional functionality
- **Documentation**: Updated README and API docs with multidimensional examples
- **Example Script**: New `multidimensional_arrays.py` example demonstrating JSON querying

### Changed
- **Schema Generation**: Enhanced to automatically detect and handle arrays
- **Data Migration**: Improved to use raw record access for array fields
- **Field Parsing**: Updated to handle array elements with proper data type conversion
- **Test Coverage**: Increased from 88 to 222 tests

### Technical Details
- Uses TopSpeed `array_element_count` attribute for authoritative array detection
- Implements both single-field and multi-field array patterns
- Raw binary parsing for accurate data extraction
- JSON storage enables SQLite's native JSON functions for querying
- Backward compatible with existing non-array tables

## [1.0.2] - 2025-01-10

### Fixed
- Corrected project naming consistency (pytopspeed_modernized vs phdwin_reader)
- Updated all build files and documentation references
- Fixed GitHub repository references

## [1.0.1] - 2025-01-10

### Fixed
- Corrected .phz file conversion to properly append phd_ and mod_ prefixes
- Updated contact information in build files
- Removed Docker-related configurations

## [1.0.0] - 2025-01-10

### Added
- Initial release of Pytopspeed Modernized
- Support for .phd, .mod, .tps, and .phz file formats
- Combined conversion of multiple TopSpeed files
- Reverse conversion from SQLite back to TopSpeed
- CLI interface and Python API
- Comprehensive error handling and progress tracking
- 88 unit tests and integration tests
