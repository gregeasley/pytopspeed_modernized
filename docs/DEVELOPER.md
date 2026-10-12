# Developer Documentation

This document provides information for developers working on the Pytopspeed Modernized library.

## 📚 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Code Structure](#code-structure)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Validating Writer Output](#validating-writer-output)
- [Code Style](#code-style)
- [Continuous Integration](#continuous-integration)
- [Extension Guidelines](#extension-guidelines)
- [Contributing](#contributing)
- [Release Process](#release-process)

## 🏗️ Architecture Overview

### High-Level Architecture

```
                 convert                                   reverse
TopSpeed files  ────────▶  pytopspeed reader  ──▶  SqliteConverter  ──▶  SQLite database
(.phd/.mod/.tps)           (src/pytopspeed)        + RecordCodec          (data tables +
     ▲                                             + FileCapture           _topspeed_* metadata)
     │                                                                          │
     └──────────── pytopspeed.tpswriter ◀── ReverseConverter ◀─────────────────┘
                   (pages, B-tree, header)     + RecordCodec (encode)
```

### Component Overview

1. **TopSpeed reader** (`src/pytopspeed/`): parses the file header, pages, records and table definitions
2. **TopSpeed writer** (`src/pytopspeed/tpswriter.py`): builds a complete TopSpeed file from raw records
   (prefix- and RLE-compressed pages, B-tree control pages, header/block table), plus builders for data,
   memo, metadata and index records
3. **Record codec** (`src/converter/record_codec.py`): one column layout and one decode/encode path per
   table, shared by the forward and reverse converters
4. **TopSpeed metadata** (`src/converter/topspeed_metadata.py`): stores what the reverse converter needs in
   hidden `_topspeed_*` tables during forward conversion
5. **SQLite converter** (`src/converter/sqlite_converter.py`): forward conversion (`convert`, `convert_multiple`)
6. **PHZ converter** (`src/converter/phz_converter.py`): extracts `.phz` (zip) archives and converts their contents
7. **Reverse converter** (`src/converter/reverse_converter.py`): rebuilds the original TopSpeed file(s) from
   a converted (and possibly edited) database
8. **CLI** (`src/cli/main.py`): `pytopspeed convert | reverse | list`

### Forward Data Flow

```
TopSpeed file → TPS reader → table definition → RecordCodec column layout → CREATE TABLE / indexes
                                                → decode each record      → INSERT rows
                           → FileCapture        → _topspeed_file/_table/_record/_row/_memo
```

Tables whose definition parses go through `RecordCodec`. A table whose definition can't be parsed falls
back to the older multidimensional/enhanced parsing path (`multidimensional_handler.py` and the
`_create_enhanced_table_definition` code in `sqlite_converter.py`); such tables are written back unchanged
by the reverse converter.

### Reverse Data Flow

`ReverseConverter.convert_sqlite_to_topspeed(sqlite_file, output_dir)` reads `_topspeed_file` to find the
source files, then for each table compares the SQLite rows (matched by rowid, falling back to content)
with the stored original payloads: unchanged rows are copied byte for byte, edited rows are re-encoded
(only the changed columns), inserted rows get new record numbers, and deleted rows and their memos are
dropped. Index entries and record-count metadata records are regenerated, then `tpswriter.build_tps`
writes the file under its original name.

## 📁 Code Structure

### Directory Layout

```
phdwin_reader/
├── src/
│   ├── pytopspeed/                  # TopSpeed reader and writer
│   │   ├── tps.py                   # TPS class: header, pages, table iteration
│   │   ├── tpspage.py               # Page header / decompression
│   │   ├── tpsrecord.py             # Record parsing
│   │   ├── tpstable.py              # Table definitions (portions joined in portion order)
│   │   ├── tpscrypt.py              # Encryption/decryption
│   │   ├── tpswriter.py             # TopSpeed file writer
│   │   └── utils.py
│   ├── converter/
│   │   ├── sqlite_converter.py      # TopSpeed -> SQLite
│   │   ├── phz_converter.py         # .phz archives
│   │   ├── reverse_converter.py     # SQLite -> TopSpeed
│   │   ├── record_codec.py          # Column layout, decode/encode of records
│   │   ├── topspeed_metadata.py     # _topspeed_* metadata tables
│   │   ├── schema_mapper.py         # Name sanitizing, type mapping (fallback path)
│   │   ├── multidimensional_handler.py  # Array detection (fallback path)
│   │   ├── batch_processor.py, data_validator.py, performance_optimizer.py,
│   │   ├── robust_converter.py, error_handler.py,
│   │   └── resilience_config.py, resilience_enhancements.py
│   └── cli/main.py                  # CLI entry point (console script `pytopspeed`)
├── tests/
│   ├── conftest.py                  # Shared fixtures, markers, autouse mocks
│   ├── unit/                        # Unit tests (run by CI) + tps_builder.py
│   ├── integration/                 # Integration tests
│   └── performance/                 # Performance tests (skipped by default)
├── examples/                        # Example scripts
├── docs/
├── assets/                          # TxWells sample files (local checks only)
├── build_package.py, release.py     # Packaging helpers
└── setup.py, setup.cfg, pyproject.toml
```

### Key Classes and Their Responsibilities

#### TPS (TopSpeed reader)
- **File**: `src/pytopspeed/tps.py`
- **Purpose**: Main interface for reading TopSpeed files
- **Key Methods**: `__init__()`, `set_current_table()`, `__iter__()`

#### RecordCodec
- **File**: `src/converter/record_codec.py`
- **Purpose**: Builds the column layout for a table definition and converts records both ways
- **Key Methods**: `data_columns`, `memo_columns`, `index_columns()`, `decode()`, `encode()`, `encode_memo()`
- **Layout rules**: scalar field → column named after the field without its `PRE:` prefix; `DIM(n)` field →
  JSON array column; members of a dimensioned `GROUP` → one JSON array per member (the group itself gets no
  column); numbered scalars `NAME1..NAMEn` (suffixes exactly 1..n, same type, evenly spaced) → one JSON
  array `NAME`; each memo → its own TEXT column. Columns follow definition order.

#### SqliteConverter
- **File**: `src/converter/sqlite_converter.py`
- **Purpose**: Converts TopSpeed data to SQLite
- **Key Methods**: `convert()`, `convert_multiple()` (prefixes tables `phd_` / `mod_` / `tps_` when several
  files go into one database)

#### PhzConverter
- **File**: `src/converter/phz_converter.py`
- **Key Methods**: `convert_phz()`, `list_phz_contents()`

#### ReverseConverter
- **File**: `src/converter/reverse_converter.py`
- **Key Method**: `convert_sqlite_to_topspeed(sqlite_file, output_dir)` → dict with `success`,
  `files_created`, `tables_processed`, `records_processed`, `duration`, `errors`, `warnings`
- Requires a database created by this version (it needs the `_topspeed_*` tables)

#### tpswriter
- **File**: `src/pytopspeed/tpswriter.py`
- **Key Functions**: `build_tps()`, `data_record()`, `memo_records()`, `metadata_record()`,
  `index_records()`, `index_key_bytes()`, `rle_compress()` / `rle_decompress()`, `read_raw_records()`
- See [phd_format_analysis.md](phd_format_analysis.md) for the file format details the writer implements.

## 🛠️ Development Setup

### Prerequisites

- Python 3.8+ (CI tests 3.8 to 3.12)
- Git

### Setup Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/gregeasley/pytopspeed_modernized
   cd pytopspeed_modernized
   ```

2. **Create an environment** (conda or venv), then **install**:
   ```bash
   pip install -r requirements.txt
   pip install -e .            # installs the `pytopspeed` console script
   pip install -e .[dev]       # optional: pytest, pytest-cov, black, flake8, mypy
   ```

3. **Verify setup**:
   ```bash
   python -m pytest tests/unit -q
   python src/cli/main.py --help     # or `pytopspeed --help` after `pip install -e .`
   ```

Global options go before the subcommand: `python src/cli/main.py -v convert in.phd out.sqlite`.

## 🧪 Testing

### Test Structure

```
tests/
├── conftest.py              # fixtures, markers, --run-performance option, autouse mocks
├── unit/                    # 358 tests; the only suite CI runs
│   ├── tps_builder.py       # builds synthetic TopSpeed files for tests
│   ├── test_tpswriter.py, test_record_codec.py, test_reverse_converter.py,
│   ├── test_array_schema_consistency.py, test_tps_parser.py, test_sqlite_converter*.py, ...
├── integration/             # 18 tests
└── performance/             # 14 tests, skipped unless --run-performance
```

### Running Tests

```bash
# Unit + integration (376 tests)
python -m pytest tests/unit tests/integration

# Unit tests only (what CI runs)
python -m pytest tests/unit

# Only tests that use the real TopSpeed reader/writer
python -m pytest tests/unit -m real_topspeed

# Performance tests
python -m pytest tests/performance --run-performance

# Coverage (needs pytest-cov)
python -m pytest tests/unit --cov=src --cov-report=html
```

`pyproject.toml` sets `--strict-markers`, so new markers must be registered (see `pytest_configure` in
`tests/conftest.py`: `performance`, `slow`, `memory`, `real_topspeed`; plus `slow`, `integration`, `unit`
in `pyproject.toml`).

### Mocks and the `real_topspeed` Marker

`tests/conftest.py` has an autouse fixture (`mock_external_dependencies`) that replaces `TPS.__init__` and
`TpsRecordsList` with mocks for every test, so most converter tests never touch a real file. Tests that
read or write real TopSpeed files must opt out with:

```python
pytestmark = pytest.mark.real_topspeed
```

(as `test_tpswriter.py` and `test_reverse_converter.py` do).

### Test Data: Synthetic Only

Tests must use synthetic data. The TxWells sample files in `assets/` are not shipped and are not used by CI.
Build test files with `tests/unit/tps_builder.py`, which assembles a TopSpeed file from table definitions,
rows, indexes and memos using the writer's record builders:

```python
from tps_builder import build_file, field, index, memo, table_definition
```

`build_file(path, tables)` writes the file; `field()`, `memo()`, `index()` and `table_definition()` describe
the schema; `wells_table()` / `well_payload()` provide a ready-made example table.

Some older integration scripts still reference `assets/TxWells.*`:
`tests/integration/test_end_to_end_conversion.py` is not collected by pytest (its class defines
`__init__`) and is a manual script; `test_mod_conversion.py` and `test_performance.py` run under the
conftest mocks. Use the TxWells files only for local manual checks, never in committed tests.

## ✅ Validating Writer Output

Unit tests check the writer against the reader, but the reader is more forgiving than Clarion/PHDWin.
The practical local oracle for "will PHDWin accept this file" is the 32-bit SoftVelocity TopSpeed ODBC
driver (`c4otrx.dll` read-only, `c4otsx.dll` read-write). It is 32-bit only, so use 32-bit PowerShell
(`%WINDIR%\SysWOW64\WindowsPowerShell\v1.0\powershell.exe`) and `System.Data.Odbc`:

```powershell
param([string]$File, [string]$Table)
$conn = New-Object System.Data.Odbc.OdbcConnection("Driver={SoftVelocity Topspeed driver Read-Only (*.tps)};DBQ=$File\!;Extension=phd")
$conn.Open()
$conn.GetSchema('Tables') | Select-Object -ExpandProperty TABLENAME
$cmd = $conn.CreateCommand()
$cmd.CommandText = "SELECT * FROM `"$Table`""
$reader = $cmd.ExecuteReader()
$rows = 0
while ($reader.Read()) { $rows++ }
$reader.Close()
"$Table rows: $rows"
$conn.Close()
```

Notes:
- The table list column from `GetSchema('Tables')` is `TABLENAME`.
- `ExecuteScalar()` with `COUNT(*)` crashes .NET with this driver; count with a data reader as above.
- Quote reserved-word table names such as `ORDER` and `USER`.
- The driver crashes on files whose header block table mixes page sizes within a block (see the format reference).

Final check in PHDWin itself. **Gotcha**: once PHDWin fails to open a database it keeps reporting
"Version file is UNAVAILABLE ... File System Error" for every later open, including valid files, until
PHDWin is restarted. Restart PHDWin before concluding that a file is bad.

## 🎨 Code Style

- PEP 8; Black is configured in `pyproject.toml` (`black src/ tests/`)
- Docstrings: Google style; type hints on public functions
- flake8 and mypy are available via the `dev` extra:

```bash
flake8 src/ --count --select=E9,F63,F7,F82 --show-source --statistics   # the blocking CI check
mypy src/ --ignore-missing-imports                                       # report-only in CI
```

## 🔁 Continuous Integration

GitHub Actions (`.github/workflows/ci.yml`) runs on pushes to `master`/`develop`, pull requests to
`master`, and published releases:

- **test** (ubuntu-24.04, Python 3.8, 3.9, 3.10, 3.11, 3.12): flake8 syntax/undefined-name errors
  (E9, F63, F7, F82) are blocking; the full flake8 report is informational; mypy runs but does not fail the
  build (the codebase has a large number of pre-existing type errors); `pytest tests/unit/` with coverage,
  uploaded to Codecov (non-blocking).
- **build**: `python -m build` and `twine check dist/*`.
- **publish-testpypi**: on pull requests, uploads to TestPyPI with `--skip-existing`.
- **publish-pypi**: on published releases, uploads to PyPI.

Integration and performance tests are not run in CI.

## 🔧 Extension Guidelines

### Adding a Field Type

Field types are defined in `FIELD_TYPE_STRUCT` (`src/pytopspeed/tpstable.py`). To support a new type in
the conversion path:

1. Add decoding and encoding in `RecordCodec.decode_element()` / `encode_element()`
   (`src/converter/record_codec.py`) and the SQLite column type to `SQLITE_TYPES`.
2. If the type can be an index key field, add its key encoding to `index_key_bytes()` in
   `src/pytopspeed/tpswriter.py` and verify it against a file written by TopSpeed; until then add nothing to
   `VERIFIED_KEY_TYPES` (the reverse converter warns when an edited table uses unverified key types).
3. Add round-trip tests using `tests/unit/tps_builder.py`.

### Adding a CLI Command

Add a subparser and handler in `src/cli/main.py`, add tests, and update `docs/` and `README.md`.

## 🤝 Contributing

1. Create a feature branch from `master`.
2. Make changes, with tests (synthetic data only) and documentation updates.
3. Run `python -m pytest tests/unit tests/integration` and the blocking flake8 check above.
4. Open a pull request to `master`; CI must pass.

## 🚀 Release Process

### Version Numbering

Semantic versioning (MAJOR.MINOR.PATCH). The current version is 1.2.0. Changes not yet in a release go
under `[Unreleased]` in `CHANGELOG.md`.

The version string is duplicated and must be updated in all of:
`setup.py`, `setup.cfg`, `pyproject.toml`, `src/pytopspeed/__init__.py` and `src/cli/__init__.py`
(the CLI's `--version` reads `cli.__version__`).

### Release Steps

1. Update the version in the files above and move the `[Unreleased]` entries in `CHANGELOG.md` under the
   new version.
2. Run the tests: `python -m pytest tests/unit tests/integration`.
3. Either publish a GitHub release (CI builds and uploads to PyPI), or run the helper script locally:
   ```bash
   python release.py testpypi   # check git status, run tests, build, twine check, upload to TestPyPI
   python release.py pypi       # same, upload to PyPI, then tag v<version> and push tags
   ```
   `build_package.py` deletes `build/`, `dist/` and `*.egg-info`, then builds the package without uploading.

## 🔍 Debugging

- Run the CLI with `-v` (before the subcommand) for debug logging.
- `pytopspeed.tpswriter.read_raw_records(tps)` returns every raw record of an open file as
  `(header size, bytes)` pairs, which is useful for diffing an original file against a rebuilt one.
- For files that the reader accepts but PHDWin rejects, check with the ODBC driver (above).

## 📚 Additional Resources

- [TopSpeed / PHD format reference](phd_format_analysis.md)
- [pytest Documentation](https://docs.pytest.org/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
