# Troubleshooting Guide

This guide helps you diagnose and resolve common issues when using the Pytopspeed Modernized library.

Commands below use the installed `pytopspeed` console script. From a source checkout without installing,
use `python src/cli/main.py` instead.

## 🔍 General Debugging

### Enable Verbose Logging

Global options (`-v/--verbose`, `-q/--quiet`) go **before** the subcommand:

```bash
pytopspeed -v convert input.phd output.sqlite
```

`pytopspeed convert input.phd output.sqlite --verbose` fails with `unrecognized arguments: --verbose`.

Verbose output shows file processing steps, table parsing details, conversion progress, and error context.

### Check File Permissions

Ensure you can read the input and write the output location, and that the files are not open in another
program (PHDWin, an SQLite browser, etc.).

## 🚨 Common Error Messages

### 1. "stream read less than specified amount"

**Error**: `stream read less than specified amount, expected X, found Y`

**Causes**:
- The input is not a TopSpeed file (for example a wrong file or an unsupported extension). There is no
  separate "unsupported file type" check: a non-TopSpeed file typically fails while parsing the header, e.g.
  `Error in path (parsing) -> offset`.
- A table definition that can't be parsed. That table falls back to the older parsing path; the rest of the
  conversion continues.

**Solutions**:
- Verify the input is a `.phd`, `.mod`, `.tps` or `.phz` file produced by PHDWin/Clarion
- Check the log for which tables are affected

### 2. "File not found" Errors

**Error**: for `.phd/.mod/.tps` input the error is just the file name (for example `Error: nofile.phd`);
for `.phz` input it is `Input file not found: filename`.

**Solutions**:
- Verify the path; use an absolute path if a relative one doesn't resolve
- Ensure the file is not locked by another application

### 3. Mixing .phz with Other Files

**Error**: `Cannot mix .phz files with other file types in a single conversion` or
`Only one .phz file can be converted at a time`

**Solution**: Convert a `.phz` on its own. Several `.phd/.mod/.tps` files can be converted together into
one database; their tables are prefixed `phd_`, `mod_` and `tps_`.

### 4. "Permission denied" / "database is locked"

**Solutions**:
- Close applications using the input file or the SQLite database
- Ensure the output directory is writable
- Use a different output filename

## 🔧 Conversion Issues

### The SQLite Database Is Much Larger Than Before

**Expected.** The database now includes hidden `_topspeed_*` tables (`_topspeed_file`, `_topspeed_table`,
`_topspeed_record`, `_topspeed_row`, `_topspeed_memo`) holding the original records, which reverse
conversion needs. For example, TxWells.PHD converts to about 37 MB, compared with about 7 MB from earlier
versions. Don't drop these tables if you intend to convert the database back.

### `-q` Doesn't Silence Output / Log Lines Appear Twice

**Known issue.** `-q/--quiet` only suppresses the progress display; log messages are still printed, and
some log lines appear twice (once with milliseconds in the timestamp, once without). This is cosmetic.
To keep the output short, redirect it to a file:

```bash
pytopspeed -q convert input.phd output.sqlite > convert.log 2>&1
```

### Warning: "Failed to clean up temporary directory ... phz_extract_..."

**Known issue on Windows.** When converting a `.phz`, the extracted files are still open when the converter
tries to delete its temporary folder, so it logs:

```
WARNING - Failed to clean up temporary directory C:\Users\...\Temp\phz_extract_xxxxxxxx: [WinError 32] ...
```

The conversion itself succeeded. Delete the leftover `phz_extract_*` folder from your temp directory
manually if needed.

### Values Look Different From Older Versions

The column layout is derived from each table's definition:
- `DIM(n)` fields, members of dimensioned GROUPs, and numbered fields `NAME1..NAMEn` become one TEXT column
  holding a JSON array
- BYTE fields are integers (not true/false)
- DATE is `YYYY-MM-DD` (NULL when zero), TIME is `HH:MM:SS.cc`
- NaN/infinity become NULL (`null` inside arrays)
- Column names drop the Clarion `PRE:` prefix

### Slow Conversion or Memory Issues

- Try a smaller batch size: `pytopspeed convert --batch-size 500 input.phd output.sqlite`
- Use local (not network) storage with enough free space for the larger database
- Convert files individually instead of combined

## 🔄 Reverse Conversion Issues

Reverse conversion: `pytopspeed reverse input.sqlite output_dir`. Output files keep their original names
(for example `TxWells.PHD`, `TxWells.mod`); a database made from a `.phz` produces both files (they are not
re-zipped). If an error occurs, no file is written for that source file.

### "This database has no TopSpeed metadata (_topspeed_* tables)"

**Cause**: the database was created by an older version, or the `_topspeed_*` tables were removed.

**Solution**: convert the original TopSpeed file again with this version, make your changes in that new
database, then reverse it.

### "... have the same value for unique key ..."

**Error**: `table X: two rows (record numbers A and B) have the same value for unique key KEYNAME (columns)`

**Cause**: an edit or insert gave two rows the same value in a TopSpeed unique key.

**Solution**: change one of the rows so the key columns are unique, then run the reverse conversion again.

### "cannot store ..." / "does not fit in DECIMAL" / "expected a JSON array of up to ..."

**Error**: `table X, record N: COLUMN: ...`

**Cause**: an edited value doesn't fit the TopSpeed field, for example an array column with more elements
than the field's `DIM`, a number too large for a DECIMAL, or text that isn't a number in a numeric column.

**Solution**: correct the named column in the named table. Array columns must stay JSON arrays.

### Warning: "... was converted without a field layout, so changes to it can't be written back"

**Cause**: the table's definition couldn't be parsed during forward conversion, so it went through the
fallback path. Such tables are written back with their original records; edits to them are ignored.

### Warning: "key encoding for ... has not been checked against TopSpeed"

**Cause**: you edited a table with an index on a field type whose key encoding hasn't been verified (only
BYTE, SHORT, LONG, DOUBLE and STRING are). Open the result in PHDWin (or check it with the ODBC driver, see
[DEVELOPER.md](DEVELOPER.md#validating-writer-output)) before relying on it.

### Edits Seem To Have Been Treated As New Records

Rows are matched to their original records by SQLite rowid. If rowids changed (for example `VACUUM` after
deletes, or a recreated table), unchanged rows are still matched by content, but edited rows are written as
new records with new record numbers.

### PHDWin: "Version file is UNAVAILABLE ... File System Error"

**Cause**: after PHDWin fails to open one database, it keeps showing this error for every database it
opens afterwards, including valid ones.

**Solution**: restart PHDWin and open the file again before concluding the file is bad.

## 🧪 Testing and Validation

### Verify Conversion Results

```bash
# List tables (hidden _topspeed_* tables included)
sqlite3 output.sqlite ".tables"

# Count and sample rows
sqlite3 output.sqlite "SELECT COUNT(*) FROM OWNER;"
sqlite3 output.sqlite "SELECT * FROM OWNER LIMIT 5;"
```

In a combined database the tables are prefixed (`phd_OWNER`, ...). To compare against the original file,
read it with the SoftVelocity TopSpeed ODBC driver (see [DEVELOPER.md](DEVELOPER.md#validating-writer-output)).

## 🆘 Getting Additional Help

When reporting issues, include:

1. **Versions**:
   ```bash
   pytopspeed --version
   python --version
   ```
2. **Complete output**:
   ```bash
   pytopspeed -v convert input.phd output.sqlite > debug.log 2>&1
   ```
3. **Test results**: `python -m pytest tests/unit -q`
4. A minimal reproduction if possible (smallest input file, single table, exact steps). Don't attach client
   data to public issues.
