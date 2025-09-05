# Troubleshooting Guide

This guide helps you diagnose and resolve common issues when using the Pytopspeed Modernized library.

## 🔍 General Debugging

### Enable Verbose Logging

Always start troubleshooting by enabling verbose logging:

```bash
python pytopspeed.py convert input.phd output.sqlite --verbose
```

This provides detailed information about:
- File processing steps
- Table parsing details
- Data conversion progress
- Error locations and context

### Check File Permissions

Ensure you have proper permissions:

```bash
# Check file permissions (Linux/macOS)
ls -la input.phd output.sqlite

# Check if files are locked (Windows)
# Use Process Explorer or Task Manager
```

## 🚨 Common Error Messages

### 1. "stream read less than specified amount"

**Error**: `stream read less than specified amount, expected X, found Y`

**Cause**: Corrupted or incomplete table definitions in the TopSpeed file

**Solutions**:
- The conversion will continue and skip problematic tables
- Check the log for which specific tables are affected
- This is often a warning, not a fatal error
- Verify the source TopSpeed file is not corrupted

**Example**:
```
2025-09-05 12:34:10 - SqliteConverter - ERROR - Error creating schema for table ENERGYADJ: 
Error in path (parsing) -> fields -> name
stream read less than specified amount, expected 1, found 0
```

### 2. "charmap codec can't decode"

**Error**: `'charmap' codec can't decode byte 0xXX in position Y: character maps to <undefined>`

**Cause**: Non-ASCII characters in text data that can't be decoded with the default encoding

**Solutions**:
- The conversion handles this gracefully by replacing problematic characters
- Data is preserved but may have encoding warnings
- This is typically a warning, not a fatal error
- Check the log for specific table and field information

**Example**:
```
2025-09-05 12:34:31 - SqliteConverter - ERROR - Error migrating data for table CURRENCYRATE: 
'charmap' codec can't decode byte 0x98 in position 4: character maps to <undefined>
```

### 3. "File not found" Errors

**Error**: `Input file not found: filename`

**Solutions**:
- Verify the file path is correct
- Check if the file exists: `ls filename` or `dir filename`
- Use absolute paths if relative paths don't work
- Ensure the file is not locked by another application

### 4. "Table name collision" Warnings

**Warning**: `Table name collision detected: TABLENAME -> prefixed_name`

**Cause**: Multiple files have tables with the same name

**Solutions**:
- This is automatically handled with prefixing
- Check the log to see which tables were affected
- The conversion will continue successfully
- Verify the prefixes are applied correctly

### 5. "Permission denied" Errors

**Error**: `[Errno 13] Permission denied: 'filename'`

**Solutions**:
- Check file permissions
- Ensure the output directory is writable
- Close any applications that might be using the files
- Run with appropriate permissions (avoid using sudo unless necessary)

### 6. "Database is locked" Errors

**Error**: `database is locked`

**Solutions**:
- Close any applications using the SQLite database
- Wait a moment and try again
- Check for zombie processes
- Use a different output filename

## 🔧 Conversion Issues

### Slow Conversion Performance

**Symptoms**: Conversion takes much longer than expected

**Solutions**:
- Reduce batch size: `--batch-size 500`
- Use SSD storage for better I/O performance
- Close other applications to free up memory
- Check available disk space
- Monitor system resources during conversion

### Memory Issues

**Symptoms**: Out of memory errors or system slowdown

**Solutions**:
- Reduce batch size: `--batch-size 250`
- Close other applications
- Use 64-bit Python if available
- Increase virtual memory/swap space
- Process files individually instead of combined

### Incomplete Data Conversion

**Symptoms**: Some records show as NULL or missing data

**Solutions**:
- Check the verbose log for data conversion errors
- Verify the source TopSpeed file is not corrupted
- Look for encoding issues in the log
- Test with a smaller subset of data
- Compare with original TopSpeed file using ODBC

### Table Name Issues

**Symptoms**: Tables have unexpected names or prefixes

**Solutions**:
- Check the log for table name sanitization details
- Verify the file type detection is working correctly
- Look for reserved word handling in the log
- Review the prefixing logic for multiple files

## 🔄 Reverse Conversion Issues

### "No PHD/MOD tables found"

**Error**: `Found 0 PHD tables, 0 MOD tables`

**Cause**: The SQLite database doesn't have tables with the expected prefixes

**Solutions**:
- Ensure the database was created with the combined conversion
- Check that tables have `phd_` or `mod_` prefixes
- Use a database created from multiple TopSpeed files
- Verify the database structure with: `sqlite3 database.db ".tables"`

### "seek out of range" Errors

**Error**: `seek out of range` when reading generated files

**Cause**: Generated TopSpeed files may not be fully compatible with the original parser

**Solutions**:
- This is a known limitation of reverse conversion
- The data is correctly transferred
- Generated files may not be readable by original TopSpeed software
- Focus on data integrity rather than file format compatibility

### Encoding Issues in Reverse Conversion

**Error**: `'ascii' codec can't encode character`

**Solutions**:
- The converter handles this automatically
- Non-ASCII characters are replaced with safe alternatives
- Check the log for specific encoding warnings
- Verify the original data doesn't contain problematic characters

## 📁 File Format Issues

### Unsupported File Types

**Error**: `Unsupported file type: .xyz`

**Solutions**:
- Verify the file extension is supported (.phd, .mod, .tps, .phz)
- Check if the file is actually a TopSpeed file
- Try renaming the file with the correct extension
- Use a file analysis tool to verify the file format

### Corrupted PHZ Files

**Error**: `Invalid .phz file (not a valid zip)`

**Solutions**:
- Verify the .phz file is not corrupted
- Try extracting it manually with a zip utility
- Check if the file was completely downloaded
- Re-download or re-create the .phz file

### Large File Handling

**Symptoms**: Very large files cause issues

**Solutions**:
- Use smaller batch sizes
- Ensure sufficient disk space (2-3x file size)
- Monitor memory usage
- Consider processing in chunks if possible

## 🧪 Testing and Validation

### Verify Conversion Results

**Check record counts**:
```bash
# Count records in original (if possible with ODBC)
# Compare with SQLite results
sqlite3 output.sqlite "SELECT COUNT(*) FROM phd_OWNER;"
```

**Verify data integrity**:
```bash
# Check for NULL values
sqlite3 output.sqlite "SELECT COUNT(*) FROM phd_OWNER WHERE field_name IS NULL;"

# Sample data
sqlite3 output.sqlite "SELECT * FROM phd_OWNER LIMIT 5;"
```

### Test with Sample Data

Always test with known good data first:

```bash
# Test with provided sample files
python pytopspeed.py convert assets/TxWells.PHD test_output.sqlite
python pytopspeed.py list assets/TxWells.phz
```

## 📊 Performance Optimization

### Batch Size Tuning

Test different batch sizes to find the optimal setting:

```bash
# Test with different batch sizes
python pytopspeed.py convert input.phd output1.sqlite --batch-size 250
python pytopspeed.py convert input.phd output2.sqlite --batch-size 500
python pytopspeed.py convert input.phd output3.sqlite --batch-size 1000
python pytopspeed.py convert input.phd output4.sqlite --batch-size 2000
```

### System Resource Monitoring

Monitor system resources during conversion:

```bash
# Monitor CPU and memory usage
# Use Task Manager (Windows), Activity Monitor (macOS), or htop (Linux)
```

### Disk I/O Optimization

- Use SSD storage for better performance
- Ensure sufficient free disk space
- Close other disk-intensive applications
- Use local storage instead of network drives

## 🆘 Getting Additional Help

### Collect Debug Information

When reporting issues, include:

1. **System Information**:
   ```bash
   python --version
   python -c "import platform; print(platform.platform())"
   ```

2. **File Information**:
   ```bash
   ls -la input_file.phd
   file input_file.phd
   ```

3. **Complete Error Output**:
   ```bash
   python pytopspeed.py convert input.phd output.sqlite --verbose 2>&1 | tee debug.log
   ```

4. **Test Results**:
   ```bash
   python -m pytest tests/unit/ -v
   ```

### Create Minimal Test Case

If possible, create a minimal test case:

1. Use the smallest possible input file
2. Test with a single table
3. Isolate the specific error
4. Document the exact steps to reproduce

### Check Known Issues

- Review the project's issue tracker
- Check for similar problems in the original pytopspeed repository
- Look for solutions in the documentation

### Contact Information

For additional help:
- Open an issue in the project repository
- Include all debug information
- Provide a minimal test case if possible
- Describe what you've already tried
