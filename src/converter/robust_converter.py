"""
Robust TopSpeed to SQLite Converter with Error Handling

This module provides a robust conversion system with comprehensive error handling,
recovery mechanisms, and detailed reporting.
"""

import logging
import sqlite3
import os
import tempfile
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from pytopspeed.tps import TPS
from .error_handler import ErrorHandler, ErrorCategory, ErrorSeverity, ConversionError
from .schema_mapper import TopSpeedToSQLiteMapper
from .sqlite_converter import SqliteConverter


class RobustConverter:
    """
    Robust TopSpeed to SQLite converter with comprehensive error handling
    """
    
    def __init__(self, error_handler: Optional[ErrorHandler] = None):
        """Initialize robust converter"""
        self.logger = logging.getLogger(__name__)
        self.error_handler = error_handler or ErrorHandler()
        self.schema_mapper = TopSpeedToSQLiteMapper()
        self.data_converter = SqliteConverter()
        
        # Conversion state
        self.conversion_stats = {
            "tables_processed": 0,
            "tables_failed": 0,
            "records_processed": 0,
            "records_failed": 0,
            "start_time": None,
            "end_time": None
        }
        
        # Configuration
        self.enable_partial_conversion = True
        self.max_retry_attempts = 3
        self.batch_size = 1000
        
    def convert_file(self, input_file: str, output_file: str, 
                    options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Convert TopSpeed file to SQLite with robust error handling
        
        Args:
            input_file: Path to TopSpeed file (.phd, .mod, .phz)
            output_file: Path to output SQLite file
            options: Conversion options
            
        Returns:
            Conversion result with statistics and error information
        """
        options = options or {}
        result = {
            "success": False,
            "input_file": input_file,
            "output_file": output_file,
            "stats": self.conversion_stats.copy(),
            "errors": [],
            "warnings": [],
            "partial_success": False
        }
        
        try:
            self.logger.info(f"Starting robust conversion: {input_file} -> {output_file}")
            self.conversion_stats["start_time"] = self.error_handler.errors[0].timestamp if self.error_handler.errors else None
            
            # Validate input file
            if not self._validate_input_file(input_file):
                raise ConversionError(f"Invalid input file: {input_file}")
            
            # Create backup if requested
            if options.get("create_backup", True):
                backup_path = self.error_handler.create_backup(input_file)
                if backup_path:
                    result["backup_file"] = backup_path
            
            # Create checkpoint
            checkpoint_data = {
                "input_file": input_file,
                "output_file": output_file,
                "options": options,
                "stats": self.conversion_stats.copy()
            }
            checkpoint_path = self.error_handler.create_checkpoint("conversion_start", checkpoint_data)
            if checkpoint_path:
                result["checkpoint_file"] = checkpoint_path
            
            # Perform conversion based on file type
            if input_file.lower().endswith('.phz'):
                conversion_result = self._convert_phz_file(input_file, output_file, options)
            else:
                conversion_result = self._convert_single_file(input_file, output_file, options)
            
            result.update(conversion_result)
            result["success"] = True
            
        except ConversionError as e:
            self.error_handler.log_error(
                ErrorCategory.CONVERSION,
                ErrorSeverity.ERROR,
                f"Conversion failed: {e}",
                e.details,
                e
            )
            result["errors"].append(str(e))
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.SYSTEM,
                ErrorSeverity.CRITICAL,
                f"Unexpected error during conversion: {e}",
                {"exception_type": type(e).__name__},
                e
            )
            result["errors"].append(f"Unexpected error: {e}")
        
        finally:
            self.conversion_stats["end_time"] = self.error_handler.errors[-1].timestamp if self.error_handler.errors else None
            result["stats"] = self.conversion_stats.copy()
            result["error_summary"] = self.error_handler.get_error_summary()
            
            # Generate error report if there were errors
            if self.error_handler.errors:
                error_report = self.error_handler.generate_error_report()
                if error_report:
                    result["error_report"] = error_report
        
        return result
    
    def _validate_input_file(self, input_file: str) -> bool:
        """Validate input file exists and is accessible"""
        try:
            if not os.path.exists(input_file):
                self.error_handler.log_error(
                    ErrorCategory.FILE_ACCESS,
                    ErrorSeverity.ERROR,
                    f"Input file does not exist: {input_file}"
                )
                return False
            
            if not os.access(input_file, os.R_OK):
                self.error_handler.log_error(
                    ErrorCategory.FILE_ACCESS,
                    ErrorSeverity.ERROR,
                    f"Input file is not readable: {input_file}"
                )
                return False
            
            return True
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.FILE_ACCESS,
                ErrorSeverity.ERROR,
                f"Error validating input file: {input_file}",
                {"exception": str(e)},
                e
            )
            return False
    
    def _convert_single_file(self, input_file: str, output_file: str, 
                           options: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single TopSpeed file"""
        result = {"tables_processed": [], "tables_failed": []}
        
        try:
            # Initialize TopSpeed file
            tps = self._initialize_topspeed_file(input_file)
            if not tps:
                raise ConversionError("Failed to initialize TopSpeed file")
            
            # Create SQLite database
            conn = self._create_sqlite_database(output_file, options)
            if not conn:
                raise ConversionError("Failed to create SQLite database")
            
            # Process tables
            for table_name in tps.tables:
                try:
                    table_result = self._convert_table_robust(tps, table_name, conn, options)
                    if table_result["success"]:
                        result["tables_processed"].append(table_name)
                        self.conversion_stats["tables_processed"] += 1
                    else:
                        result["tables_failed"].append({
                            "table": table_name,
                            "error": table_result["error"]
                        })
                        self.conversion_stats["tables_failed"] += 1
                        
                except Exception as e:
                    self.error_handler.log_error(
                        ErrorCategory.CONVERSION,
                        ErrorSeverity.ERROR,
                        f"Failed to convert table {table_name}",
                        {"table": table_name, "exception": str(e)},
                        e
                    )
                    result["tables_failed"].append({
                        "table": table_name,
                        "error": str(e)
                    })
                    self.conversion_stats["tables_failed"] += 1
            
            conn.close()
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.CONVERSION,
                ErrorSeverity.ERROR,
                f"Error in single file conversion",
                {"input_file": input_file, "exception": str(e)},
                e
            )
            raise ConversionError(f"Single file conversion failed: {e}")
        
        return result
    
    def _convert_phz_file(self, input_file: str, output_file: str, 
                         options: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a PHZ (zip) file containing TopSpeed files"""
        result = {"files_processed": [], "files_failed": []}
        
        try:
            import zipfile
            
            with zipfile.ZipFile(input_file, 'r') as zip_file:
                # Extract files to temporary directory
                temp_dir = tempfile.mkdtemp(prefix="phdwin_phz_")
                
                try:
                    zip_file.extractall(temp_dir)
                    
                    # Find TopSpeed files
                    phd_files = []
                    mod_files = []
                    
                    for root, dirs, files in os.walk(temp_dir):
                        for file in files:
                            if file.lower().endswith('.phd'):
                                phd_files.append(os.path.join(root, file))
                            elif file.lower().endswith('.mod'):
                                mod_files.append(os.path.join(root, file))
                    
                    # Convert files
                    conn = self._create_sqlite_database(output_file, options)
                    if not conn:
                        raise ConversionError("Failed to create SQLite database")
                    
                    # Process PHD files
                    for phd_file in phd_files:
                        try:
                            file_result = self._convert_file_to_database(phd_file, conn, "phd_", options)
                            result["files_processed"].append({"file": phd_file, "type": "phd"})
                        except Exception as e:
                            result["files_failed"].append({
                                "file": phd_file,
                                "type": "phd",
                                "error": str(e)
                            })
                    
                    # Process MOD files
                    for mod_file in mod_files:
                        try:
                            file_result = self._convert_file_to_database(mod_file, conn, "mod_", options)
                            result["files_processed"].append({"file": mod_file, "type": "mod"})
                        except Exception as e:
                            result["files_failed"].append({
                                "file": mod_file,
                                "type": "mod",
                                "error": str(e)
                            })
                    
                    conn.close()
                    
                finally:
                    # Clean up temporary directory
                    import shutil
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.CONVERSION,
                ErrorSeverity.ERROR,
                f"Error in PHZ file conversion",
                {"input_file": input_file, "exception": str(e)},
                e
            )
            raise ConversionError(f"PHZ file conversion failed: {e}")
        
        return result
    
    def _initialize_topspeed_file(self, file_path: str) -> Optional[TPS]:
        """Initialize TopSpeed file with error handling"""
        try:
            tps = TPS(file_path)
            return tps
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.FILE_ACCESS,
                ErrorSeverity.ERROR,
                f"Failed to initialize TopSpeed file: {file_path}",
                {"file_path": file_path, "exception": str(e)},
                e
            )
            return None
    
    def _create_sqlite_database(self, output_file: str, options: Dict[str, Any]) -> Optional[sqlite3.Connection]:
        """Create SQLite database with error handling"""
        try:
            # Remove existing file if it exists
            if os.path.exists(output_file):
                os.remove(output_file)
            
            conn = sqlite3.connect(output_file)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            return conn
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.DATABASE_OPERATION,
                ErrorSeverity.ERROR,
                f"Failed to create SQLite database: {output_file}",
                {"output_file": output_file, "exception": str(e)},
                e
            )
            return None
    
    def _convert_table_robust(self, tps: TPS, table_name: str, conn: sqlite3.Connection,
                            options: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single table with robust error handling"""
        result = {"success": False, "records_processed": 0, "records_failed": 0}
        
        try:
            # Set current table
            tps.set_current_table(table_name)
            table_def = tps.tables.get_definition(table_name)
            
            if not table_def:
                raise ConversionError(f"Could not get definition for table: {table_name}")
            
            # Create table schema
            schema_sql = self.schema_mapper.create_table_schema(table_def, table_name)
            conn.execute(schema_sql)
            
            # Convert data in batches
            batch_size = options.get("batch_size", self.batch_size)
            batch_data = []
            
            for record in tps:
                try:
                    # Convert record
                    converted_record = self.data_converter._convert_record_to_tuple(record, table_def)
                    batch_data.append(converted_record)
                    
                    # Process batch when full
                    if len(batch_data) >= batch_size:
                        self._insert_batch_robust(conn, table_name, batch_data, table_def)
                        result["records_processed"] += len(batch_data)
                        self.conversion_stats["records_processed"] += len(batch_data)
                        batch_data = []
                        
                except Exception as e:
                    self.error_handler.log_error(
                        ErrorCategory.DATA_PARSING,
                        ErrorSeverity.WARNING,
                        f"Failed to convert record in table {table_name}",
                        {"table": table_name, "exception": str(e)},
                        e
                    )
                    result["records_failed"] += 1
                    self.conversion_stats["records_failed"] += 1
                    
                    # Continue processing if partial conversion is enabled
                    if not self.enable_partial_conversion:
                        raise
            
            # Process remaining batch
            if batch_data:
                self._insert_batch_robust(conn, table_name, batch_data, table_def)
                result["records_processed"] += len(batch_data)
                self.conversion_stats["records_processed"] += len(batch_data)
            
            # Create indexes
            self._create_indexes_robust(conn, table_def, table_name)
            
            conn.commit()
            result["success"] = True
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.CONVERSION,
                ErrorSeverity.ERROR,
                f"Failed to convert table {table_name}",
                {"table": table_name, "exception": str(e)},
                e
            )
            result["error"] = str(e)
        
        return result
    
    def _insert_batch_robust(self, conn: sqlite3.Connection, table_name: str, 
                           batch_data: List[tuple], table_def) -> None:
        """Insert batch of data with error handling"""
        try:
            if not batch_data:
                return
            
            # Get column names
            columns = [field.name for field in table_def.fields]
            placeholders = ",".join(["?" for _ in columns])
            sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
            
            conn.executemany(sql, batch_data)
            
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.DATABASE_OPERATION,
                ErrorSeverity.ERROR,
                f"Failed to insert batch for table {table_name}",
                {"table": table_name, "batch_size": len(batch_data), "exception": str(e)},
                e
            )
            raise
    
    def _create_indexes_robust(self, conn: sqlite3.Connection, table_def, table_name: str) -> None:
        """Create indexes with error handling"""
        try:
            indexes = self.schema_mapper.create_indexes(table_def, table_name)
            for index_sql in indexes:
                try:
                    conn.execute(index_sql)
                except Exception as e:
                    self.error_handler.log_error(
                        ErrorCategory.DATABASE_OPERATION,
                        ErrorSeverity.WARNING,
                        f"Failed to create index for table {table_name}",
                        {"table": table_name, "index_sql": index_sql, "exception": str(e)},
                        e
                    )
        except Exception as e:
            self.error_handler.log_error(
                ErrorCategory.DATABASE_OPERATION,
                ErrorSeverity.WARNING,
                f"Failed to create indexes for table {table_name}",
                {"table": table_name, "exception": str(e)},
                e
            )
    
    def _convert_file_to_database(self, file_path: str, conn: sqlite3.Connection, 
                                 prefix: str, options: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a single file to existing database with prefix"""
        result = {"tables_processed": [], "tables_failed": []}
        
        try:
            tps = self._initialize_topspeed_file(file_path)
            if not tps:
                raise ConversionError(f"Failed to initialize TopSpeed file: {file_path}")
            
            for table_name in tps.tables:
                try:
                    prefixed_table_name = f"{prefix}{table_name}"
                    table_result = self._convert_table_robust(tps, prefixed_table_name, conn, options)
                    
                    if table_result["success"]:
                        result["tables_processed"].append(table_name)
                    else:
                        result["tables_failed"].append({
                            "table": table_name,
                            "error": table_result["error"]
                        })
                        
                except Exception as e:
                    result["tables_failed"].append({
                        "table": table_name,
                        "error": str(e)
                    })
            
        except Exception as e:
            raise ConversionError(f"Failed to convert file to database: {e}")
        
        return result
