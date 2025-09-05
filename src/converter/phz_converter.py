#!/usr/bin/env python3
"""
PHZ Converter - Handle .phz files (zip files containing .phd and .mod files)

This module provides functionality to extract and convert .phz files,
which are zip archives containing TopSpeed database files.
"""

import os
import zipfile
import tempfile
import shutil
import logging
from datetime import datetime
from typing import Dict, Any, List

from .sqlite_converter import SqliteConverter


class PhzConverter:
    """
    Converter for .phz files (zip archives containing TopSpeed files)
    """
    
    def __init__(self, batch_size: int = 1000, progress_callback=None):
        """
        Initialize PHZ converter
        
        Args:
            batch_size: Number of records to process in each batch
            progress_callback: Optional callback function for progress updates
        """
        self.batch_size = batch_size
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)
        
        # Initialize the SQLite converter
        self.sqlite_converter = SqliteConverter(batch_size, progress_callback)
    
    def convert_phz(self, phz_file: str, output_file: str) -> Dict[str, Any]:
        """
        Convert .phz file (zip containing .phd and .mod files) to SQLite database
        
        Args:
            phz_file: Path to input .phz file
            output_file: Path to output SQLite file
            
        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results = {
            'success': False,
            'tables_created': 0,
            'total_records': 0,
            'duration': 0,
            'errors': [],
            'extracted_files': [],
            'phz_contents': []
        }
        
        temp_dir = None
        
        try:
            self.logger.info(f"Processing .phz file: {phz_file}")
            
            # Validate input file
            if not os.path.exists(phz_file):
                error_msg = f"Input file not found: {phz_file}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            # Create temporary directory for extraction
            temp_dir = tempfile.mkdtemp(prefix="phz_extract_")
            self.logger.info(f"Extracting to temporary directory: {temp_dir}")
            
            # Extract .phz file
            with zipfile.ZipFile(phz_file, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
                extracted_files = zip_ref.namelist()
                results['phz_contents'] = extracted_files
                self.logger.info(f"Extracted {len(extracted_files)} files from .phz")
            
            # Find .phd and .mod files in extracted contents
            phd_files = []
            mod_files = []
            other_files = []
            
            for file_name in extracted_files:
                file_path = os.path.join(temp_dir, file_name)
                if os.path.isfile(file_path):
                    file_ext = os.path.splitext(file_name)[1].lower()
                    if file_ext == '.phd':
                        phd_files.append(file_path)
                    elif file_ext == '.mod':
                        mod_files.append(file_path)
                    else:
                        other_files.append(file_name)
            
            self.logger.info(f"Found {len(phd_files)} .phd files, {len(mod_files)} .mod files, {len(other_files)} other files")
            
            if not phd_files and not mod_files:
                error_msg = "No .phd or .mod files found in .phz archive"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            # Combine all TopSpeed files
            all_files = phd_files + mod_files
            results['extracted_files'] = [os.path.basename(f) for f in all_files]
            
            # Use the existing convert_multiple method
            self.logger.info(f"Converting {len(all_files)} TopSpeed files to SQLite...")
            conversion_results = self.sqlite_converter.convert_multiple(all_files, output_file)
            
            # Merge results
            results.update(conversion_results)
            results['success'] = conversion_results['success']
            
            self.logger.info(f".phz conversion completed: {results['success']}")
            
        except zipfile.BadZipFile as e:
            error_msg = f"Invalid .phz file (not a valid zip): {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        except Exception as e:
            error_msg = f"Error processing .phz file: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        finally:
            # Clean up temporary directory
            if temp_dir and os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    self.logger.info(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up temporary directory {temp_dir}: {e}")
            
            end_time = datetime.now()
            results['duration'] = (end_time - start_time).total_seconds()
            
        return results
    
    def list_phz_contents(self, phz_file: str) -> Dict[str, Any]:
        """
        List the contents of a .phz file without extracting
        
        Args:
            phz_file: Path to input .phz file
            
        Returns:
            Dictionary with file contents information
        """
        results = {
            'success': False,
            'phz_contents': [],
            'phd_files': [],
            'mod_files': [],
            'other_files': [],
            'errors': []
        }
        
        try:
            self.logger.info(f"Listing contents of .phz file: {phz_file}")
            
            # Validate input file
            if not os.path.exists(phz_file):
                error_msg = f"Input file not found: {phz_file}"
                self.logger.error(error_msg)
                results['errors'].append(error_msg)
                return results
            
            # Read zip file contents
            with zipfile.ZipFile(phz_file, 'r') as zip_ref:
                extracted_files = zip_ref.namelist()
                results['phz_contents'] = extracted_files
                
                # Categorize files
                for file_name in extracted_files:
                    file_ext = os.path.splitext(file_name)[1].lower()
                    if file_ext == '.phd':
                        results['phd_files'].append(file_name)
                    elif file_ext == '.mod':
                        results['mod_files'].append(file_name)
                    else:
                        results['other_files'].append(file_name)
                
                results['success'] = True
                self.logger.info(f"Found {len(results['phd_files'])} .phd files, {len(results['mod_files'])} .mod files, {len(results['other_files'])} other files")
                
        except zipfile.BadZipFile as e:
            error_msg = f"Invalid .phz file (not a valid zip): {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        except Exception as e:
            error_msg = f"Error reading .phz file: {e}"
            self.logger.error(error_msg)
            results['errors'].append(error_msg)
            
        return results
