"""
Unit tests for TPS parser components
"""

import pytest
import os
from pathlib import Path


class TestTPSFileLoading:
    """Test TPS file loading and initialization"""
    
    def test_tps_initialization(self, sample_phd_file):
        """Test TPS object initialization"""
        from pytopspeed import TPS
        
        tps = TPS(sample_phd_file, encoding='cp1251', cached=True, check=True)
        
        assert tps is not None
        assert tps.filename == sample_phd_file
        assert tps.encoding == 'cp1251'
        assert tps.cached is True
        assert tps.check is True
    
    def test_tps_header_parsing(self, sample_tps):
        """Test TPS header parsing"""
        assert sample_tps.header is not None
        assert hasattr(sample_tps.header, 'top_speed_mark')
        assert hasattr(sample_tps.header, 'file_size')
        assert hasattr(sample_tps.header, 'page_root_ref')
        assert hasattr(sample_tps.header, 'last_issued_row')
    
    def test_tps_pages_loading(self, sample_tps):
        """Test TPS pages loading"""
        assert sample_tps.pages is not None
        # Check that pages object exists and has some content
        assert hasattr(sample_tps.pages, '__getitem__')
        assert hasattr(sample_tps.pages, 'list')
        # Check that we can get page list
        page_list = sample_tps.pages.list()
        assert isinstance(page_list, list)
        assert len(page_list) > 0, "No pages found"
    
    def test_tps_tables_loading(self, sample_tps):
        """Test TPS tables loading"""
        assert sample_tps.tables is not None
        assert len(sample_tps.tables._TpsTablesList__tables) > 0


class TestTPSPageStructure:
    """Test TPS page structure and decompression"""
    
    def test_page_structure(self, sample_tps):
        """Test page structure"""
        first_page = sample_tps.pages[0]
        assert hasattr(first_page, 'offset')
        assert hasattr(first_page, 'size')
        assert hasattr(first_page, 'record_count')
        assert hasattr(first_page, 'hierarchy_level')
        assert isinstance(first_page.offset, int)
        assert isinstance(first_page.size, int)
        assert isinstance(first_page.record_count, int)
        assert isinstance(first_page.hierarchy_level, int)
    
    def test_page_records_access(self, sample_tps):
        """Test page records access"""
        first_page = sample_tps.pages[0]
        # Check that we can access records through TpsRecordsList
        from pytopspeed.tpsrecord import TpsRecordsList
        records = TpsRecordsList(sample_tps, first_page, sample_tps.current_table_number)
        assert records is not None


class TestTPSRecordParsing:
    """Test TPS record parsing and data extraction"""
    
    def test_record_structure(self, sample_record):
        """Test record structure"""
        assert sample_record is not None
        assert isinstance(sample_record, dict)
        assert len(sample_record) > 0
    
    def test_record_data_types(self, sample_record):
        """Test record data types"""
        for key, value in sample_record.items():
            assert value is None or isinstance(value, (int, float, str, bytes))
    
    def test_record_number_field(self, sample_record):
        """Test record number field presence"""
        # Check for record number field (usually "b':RecNo'" or similar)
        recno_keys = [key for key in sample_record.keys() if 'recno' in key.lower() or 'rec_no' in key.lower()]
        assert len(recno_keys) > 0, "No record number field found"
    
    def test_record_field_names(self, sample_record):
        """Test record field names format"""
        for key in sample_record.keys():
            # Field names should be strings
            assert isinstance(key, str)
            # Should not be empty
            assert len(key) > 0


class TestTPSTableDefinitions:
    """Test TPS table definitions parsing"""
    
    def test_table_definitions_exist(self, sample_tps):
        """Test that table definitions exist"""
        assert len(sample_tps.tables._TpsTablesList__tables) > 0
    
    def test_table_names_extraction(self, sample_tps):
        """Test table names extraction"""
        named_tables = 0
        for table_number in sample_tps.tables._TpsTablesList__tables:
            table = sample_tps.tables._TpsTablesList__tables[table_number]
            if table.name and table.name != '':
                named_tables += 1
                assert isinstance(table.name, str)
                assert len(table.name) > 0
        
        assert named_tables > 0, "No named tables found"
    
    def test_table_definition_structure(self, sample_table_def):
        """Test table definition structure"""
        assert sample_table_def is not None
        assert hasattr(sample_table_def, 'fields')
        assert hasattr(sample_table_def, 'memos')
        assert hasattr(sample_table_def, 'indexes')
        
        assert isinstance(sample_table_def.fields, list)
        assert isinstance(sample_table_def.memos, list)
        assert isinstance(sample_table_def.indexes, list)
    
    def test_field_definitions(self, sample_table_def):
        """Test field definitions"""
        assert len(sample_table_def.fields) > 0
        
        for field in sample_table_def.fields:
            assert hasattr(field, 'name')
            assert hasattr(field, 'type')
            assert hasattr(field, 'size')
            
            assert isinstance(field.name, str)
            assert isinstance(field.type, (str, int))  # type can be string or enum
            assert isinstance(field.size, int)
            
            assert len(field.name) > 0
            assert field.size >= 0
    
    def test_memo_definitions(self, sample_table_def):
        """Test memo definitions"""
        for memo in sample_table_def.memos:
            assert hasattr(memo, 'name')
            assert hasattr(memo, 'type')
            
            assert isinstance(memo.name, str)
            assert isinstance(memo.type, str)
            
            assert len(memo.name) > 0
            assert len(memo.type) > 0
    
    def test_index_definitions(self, sample_table_def):
        """Test index definitions"""
        for index in sample_table_def.indexes:
            assert hasattr(index, 'name')
            assert hasattr(index, 'fields')
            
            assert isinstance(index.name, str)
            assert isinstance(index.fields, list)
            
            assert len(index.name) > 0
            assert len(index.fields) > 0
            
            for field in index.fields:
                assert hasattr(field, 'field_number')
                assert isinstance(field.field_number, int)


class TestTPSDataTypes:
    """Test TPS data type handling"""
    
    def test_string_field_parsing(self, sample_record):
        """Test string field parsing"""
        string_fields = [key for key, value in sample_record.items() 
                        if isinstance(value, str)]
        
        for field in string_fields:
            value = sample_record[field]
            # String values should be properly decoded
            assert isinstance(value, str)
            # Should not contain null bytes
            assert '\x00' not in value
    
    def test_numeric_field_parsing(self, sample_record):
        """Test numeric field parsing"""
        numeric_fields = [key for key, value in sample_record.items() 
                         if isinstance(value, (int, float))]
        
        for field in numeric_fields:
            value = sample_record[field]
            assert isinstance(value, (int, float))
            # Numeric values should be reasonable
            assert not (isinstance(value, float) and (value != value))  # Not NaN
    
    def test_date_field_parsing(self, sample_record):
        """Test date field parsing"""
        # Look for fields that might be dates (integer values in date range)
        date_candidates = [key for key, value in sample_record.items() 
                          if isinstance(value, int) and 10000 < value < 99999999]
        
        # Date-like fields may or may not exist depending on the database
        # Just verify that if they exist, they are integers in a reasonable range
        for candidate in date_candidates:
            value = sample_record[candidate]
            assert isinstance(value, int)
            assert 10000 <= value <= 99999999


class TestTPSIteration:
    """Test TPS record iteration"""
    
    def test_table_iteration(self, sample_tps, sample_table_name):
        """Test table record iteration"""
        sample_tps.set_current_table(sample_table_name)
        
        record_count = 0
        for record in sample_tps:
            assert isinstance(record, dict)
            assert len(record) > 0
            record_count += 1
            
            if record_count >= 10:  # Limit to first 10 records for performance
                break
        
        assert record_count > 0, "No records found in table"
    
    def test_iteration_consistency(self, sample_tps, sample_table_name):
        """Test iteration consistency"""
        sample_tps.set_current_table(sample_table_name)
        
        # First iteration
        records1 = []
        for i, record in enumerate(sample_tps):
            if i >= 5:  # Limit to first 5 records
                break
            records1.append(record)
        
        # Second iteration
        records2 = []
        for i, record in enumerate(sample_tps):
            if i >= 5:  # Limit to first 5 records
                break
            records2.append(record)
        
        # Should get same records
        assert len(records1) == len(records2)
        for r1, r2 in zip(records1, records2):
            assert r1 == r2
