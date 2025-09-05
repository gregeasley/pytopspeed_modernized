"""
Pytest configuration and shared fixtures for the phdwin_reader test suite
"""

import pytest
import os
import sys
import tempfile
import sqlite3
from pathlib import Path

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from pytopspeed import TPS
from converter.schema_mapper import TopSpeedToSQLiteMapper
from converter.sqlite_converter import SqliteConverter


@pytest.fixture
def sample_phd_file():
    """Fixture providing path to sample .phd file"""
    phd_file = Path(__file__).parent.parent / 'assets' / 'TxWells.PHD'
    if not phd_file.exists():
        pytest.skip("Sample .phd file not found")
    return str(phd_file)


@pytest.fixture
def sample_tps():
    """Fixture providing loaded TPS object from sample file"""
    phd_file = Path(__file__).parent.parent / 'assets' / 'TxWells.PHD'
    if not phd_file.exists():
        pytest.skip("Sample .phd file not found")
    
    tps = TPS(str(phd_file), encoding='cp1251', cached=True, check=True)
    return tps


@pytest.fixture
def temp_sqlite_db():
    """Fixture providing temporary SQLite database for testing"""
    with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
        db_path = tmp.name
    
    yield db_path
    
    # Cleanup
    try:
        os.unlink(db_path)
    except OSError:
        pass


@pytest.fixture
def schema_mapper():
    """Fixture providing TopSpeedToSQLiteMapper instance"""
    return TopSpeedToSQLiteMapper()


@pytest.fixture
def sqlite_converter():
    """Fixture providing SqliteConverter instance"""
    return SqliteConverter(batch_size=100)


@pytest.fixture
def sample_table_def(sample_tps):
    """Fixture providing a sample table definition"""
    # Get the first table with a name
    for table_number in sample_tps.tables._TpsTablesList__tables:
        table = sample_tps.tables._TpsTablesList__tables[table_number]
        if table.name and table.name != '':
            return sample_tps.tables.get_definition(table_number)
    
    pytest.skip("No named tables found in sample file")


@pytest.fixture
def sample_table_name(sample_tps):
    """Fixture providing a sample table name"""
    # Get the first table with a name
    for table_number in sample_tps.tables._TpsTablesList__tables:
        table = sample_tps.tables._TpsTablesList__tables[table_number]
        if table.name and table.name != '':
            return table.name
    
    pytest.skip("No named tables found in sample file")


@pytest.fixture
def sample_record(sample_tps, sample_table_name):
    """Fixture providing a sample record from the first table with data"""
    sample_tps.set_current_table(sample_table_name)
    
    # Get the first record
    for record in sample_tps:
        return record
    
    pytest.skip("No records found in sample table")


@pytest.fixture
def mock_field():
    """Fixture providing a mock field definition"""
    class MockField:
        def __init__(self, name, type, length=0):
            self.name = name
            self.type = type
            self.length = length
            self.size = length  # Add size attribute for compatibility
    
    return MockField("TEST_FIELD", "STRING", 50)


@pytest.fixture
def mock_memo():
    """Fixture providing a mock memo definition"""
    class MockMemo:
        def __init__(self, name, type="TEXT"):
            self.name = name
            self.type = type
    
    return MockMemo("TEST_MEMO", "TEXT")


@pytest.fixture
def mock_index():
    """Fixture providing a mock index definition"""
    class MockIndexField:
        def __init__(self, field_number):
            self.field_number = field_number
    
    class MockIndex:
        def __init__(self, name, fields):
            self.name = name
            self.fields = fields
    
    return MockIndex("TEST_INDEX", [MockIndexField(0), MockIndexField(1)])
