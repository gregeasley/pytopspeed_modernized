"""
Pytest configuration for resilience tests

This file provides common fixtures and configuration for all resilience tests.
"""

import pytest
import tempfile
import os
import sys
from unittest.mock import Mock, MagicMock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_tps():
    """Create a mock TPS object for testing"""
    tps = Mock()
    
    # Mock tables
    tps.tables = Mock()
    tps.tables._TpsTablesList__tables = {}
    
    # Mock pages
    tps.pages = Mock()
    tps.pages.list.return_value = []
    tps.pages.__getitem__ = Mock()
    
    # Mock current table
    tps.current_table_number = 1
    
    return tps


@pytest.fixture
def mock_table_def():
    """Create a mock table definition for testing"""
    table_def = Mock()
    table_def.name = "TEST_TABLE"
    table_def.fields = []
    table_def.memos = []
    table_def.indexes = []
    table_def.record_size = 100
    table_def.field_count = 10
    table_def.memo_count = 0
    table_def.index_count = 0
    
    return table_def


@pytest.fixture
def mock_record():
    """Create a mock TPS record for testing"""
    record = Mock()
    record.type = 'DATA'
    record.data = Mock()
    record.data.table_number = 1
    record.data.data = Mock()
    record.data.data.data = b"test data"
    
    return record


@pytest.fixture
def sample_tps():
    """Create a sample TPS object for testing"""
    tps = Mock()
    
    # Mock tables with proper string attributes
    table_mock = Mock()
    table_mock.name = "SAMPLE_TABLE"
    tps.tables = Mock()
    tps.tables._TpsTablesList__tables = {1: table_mock}
    
    # Mock get_definition method to return our sample table definition
    def mock_get_definition(table_number):
        # Create the table definition directly to avoid circular imports
        class MockField:
            def __init__(self, name, field_type, size, offset, field_number=None):
                self.name = name
                self.type = field_type
                self.size = size
                self.offset = offset
                self.array_element_count = 1
                self.array_element_size = size
                self.field_number = field_number or 0
        
        table_def = Mock()
        table_def.name = "SAMPLE_TABLE"
        field1 = MockField("NAME_FIELD", "STRING", 20, 0, 0)
        field2 = MockField("ID_FIELD", "LONG", 4, 20, 1)
        field3 = MockField("VALUE_FIELD", "DOUBLE", 8, 24, 2)
        
        table_def.fields = [field1, field2, field3]
        table_def.memos = []
        
        # Add some mock indexes that reference the actual fields
        index1 = Mock()
        index1.name = "IDX_NAME"
        index1.fields = [field1]  # Reference to field1
        index1.unique = False
        
        index2 = Mock()
        index2.name = "IDX_ID"
        index2.fields = [field2]  # Reference to field2
        index2.unique = True
        
        table_def.indexes = [index1, index2]
        table_def.record_size = 32
        table_def.field_count = 3
        table_def.memo_count = 0
        table_def.index_count = 2
        return table_def
    
    tps.tables.get_definition = mock_get_definition
    
    # Mock pages with proper attributes
    page_mock = Mock()
    page_mock.hierarchy_level = 0
    page_mock.offset = 1024
    page_mock.ref = 1
    page_mock.size = 4096  # Add size attribute as integer
    page_mock.record_count = 10  # Add record_count attribute as integer
    page_mock.uncompressed_size = 4096  # Add uncompressed_size attribute as integer
    
    tps.pages = Mock()
    tps.pages.list.return_value = [1, 2]
    tps.pages.__getitem__ = Mock(return_value=page_mock)
    
    # Mock current table
    tps.current_table_number = 1
    
    # Mock header
    tps.header = Mock()
    tps.header.size = 512
    
    # Make TPS iterable for iteration tests
    def tps_iter(self):
        # Return a few mock records
        for i in range(3):
            record = {
                'FIELD1': f'sample string {i}',
                'FIELD2': 12345 + i,
                'FIELD3': 3.14159 + i,
                'recno': i + 1
            }
            yield record
    
    tps.__iter__ = tps_iter
    
    # Mock cache_pages for TpsRecordsList
    tps.cache_pages = {}
    
    # Mock read method for TpsRecordsList with more realistic data
    tps.read = Mock(return_value=b"x" * 4096)  # 4KB of mock data
    
    return tps


@pytest.fixture
def sample_table_name():
    """Provide a sample table name for testing"""
    return "SAMPLE_TABLE"


@pytest.fixture
def sample_record():
    """Create a sample TPS record for testing"""
    # Create a dictionary-like record for TPS parser tests
    record = {
        'FIELD1': 'sample string value',
        'FIELD2': 12345,
        'FIELD3': 3.14159,
        'recno': 1,
        'rec_no': 1
    }
    return record


@pytest.fixture
def sample_table_def():
    """Create a sample table definition for testing"""
    table_def = Mock()
    table_def.name = "SAMPLE_TABLE"
    
    # Create proper field objects with string attributes
    class MockField:
        def __init__(self, name, field_type, size, offset):
            self.name = name
            self.type = field_type
            self.size = size
            self.offset = offset
            self.array_element_count = 1
            self.array_element_size = size
    
    field1 = MockField("NAME_FIELD", "STRING", 20, 0)
    field2 = MockField("ID_FIELD", "LONG", 4, 20)
    field3 = MockField("VALUE_FIELD", "DOUBLE", 8, 24)
    
    table_def.fields = [field1, field2, field3]
    table_def.memos = []
    table_def.indexes = []
    table_def.record_size = 32
    table_def.field_count = 3
    table_def.memo_count = 0
    table_def.index_count = 0
    
    return table_def


@pytest.fixture
def sample_phd_file():
    """Create a sample PHD file path for testing"""
    return "sample.phd"


@pytest.fixture
def mock_field():
    """Create a mock field for testing"""
    field = Mock()
    field.name = "TEST_FIELD"
    field.type = "STRING"
    field.size = 20
    field.offset = 0
    field.array_element_count = 1
    field.array_element_size = 20
    field.field_number = 1
    return field


@pytest.fixture
def mock_memo():
    """Create a mock memo for testing"""
    memo = Mock()
    memo.name = "TEST_MEMO"
    memo.type = "MEMO"
    memo.size = 10
    memo.offset = 0
    return memo


@pytest.fixture
def mock_index():
    """Create a mock index for testing"""
    index = Mock()
    index.name = "TEST_INDEX"
    
    # Create mock field objects for both fields
    field1_mock = Mock()
    field1_mock.field_number = 0  # Index 0 = FIELD1
    field1_mock.name = "FIELD1"
    
    field2_mock = Mock()
    field2_mock.field_number = 1  # Index 1 = FIELD2
    field2_mock.name = "FIELD2"
    
    index.fields = [field1_mock, field2_mock]
    index.unique = False
    return index


@pytest.fixture
def temp_sqlite_db():
    """Create a temporary SQLite database for testing"""
    import sqlite3
    import tempfile
    import os
    
    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()
    
    # Create SQLite connection
    conn = sqlite3.connect(temp_file.name)
    
    yield conn
    
    # Cleanup
    conn.close()
    try:
        os.unlink(temp_file.name)
    except OSError:
        pass  # File might already be deleted


@pytest.fixture
def sample_binary_data():
    """Create sample binary data for testing"""
    return {
        'small': b"small data",
        'medium': b"x" * 1000,
        'large': b"x" * 10000,
        'very_large': b"x" * 100000
    }


@pytest.fixture
def sample_table_definitions():
    """Create sample table definitions for testing"""
    return {
        'small': Mock(record_size=50, field_count=10),
        'medium': Mock(record_size=500, field_count=25),
        'large': Mock(record_size=2000, field_count=50),
        'very_large': Mock(record_size=8000, field_count=150)
    }


@pytest.fixture
def sample_size_estimates():
    """Create sample size estimates for testing"""
    return {
        'small': {'estimated_records': 1000, 'estimated_size_mb': 10},
        'medium': {'estimated_records': 10000, 'estimated_size_mb': 100},
        'large': {'estimated_records': 100000, 'estimated_size_mb': 1000},
        'enterprise': {'estimated_records': 1000000, 'estimated_size_mb': 10000}
    }


@pytest.fixture(scope="session")
def test_data_dir():
    """Create a temporary directory for test data that persists for the session"""
    temp_dir = tempfile.mkdtemp(prefix="resilience_tests_")
    yield temp_dir
    # Cleanup is handled by the system


@pytest.fixture
def mock_psutil_process():
    """Create a mock psutil process for memory testing"""
    process = Mock()
    process.memory_info.return_value.rss = 200 * 1024 * 1024  # 200MB
    return process


@pytest.fixture
def mock_gc():
    """Create a mock gc module for garbage collection testing"""
    with pytest.MonkeyPatch().context() as m:
        m.setattr('gc.collect', Mock())
        yield m


# Performance test markers
def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "performance: mark test as a performance test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "memory: mark test as memory intensive"
    )


# Skip performance tests by default unless explicitly requested
def pytest_collection_modifyitems(config, items):
    """Modify test collection to skip performance tests by default"""
    if not config.getoption("--run-performance"):
        skip_performance = pytest.mark.skip(reason="Performance tests skipped (use --run-performance to enable)")
        for item in items:
            if "performance" in item.keywords:
                item.add_marker(skip_performance)


def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--run-performance",
        action="store_true",
        default=False,
        help="Run performance tests"
    )
    parser.addoption(
        "--memory-limit",
        action="store",
        default="500",
        help="Memory limit in MB for tests"
    )


# Test data generators
class TestDataGenerator:
    """Helper class for generating test data"""
    
    @staticmethod
    def create_binary_data(size_bytes):
        """Create binary data of specified size"""
        return b"x" * size_bytes
    
    @staticmethod
    def create_table_definition(record_size, field_count, memo_count=0, index_count=0):
        """Create a table definition with specified characteristics"""
        table_def = Mock()
        table_def.record_size = record_size
        table_def.field_count = field_count
        table_def.memo_count = memo_count
        table_def.index_count = index_count
        table_def.fields = [Mock() for _ in range(field_count)]
        table_def.memos = [Mock() for _ in range(memo_count)]
        table_def.indexes = [Mock() for _ in range(index_count)]
        return table_def
    
    @staticmethod
    def create_tps_with_pages(page_count, records_per_page):
        """Create a mock TPS with specified number of pages and records"""
        tps = Mock()
        tps.tables = Mock()
        tps.tables._TpsTablesList__tables = {1: Mock(name="TEST_TABLE")}
        
        # Create pages
        pages = []
        for i in range(page_count):
            page = Mock()
            page.hierarchy_level = 0
            pages.append((i, page))
        
        tps.pages.list.return_value = [p[0] for p in pages]
        tps.pages.__getitem__.side_effect = lambda x: next(p[1] for p in pages if p[0] == x)
        
        # Mock records
        with pytest.MonkeyPatch().context() as m:
            from unittest.mock import patch
            with patch('pytopspeed.tpsrecord.TpsRecordsList') as mock_records_list:
                mock_records = []
                for i in range(records_per_page):
                    record = Mock()
                    record.type = 'DATA'
                    record.data.table_number = 1
                    mock_records.append(record)
                
                mock_records_list.return_value = mock_records
                yield tps


@pytest.fixture
def test_data_generator():
    """Provide test data generator"""
    return TestDataGenerator


# Cleanup fixtures
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Automatic cleanup after each test"""
    yield
    # Force garbage collection after each test
    import gc
    gc.collect()


# Mock external dependencies
@pytest.fixture(autouse=True)
def mock_external_dependencies():
    """Mock external dependencies that might not be available in test environment"""
    with pytest.MonkeyPatch().context() as m:
        # Mock psutil if not available
        try:
            import psutil
        except ImportError:
            m.setattr('psutil.Process', Mock())
        
        # Mock pytopspeed modules if not available
        try:
            import pytopspeed
        except ImportError:
            m.setattr('pytopspeed.tpsrecord.TpsRecordsList', Mock())
        
        # Mock TPS class to avoid file system dependencies
        def mock_tps_init(self, filename, encoding='cp1251', cached=True, check=True):
            self.filename = filename
            self.encoding = encoding
            self.cached = cached
            self.check = check
            self.tables = Mock()
            self.pages = Mock()
            self.current_table_number = 1
        
        m.setattr('pytopspeed.tps.TPS.__init__', mock_tps_init)
        
        # Mock TpsRecordsList to avoid complex parsing
        class MockTpsRecordsList:
            def __init__(self, tps, page, table_number):
                self.tps = tps
                self.tps_page = page
                self.table_number = table_number
                self.records = []  # Empty list for simplicity
        
        m.setattr('pytopspeed.tpsrecord.TpsRecordsList', MockTpsRecordsList)
        
        yield m