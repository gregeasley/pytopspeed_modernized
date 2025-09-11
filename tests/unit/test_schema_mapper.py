"""
Unit tests for schema mapper components
"""

import pytest
from converter.schema_mapper import TopSpeedToSQLiteMapper


class TestTopSpeedToSQLiteMapper:
    """Test TopSpeedToSQLiteMapper functionality"""
    
    def test_mapper_initialization(self):
        """Test mapper initialization"""
        mapper = TopSpeedToSQLiteMapper()
        assert mapper is not None
        assert hasattr(mapper, 'TYPE_MAPPING')
        assert hasattr(mapper, 'sanitize_field_name')
        assert hasattr(mapper, 'sanitize_table_name')
        assert hasattr(mapper, 'generate_create_table_sql')
        assert hasattr(mapper, 'generate_create_index_sql')
        assert hasattr(mapper, 'map_table_schema')
    
    def test_type_mapping_completeness(self):
        """Test that TYPE_MAPPING covers all expected TopSpeed types"""
        mapper = TopSpeedToSQLiteMapper()
        
        expected_types = [
            'STRING', 'CSTRING', 'PSTRING',
            'BYTE', 'SHORT', 'USHORT', 'LONG', 'ULONG',
            'FLOAT', 'DOUBLE', 'DECIMAL',
            'DATE', 'TIME',
            'GROUP', 'MEMO', 'BLOB'
        ]
        
        for tps_type in expected_types:
            assert tps_type in mapper.TYPE_MAPPING, f"Missing type mapping for {tps_type}"
            assert mapper.TYPE_MAPPING[tps_type] in ['TEXT', 'INTEGER', 'REAL', 'BLOB'], \
                f"Invalid SQLite type for {tps_type}: {mapper.TYPE_MAPPING[tps_type]}"


class TestFieldNameSanitization:
    """Test field name sanitization"""
    
    def test_sanitize_field_name_basic(self):
        """Test basic field name sanitization"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test basic sanitization
        assert mapper.sanitize_field_name("FIELD_NAME") == "FIELD_NAME"
        assert mapper.sanitize_field_name("field_name") == "field_name"
        assert mapper.sanitize_field_name("Field_Name") == "Field_Name"
    
    def test_sanitize_field_name_table_prefix(self):
        """Test table prefix removal"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test table prefix removal
        assert mapper.sanitize_field_name("TIT:PROJ_DESCR") == "PROJ_DESCR"
        assert mapper.sanitize_field_name("GRP:GRP_ID") == "GRP_ID"
        assert mapper.sanitize_field_name("TST:LSE_ID") == "LSE_ID"
    
    def test_sanitize_field_name_special_chars(self):
        """Test special character handling"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test special character replacement
        assert mapper.sanitize_field_name("FIELD-NAME") == "FIELD_NAME"
        assert mapper.sanitize_field_name("FIELD NAME") == "FIELD_NAME"
        assert mapper.sanitize_field_name("FIELD.NAME") == "FIELD_NAME"
        assert mapper.sanitize_field_name("FIELD/NAME") == "FIELD_NAME"
        assert mapper.sanitize_field_name("FIELD\\NAME") == "FIELD_NAME"
    
    def test_sanitize_field_name_edge_cases(self):
        """Test edge cases for field name sanitization"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test edge cases
        assert mapper.sanitize_field_name("") == ""
        assert mapper.sanitize_field_name("A") == "A"
        assert mapper.sanitize_field_name("123") == "_123"  # Numbers need underscore prefix
        assert mapper.sanitize_field_name("_FIELD") == "_FIELD"
        assert mapper.sanitize_field_name("FIELD_") == "FIELD_"


class TestTableNameSanitization:
    """Test table name sanitization"""
    
    def test_sanitize_table_name_basic(self):
        """Test basic table name sanitization"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test basic sanitization
        assert mapper.sanitize_table_name("TABLE_NAME") == "TABLE_NAME"
        assert mapper.sanitize_table_name("table_name") == "table_name"
        assert mapper.sanitize_table_name("Table_Name") == "Table_Name"
    
    def test_sanitize_table_name_reserved_words(self):
        """Test SQLite reserved word handling"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test reserved word handling
        assert mapper.sanitize_table_name("ORDER") == "ORDER_TABLE"
        assert mapper.sanitize_table_name("GROUP") == "GROUP_TABLE"
        assert mapper.sanitize_table_name("SELECT") == "SELECT_TABLE"
        assert mapper.sanitize_table_name("FROM") == "FROM_TABLE"
        assert mapper.sanitize_table_name("WHERE") == "WHERE_TABLE"
    
    def test_sanitize_table_name_special_chars(self):
        """Test special character handling in table names"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test special character replacement
        assert mapper.sanitize_table_name("TABLE-NAME") == "TABLE_NAME"
        assert mapper.sanitize_table_name("TABLE NAME") == "TABLE_NAME"
        assert mapper.sanitize_table_name("TABLE.NAME") == "TABLE_NAME"
        assert mapper.sanitize_table_name("TABLE/NAME") == "TABLE_NAME"
        assert mapper.sanitize_table_name("TABLE\\NAME") == "TABLE_NAME"
    
    def test_sanitize_table_name_edge_cases(self):
        """Test edge cases for table name sanitization"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Test edge cases
        assert mapper.sanitize_table_name("") == ""
        assert mapper.sanitize_table_name("A") == "A"
        assert mapper.sanitize_table_name("123") == "_123"  # Numbers need underscore prefix
        assert mapper.sanitize_table_name("_TABLE") == "_TABLE"
        assert mapper.sanitize_table_name("TABLE_") == "TABLE_"


class TestCreateTableSQL:
    """Test CREATE TABLE SQL generation"""
    
    def test_generate_create_table_sql_basic(self, mock_field):
        """Test basic CREATE TABLE SQL generation"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Create mock table definition
        class MockTableDef:
            def __init__(self):
                self.fields = [mock_field]
                self.memos = []
        
        table_def = MockTableDef()
        sql = mapper.generate_create_table_sql("TEST_TABLE", table_def)
        
        assert sql.startswith("CREATE TABLE TEST_TABLE (")
        assert sql.endswith(");")
        assert "TEST_FIELD" in sql
        assert "TEXT" in sql
    
    def test_generate_create_table_sql_multiple_fields(self):
        """Test CREATE TABLE SQL with multiple fields"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Create mock fields
        class MockField:
            def __init__(self, name, type, length=0):
                self.name = name
                self.type = type
                self.length = length
                self.size = length  # Add size attribute for compatibility
                self.offset = 0  # Add offset attribute for multidimensional handler
                self.array_element_count = 1  # Add array_element_count for multidimensional handler
                self.array_element_size = None  # Add array_element_size for multidimensional handler
        
        fields = [
            MockField("NAME", "STRING", 50),
            MockField("AGE", "LONG", 0),
            MockField("PRICE", "DOUBLE", 0)
        ]
        
        # Set different offsets to prevent array detection
        fields[0].offset = 0
        fields[1].offset = 50
        fields[2].offset = 100
        
        class MockTableDef:
            def __init__(self):
                self.fields = fields
                self.memos = []
        
        table_def = MockTableDef()
        sql = mapper.generate_create_table_sql("TEST_TABLE", table_def)
        
        assert "NAME" in sql
        assert "AGE" in sql
        assert "PRICE" in sql
        assert "TEXT" in sql
        assert "INTEGER" in sql
        assert "REAL" in sql
    
    def test_generate_create_table_sql_with_memos(self, mock_field, mock_memo):
        """Test CREATE TABLE SQL with memo fields"""
        mapper = TopSpeedToSQLiteMapper()
        
        class MockTableDef:
            def __init__(self):
                self.fields = [mock_field]
                self.memos = [mock_memo]
        
        table_def = MockTableDef()
        sql = mapper.generate_create_table_sql("TEST_TABLE", table_def)
        
        assert "TEST_FIELD" in sql
        assert "TEST_MEMO" in sql
        assert "BLOB" in sql
    
    def test_generate_create_table_sql_sanitized_names(self):
        """Test CREATE TABLE SQL with sanitized names"""
        mapper = TopSpeedToSQLiteMapper()
        
        class MockField:
            def __init__(self, name, type, length=0):
                self.name = name
                self.type = type
                self.length = length
                self.size = length  # Add size attribute for compatibility
                self.offset = 0  # Add offset attribute for multidimensional handler
                self.array_element_count = 1  # Add array_element_count for multidimensional handler
                self.array_element_size = None  # Add array_element_size for multidimensional handler
        
        field = MockField("TIT:PROJ_DESCR", "STRING", 50)
        
        class MockTableDef:
            def __init__(self):
                self.fields = [field]
                self.memos = []
        
        table_def = MockTableDef()
        sql = mapper.generate_create_table_sql("ORDER", table_def)  # Reserved word
        
        assert "PROJ_DESCR" in sql  # Sanitized field name
        assert "ORDER_TABLE" in sql  # Sanitized table name


class TestCreateIndexSQL:
    """Test CREATE INDEX SQL generation"""
    
    def test_generate_create_index_sql_basic(self, mock_index):
        """Test basic CREATE INDEX SQL generation"""
        mapper = TopSpeedToSQLiteMapper()
        
        # Create mock table definition with fields
        class MockField:
            def __init__(self, name, type, length=0):
                self.name = name
                self.type = type
                self.length = length
                self.size = length  # Add size attribute for compatibility
        
        class MockTableDef:
            def __init__(self):
                self.fields = [
                    MockField("FIELD1", "STRING", 50),
                    MockField("FIELD2", "LONG", 0)
                ]
        
        table_def = MockTableDef()
        sql = mapper.generate_create_index_sql("TEST_TABLE", mock_index, table_def)
        
        assert sql.startswith("CREATE INDEX TEST_TABLE_TEST_INDEX ON TEST_TABLE (")
        assert sql.endswith(");")
        assert "FIELD1" in sql
        assert "FIELD2" in sql
    
    def test_generate_create_index_sql_sanitized_names(self):
        """Test CREATE INDEX SQL with sanitized names"""
        mapper = TopSpeedToSQLiteMapper()
        
        class MockIndexField:
            def __init__(self, field_number):
                self.field_number = field_number
        
        class MockIndex:
            def __init__(self, name, fields):
                self.name = name
                self.fields = fields
        
        index = MockIndex("TIT:IDX_NAME", [MockIndexField(0)])
        
        class MockField:
            def __init__(self, name, type, length=0):
                self.name = name
                self.type = type
                self.length = length
                self.size = length  # Add size attribute for compatibility
        
        class MockTableDef:
            def __init__(self):
                self.fields = [MockField("TIT:PROJ_DESCR", "STRING", 50)]
        
        table_def = MockTableDef()
        sql = mapper.generate_create_index_sql("ORDER", index, table_def)  # Reserved word
        
        assert "ORDER_TABLE_IDX_NAME" in sql  # Sanitized index name
        assert "ORDER_TABLE" in sql  # Sanitized table name
        assert "PROJ_DESCR" in sql  # Sanitized field name


class TestMapTableSchema:
    """Test complete table schema mapping"""
    
    def test_map_table_schema_complete(self, sample_table_name, sample_table_def):
        """Test complete table schema mapping"""
        mapper = TopSpeedToSQLiteMapper()
        
        schema = mapper.map_table_schema(sample_table_name, sample_table_def)
        
        assert isinstance(schema, dict)
        assert 'table_name' in schema
        assert 'create_table' in schema
        assert 'create_indexes' in schema
        
        assert isinstance(schema['table_name'], str)
        assert isinstance(schema['create_table'], str)
        assert isinstance(schema['create_indexes'], list)
        
        assert len(schema['table_name']) > 0
        assert len(schema['create_table']) > 0
        assert schema['create_table'].startswith("CREATE TABLE")
        assert schema['create_table'].endswith(");")
        
        for index_sql in schema['create_indexes']:
            assert isinstance(index_sql, str)
            assert index_sql.startswith("CREATE INDEX")
            assert index_sql.endswith(");")
    
    def test_map_table_schema_sanitization(self, sample_table_name, sample_table_def):
        """Test that schema mapping applies proper sanitization"""
        mapper = TopSpeedToSQLiteMapper()
        
        schema = mapper.map_table_schema(sample_table_name, sample_table_def)
        
        # Table name should be sanitized
        sanitized_table_name = mapper.sanitize_table_name(sample_table_name)
        assert schema['table_name'] == sanitized_table_name
        
        # CREATE TABLE SQL should use sanitized table name
        assert sanitized_table_name in schema['create_table']
        
        # Index names should be unique and sanitized
        for index_sql in schema['create_indexes']:
            assert sanitized_table_name in index_sql  # Should be prefixed with table name
