"""
Unit tests for multidimensional handler components
"""

import pytest
import json
from unittest.mock import Mock, MagicMock
from converter.multidimensional_handler import MultidimensionalHandler, ArrayFieldInfo


class TestArrayFieldInfo:
    """Test ArrayFieldInfo dataclass"""
    
    def test_array_field_info_creation(self):
        """Test ArrayFieldInfo creation with all parameters"""
        array_info = ArrayFieldInfo(
            base_name="TEST:ARRAY",
            element_type="DOUBLE",
            element_size=8,
            array_size=5,
            start_offset=100,
            element_offsets=[100, 108, 116, 124, 132],
            is_single_field_array=True
        )
        
        assert array_info.base_name == "TEST:ARRAY"
        assert array_info.element_type == "DOUBLE"
        assert array_info.element_size == 8
        assert array_info.array_size == 5
        assert array_info.start_offset == 100
        assert array_info.element_offsets == [100, 108, 116, 124, 132]
        assert array_info.is_single_field_array is True
    
    def test_array_field_info_defaults(self):
        """Test ArrayFieldInfo creation with default values"""
        array_info = ArrayFieldInfo(
            base_name="TEST:ARRAY",
            element_type="BYTE",
            element_size=1,
            array_size=10,
            start_offset=0,
            element_offsets=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        )
        
        assert array_info.is_single_field_array is False  # Default value


class TestMultidimensionalHandlerInitialization:
    """Test MultidimensionalHandler initialization"""
    
    def test_handler_initialization(self):
        """Test handler initialization"""
        handler = MultidimensionalHandler()
        
        assert handler is not None
        assert hasattr(handler, 'analyze_table_structure')
        assert hasattr(handler, 'parse_record_data')


class TestArrayDetection:
    """Test array detection functionality"""
    
    def test_analyze_single_field_array_with_array_element_count(self):
        """Test single field array detection using array_element_count"""
        handler = MultidimensionalHandler()
        
        # Mock field with array_element_count > 1
        mock_field = Mock()
        mock_field.name = "TEST:BOOLPARAM"
        mock_field.type = "BYTE"
        mock_field.size = 10
        mock_field.offset = 100
        mock_field.array_element_count = 10
        mock_field.array_element_size = None
        
        result = handler._analyze_single_field_array(mock_field)
        
        assert result is not None
        assert isinstance(result, ArrayFieldInfo)
        assert result.base_name == "TEST:BOOLPARAM"
        assert result.element_type == "BYTE"
        assert result.element_size == 1  # Calculated from size/count
        assert result.array_size == 10
        assert result.start_offset == 100
        # Note: is_single_field_array is set in analyze_table_structure, not in _analyze_single_field_array
        assert len(result.element_offsets) == 10
        assert result.element_offsets == [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
    
    def test_analyze_single_field_array_with_array_element_size(self):
        """Test single field array detection with array_element_size provided"""
        handler = MultidimensionalHandler()
        
        # Mock field with array_element_size provided
        mock_field = Mock()
        mock_field.name = "TEST:REALPARAM"
        mock_field.type = "DOUBLE"
        mock_field.size = 40
        mock_field.offset = 200
        mock_field.array_element_count = 5
        mock_field.array_element_size = 8
        
        result = handler._analyze_single_field_array(mock_field)
        
        assert result is not None
        assert result.element_size == 8  # Uses provided array_element_size
        assert result.array_size == 5
        assert result.element_offsets == [200, 208, 216, 224, 232]
    
    def test_analyze_single_field_array_not_array(self):
        """Test single field that is not an array (array_element_count = 1)"""
        handler = MultidimensionalHandler()
        
        # Mock field that is not an array
        mock_field = Mock()
        mock_field.name = "TEST:IDLBL"
        mock_field.type = "STRING"
        mock_field.size = 15
        mock_field.offset = 50
        mock_field.array_element_count = 1
        mock_field.array_element_size = None
        
        result = handler._analyze_single_field_array(mock_field)
        
        assert result is None  # Should not be detected as array
    
    def test_analyze_array_pattern_multi_field_array(self):
        """Test multi-field array pattern detection"""
        handler = MultidimensionalHandler()
        
        # Create mock fields that form a multi-field array
        mock_fields = []
        for i in range(5):
            field = Mock()
            field.name = f"TEST:PROD{i+1}"
            field.type = "DOUBLE"
            field.size = 8
            field.offset = 100 + i * 8
            field.array_element_count = 1
            mock_fields.append(field)
        
        result = handler._analyze_array_pattern("TEST:PROD", mock_fields)
        
        assert result is not None
        assert result.base_name == "TEST:PROD"
        assert result.element_type == "DOUBLE"
        assert result.element_size == 8
        assert result.array_size == 5
        assert result.start_offset == 100
        assert result.is_single_field_array is False
        assert result.element_offsets == [100, 108, 116, 124, 132]
    
    def test_analyze_array_pattern_no_pattern(self):
        """Test that non-array fields are not detected as arrays"""
        handler = MultidimensionalHandler()
        
        # Create mock fields that don't form an array pattern (irregular spacing)
        mock_fields = []
        field_names = ["TEST:ID", "TEST:NAME", "TEST:VALUE", "TEST:DATE", "TEST:STATUS"]
        # Use irregular offsets to prevent array detection
        offsets = [100, 125, 150, 180, 220]  # Irregular spacing
        for i, (name, offset) in enumerate(zip(field_names, offsets)):
            field = Mock()
            field.name = name
            field.type = "STRING"
            field.size = 20
            field.offset = offset
            field.array_element_count = 1
            mock_fields.append(field)
        
        result = handler._analyze_array_pattern("TEST:MIXED", mock_fields)
        
        assert result is None  # Should not detect array pattern due to irregular spacing


class TestTableStructureAnalysis:
    """Test table structure analysis"""
    
    def test_analyze_table_structure_no_arrays(self):
        """Test table structure analysis with no arrays"""
        handler = MultidimensionalHandler()
        
        # Create mock table definition with no arrays
        mock_table_def = Mock()
        mock_fields = []
        for i in range(3):
            field = Mock()
            field.name = f"TEST:FIELD{i+1}"
            field.type = "STRING"
            field.size = 20
            field.offset = i * 20
            field.array_element_count = 1
            mock_fields.append(field)
        
        mock_table_def.fields = mock_fields
        
        result = handler.analyze_table_structure(mock_table_def)
        
        assert result['has_arrays'] is False
        assert len(result['array_fields']) == 0
        assert len(result['regular_fields']) == 3
        assert result['regular_fields'] == mock_fields
    
    def test_analyze_table_structure_with_single_field_array(self):
        """Test table structure analysis with single-field arrays"""
        handler = MultidimensionalHandler()
        
        # Create mock table definition with single-field array
        mock_table_def = Mock()
        mock_fields = []
        
        # Regular field
        field1 = Mock()
        field1.name = "TEST:ID"
        field1.type = "SHORT"
        field1.size = 2
        field1.offset = 0
        field1.array_element_count = 1
        field1.is_enhanced_field = False  # Explicitly set to False for test
        mock_fields.append(field1)
        
        # Single-field array
        field2 = Mock()
        field2.name = "TEST:BOOLPARAM"
        field2.type = "BYTE"
        field2.size = 10
        field2.offset = 2
        field2.array_element_count = 10
        field2.array_element_size = None
        field2.is_enhanced_field = False  # Explicitly set to False for test
        mock_fields.append(field2)
        
        # Another regular field
        field3 = Mock()
        field3.name = "TEST:STATUS"
        field3.type = "SHORT"
        field3.size = 2
        field3.offset = 12
        field3.array_element_count = 1
        field3.is_enhanced_field = False  # Explicitly set to False for test
        mock_fields.append(field3)
        
        mock_table_def.fields = mock_fields
        
        result = handler.analyze_table_structure(mock_table_def)
        
        assert result['has_arrays'] is True
        assert len(result['array_fields']) == 1
        assert len(result['regular_fields']) == 2
        
        # Check array field
        array_field = result['array_fields'][0]
        assert array_field.base_name == "TEST:BOOLPARAM"
        assert array_field.is_single_field_array is True
        
        # Check regular fields
        regular_field_names = [f.name for f in result['regular_fields']]
        assert "TEST:ID" in regular_field_names
        assert "TEST:STATUS" in regular_field_names
    
    def test_analyze_table_structure_with_multi_field_array(self):
        """Test table structure analysis with multi-field arrays"""
        handler = MultidimensionalHandler()
        
        # Create mock table definition with multi-field array
        mock_table_def = Mock()
        mock_fields = []
        
        # Regular field
        field1 = Mock()
        field1.name = "TEST:ID"
        field1.type = "SHORT"
        field1.size = 2
        field1.offset = 0
        field1.array_element_count = 1
        field1.is_enhanced_field = False  # Explicitly set to False for test
        mock_fields.append(field1)
        
        # Multi-field array (PROD1, PROD2, PROD3, PROD4, PROD5)
        for i in range(5):
            field = Mock()
            field.name = f"TEST:PROD{i+1}"
            field.type = "DOUBLE"
            field.size = 8
            field.offset = 2 + i * 8
            field.array_element_count = 1
            field.is_enhanced_field = False  # Explicitly set to False for test
            mock_fields.append(field)
        
        # Another regular field
        field7 = Mock()
        field7.name = "TEST:STATUS"
        field7.type = "SHORT"
        field7.size = 2
        field7.offset = 42
        field7.array_element_count = 1
        field7.is_enhanced_field = False  # Explicitly set to False for test
        mock_fields.append(field7)
        
        mock_table_def.fields = mock_fields
        
        result = handler.analyze_table_structure(mock_table_def)
        
        assert result['has_arrays'] is True
        assert len(result['array_fields']) == 1
        assert len(result['regular_fields']) == 2  # ID and STATUS
        
        # Check array field
        array_field = result['array_fields'][0]
        assert array_field.base_name == "TEST:PROD"
        assert array_field.array_size == 5
        assert array_field.is_single_field_array is False
        
        # Check regular fields
        regular_field_names = [f.name for f in result['regular_fields']]
        assert "TEST:ID" in regular_field_names
        assert "TEST:STATUS" in regular_field_names


class TestFieldNameSanitization:
    """Test field name sanitization"""
    
    def test_sanitize_field_name_basic(self):
        """Test basic field name sanitization"""
        handler = MultidimensionalHandler()
        
        # Test basic sanitization
        assert handler._sanitize_field_name("TEST:ID") == "ID"
        assert handler._sanitize_field_name("LPV:BOOLPARAM") == "BOOLPARAM"
        assert handler._sanitize_field_name("DAT:PROD1") == "PROD1"
    
    def test_sanitize_field_name_with_prefix(self):
        """Test field name sanitization with prefix removal"""
        handler = MultidimensionalHandler()
        
        # Test with different prefixes
        assert handler._sanitize_field_name("ILF:IDLBL") == "IDLBL"
        assert handler._sanitize_field_name("CUM:PROD1") == "PROD1"
        assert handler._sanitize_field_name("MON:DAT1") == "DAT1"
    
    def test_sanitize_field_name_no_prefix(self):
        """Test field name sanitization without prefix"""
        handler = MultidimensionalHandler()
        
        # Test without prefix
        assert handler._sanitize_field_name("ID") == "ID"
        assert handler._sanitize_field_name("NAME") == "NAME"
        assert handler._sanitize_field_name("VALUE") == "VALUE"


class TestFieldValueParsing:
    """Test field value parsing"""
    
    def test_parse_field_value_string(self):
        """Test string field value parsing"""
        handler = MultidimensionalHandler()
        
        # Create mock field
        mock_field = Mock()
        mock_field.offset = 0
        mock_field.type = "STRING"
        mock_field.size = 12
        
        # Test STRING type
        field_data = b"Hello World\x00"
        result = handler._parse_field_value(field_data, mock_field)
        assert result == "Hello World"
        
        # Test with null bytes
        field_data = b"Test\x00\x00\x00"
        result = handler._parse_field_value(field_data, mock_field)
        assert result == "Test"
    
    def test_parse_field_value_numeric(self):
        """Test numeric field value parsing"""
        handler = MultidimensionalHandler()
        
        # Test SHORT type
        mock_field = Mock()
        mock_field.offset = 0
        mock_field.type = "SHORT"
        mock_field.size = 2
        
        field_data = b'\x01\x00'  # Little-endian 1
        result = handler._parse_field_value(field_data, mock_field)
        assert result == 1
        
        # Test LONG type
        mock_field.type = "LONG"
        mock_field.size = 4
        field_data = b'\x02\x00\x00\x00'  # Little-endian 2
        result = handler._parse_field_value(field_data, mock_field)
        assert result == 2
        
        # Test DOUBLE type
        mock_field.type = "DOUBLE"
        mock_field.size = 8
        field_data = b'\x00\x00\x00\x00\x00\x00\xf0\x3f'  # 1.0 as double
        result = handler._parse_field_value(field_data, mock_field)
        assert abs(result - 1.0) < 0.0001
    
    def test_parse_field_value_boolean(self):
        """Test boolean field value parsing"""
        handler = MultidimensionalHandler()
        
        # Test BYTE type (boolean)
        mock_field = Mock()
        mock_field.offset = 0
        mock_field.type = "BYTE"
        mock_field.size = 1
        
        field_data = b'\x01'
        result = handler._parse_field_value(field_data, mock_field)
        assert result is True
        
        field_data = b'\x00'
        result = handler._parse_field_value(field_data, mock_field)
        assert result is False
        
        # Test BOOL type
        mock_field.type = "BOOL"
        field_data = b'\x01'
        result = handler._parse_field_value(field_data, mock_field)
        assert result is True
        
        field_data = b'\x00'
        result = handler._parse_field_value(field_data, mock_field)
        assert result is False
    
    def test_parse_field_value_insufficient_data(self):
        """Test field value parsing with insufficient data"""
        handler = MultidimensionalHandler()
        
        # Test with empty data
        mock_field = Mock()
        mock_field.offset = 0
        mock_field.type = "SHORT"
        mock_field.size = 2
        
        field_data = b''
        result = handler._parse_field_value(field_data, mock_field)
        assert result is None
        
        # Test with partial data - the method returns hex string for insufficient data
        field_data = b'\x01'  # Only 1 byte for SHORT (needs 2)
        result = handler._parse_field_value(field_data, mock_field)
        assert result == '01'  # Returns hex string when insufficient data


class TestRecordDataParsing:
    """Test record data parsing functionality"""
    
    def test_parse_record_data_no_arrays(self):
        """Test record data parsing with no arrays"""
        handler = MultidimensionalHandler()
        
        # Mock analysis with no arrays
        analysis = {
            'has_arrays': False,
            'array_fields': [],
            'regular_fields': []
        }
        
        # Mock record data
        data = b"test data"
        
        result = handler.parse_record_data(data, analysis)
        
        assert result == {}
    
    def test_parse_record_data_with_arrays(self):
        """Test record data parsing with arrays"""
        handler = MultidimensionalHandler()
        
        # Mock array field info
        array_info = ArrayFieldInfo(
            base_name="TEST:BOOLPARAM",
            element_type="BYTE",
            element_size=1,
            array_size=3,
            start_offset=0,
            element_offsets=[0, 1, 2],
            is_single_field_array=True
        )
        
        # Mock analysis with arrays
        analysis = {
            'has_arrays': True,
            'array_fields': [array_info],
            'regular_fields': []
        }
        
        # Mock record data (3 bytes: True, False, True)
        data = b'\x01\x00\x01'
        
        result = handler.parse_record_data(data, analysis)
        
        assert 'TEST:BOOLPARAM' in result
        assert result['TEST:BOOLPARAM'] == [True, False, True]


class TestBaseFieldNameExtraction:
    """Test base field name extraction"""
    
    def test_get_base_field_name_single_field(self):
        """Test base field name extraction for single fields"""
        handler = MultidimensionalHandler()
        
        # Test single field (no numeric suffix)
        result = handler._get_base_field_name("TEST:ID", 2)
        assert result == "TEST:ID"  # No change for fields without numeric suffix
    
    def test_get_base_field_name_array_field(self):
        """Test base field name extraction for array fields"""
        handler = MultidimensionalHandler()
        
        # Test array field (PROD1, PROD2, etc.)
        result = handler._get_base_field_name("TEST:PROD1", 8)
        assert result == "TEST:PROD"  # Removes trailing number
    
    def test_get_base_field_name_no_number_suffix(self):
        """Test base field name extraction without number suffix"""
        handler = MultidimensionalHandler()
        
        # Test field without number suffix
        result = handler._get_base_field_name("TEST:NAME", 20)
        assert result == "TEST:NAME"  # No change for fields without numeric suffix


if __name__ == "__main__":
    pytest.main([__file__])
