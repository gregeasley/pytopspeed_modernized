"""
Multi-dimensional field and table handler for TopSpeed to SQLite conversion
"""

import json
import struct
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass

@dataclass
class ArrayFieldInfo:
    """Information about an array field"""
    base_name: str
    element_type: str
    element_size: int
    array_size: int
    start_offset: int
    element_offsets: List[int]
    is_single_field_array: bool = False  # True if detected as single-field array, False if multi-field array

class MultidimensionalHandler:
    """Handles multi-dimensional fields and tables in TopSpeed format"""
    
    def __init__(self):
        self.array_fields: Dict[str, ArrayFieldInfo] = {}
    
    def analyze_table_structure(self, table_def) -> Dict[str, Any]:
        """Analyze table structure for multi-dimensional patterns"""
        analysis = {
            'has_arrays': False,
            'array_fields': [],
            'regular_fields': [],
            'total_record_size': 0
        }
        
        if not table_def or not table_def.fields:
            return analysis
        
        # Group fields by base name to identify arrays
        field_groups = {}
        for field in table_def.fields:
            field_size = getattr(field, 'size', 8)
            base_name = self._get_base_field_name(field.name, field_size)
            if base_name not in field_groups:
                field_groups[base_name] = []
            field_groups[base_name].append(field)
        
        # Analyze each group for array patterns
        for base_name, fields in field_groups.items():
            if len(fields) > 1:
                # Check if this looks like an array
                array_info = self._analyze_array_pattern(base_name, fields)
                if array_info:
                    array_info.is_single_field_array = False  # Mark as multi-field array
                    analysis['has_arrays'] = True
                    analysis['array_fields'].append(array_info)
                    self.array_fields[base_name] = array_info
                else:
                    # Not an array, add as regular fields
                    analysis['regular_fields'].extend(fields)
            else:
                # Single field - check if it's a single-field array
                field = fields[0]
                array_info = self._analyze_single_field_array(field)
                if array_info:
                    array_info.is_single_field_array = True  # Mark as single-field array
                    analysis['has_arrays'] = True
                    analysis['array_fields'].append(array_info)
                    self.array_fields[base_name] = array_info
                else:
                    # Regular field
                    analysis['regular_fields'].append(field)
        
        # Calculate total record size
        if analysis['regular_fields']:
            last_field = max(analysis['regular_fields'], key=lambda f: f.offset)
            analysis['total_record_size'] = last_field.offset + getattr(last_field, 'size', 8)
        
        return analysis
    
    def _get_base_field_name(self, field_name: str, field_size: int = None) -> str:
        """Extract base field name for grouping array elements"""
        import re
        
        # Pattern 1: PROD1$1, PROD1$2, etc. -> PROD1
        if '$' in field_name:
            base = field_name.split('$')[0]
            return base
        
        # Pattern 2: Check if this is a large field that should be treated as a single array
        # Large fields (like 96-byte DAT:PROD1) are single-field arrays, not multi-field arrays
        if field_size and field_size > 8:  # More than a single DOUBLE
            return field_name  # Keep the full name to treat as separate single-field array
        
        # Pattern 3: Small fields with numeric suffixes should be grouped into arrays
        # Remove trailing numbers to group CUM:PROD1, CUM:PROD2, etc. -> CUM:PROD
        base = re.sub(r'\d+$', '', field_name)
        
        # Don't remove TD suffixes as they are meaningful field names
        # (e.g., DAT:PROD1TD should not be grouped with DAT:PROD1)
        
        return base
    
    def _analyze_array_pattern(self, base_name: str, fields: List) -> ArrayFieldInfo:
        """Analyze if a group of fields forms an array pattern"""
        if len(fields) < 2:
            return None
        
        # Sort fields by offset
        fields.sort(key=lambda f: f.offset)
        
        # Check for regular spacing (array pattern)
        offsets = [f.offset for f in fields]
        if len(offsets) < 2:
            return None
        
        # Calculate differences between consecutive offsets
        differences = [offsets[i+1] - offsets[i] for i in range(len(offsets)-1)]
        
        # Check if all differences are the same (regular array)
        if len(set(differences)) == 1:
            # For interleaved fields, the difference might be larger than the actual element size
            # Use the field size as the element size instead of the offset difference
            element_size = getattr(fields[0], 'size', 8)
            array_size = len(fields)
            
            # Get element type from first field
            element_type = fields[0].type
            
            return ArrayFieldInfo(
                base_name=base_name,
                element_type=element_type,
                element_size=element_size,
                array_size=array_size,
                start_offset=offsets[0],
                element_offsets=offsets
            )
        
        return None
    
    def _analyze_single_field_array(self, field) -> ArrayFieldInfo:
        """Analyze if a single field contains an array using the field's array_element_count attribute"""
        # Use the field's array_element_count attribute to determine if it's an array
        array_element_count = getattr(field, 'array_element_count', 1)
        array_element_size = getattr(field, 'array_element_size', None)
        field_size = getattr(field, 'size', 0)
        field_type = field.type
        
        # If array_element_count > 1, this is definitely an array
        if array_element_count > 1:
            # Calculate element size
            if array_element_size is not None:
                element_size = array_element_size
            else:
                # Fallback: calculate element size from total size and count
                element_size = field_size // array_element_count if array_element_count > 0 else 1
            
            return ArrayFieldInfo(
                base_name=field.name,
                element_type=field_type,
                element_size=element_size,
                array_size=array_element_count,
                start_offset=field.offset,
                element_offsets=[field.offset + i * element_size for i in range(array_element_count)]
            )
        
        # If array_element_count == 1, this is a single field (not an array)
        return None
    
    def parse_record_data(self, data: bytes, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Parse record data handling multi-dimensional fields"""
        parsed_data = {}
        
        # Parse regular fields
        for field in analysis['regular_fields']:
            field_name = field.name
            field_value = self._parse_field_value(data, field)
            parsed_data[field_name] = field_value
        
        # Parse array fields
        for array_info in analysis['array_fields']:
            array_data = self._parse_array_field(data, array_info)
            parsed_data[array_info.base_name] = array_data
        
        return parsed_data
    
    def _parse_field_value(self, data: bytes, field) -> Any:
        """Parse a single field value from data"""
        try:
            offset = field.offset
            field_type = field.type
            field_size = getattr(field, 'size', 8)
            
            if offset >= len(data):
                return None
            
            field_data = data[offset:offset + field_size]
            
            if field_type == 'STRING':
                return field_data.decode('ascii', errors='replace').rstrip('\x00')
            elif field_type == 'DOUBLE':
                if len(field_data) >= 8:
                    return struct.unpack('<d', field_data[:8])[0]
            elif field_type == 'SHORT':
                if len(field_data) >= 2:
                    return struct.unpack('<h', field_data[:2])[0]
            elif field_type == 'LONG':
                if len(field_data) >= 4:
                    return struct.unpack('<i', field_data[:4])[0]
            elif field_type == 'BYTE':
                if len(field_data) >= 1:
                    # Convert BYTE to boolean: 0 = False, non-zero = True
                    return bool(field_data[0])
            elif field_type in ['BOOL', 'BOOLEAN']:
                if len(field_data) >= 1:
                    # Convert BOOL/BOOLEAN to boolean: 0 = False, non-zero = True
                    return bool(field_data[0])
            
            return field_data.hex()
            
        except Exception as e:
            return None
    
    def _parse_array_field(self, data: bytes, array_info: ArrayFieldInfo) -> List[Any]:
        """Parse an array field from data"""
        array_data = []
        
        for i, offset in enumerate(array_info.element_offsets):
            if offset < len(data):
                # Create a mock field for this array element
                mock_field = type('MockField', (), {
                    'offset': offset,
                    'type': array_info.element_type,
                    'size': array_info.element_size
                })()
                
                element_value = self._parse_field_value(data, mock_field)
                array_data.append(element_value)
            else:
                array_data.append(None)
        
        return array_data
    
    def create_sqlite_schema(self, table_name: str, analysis: Dict[str, Any]) -> str:
        """Create SQLite schema for a table with multi-dimensional fields"""
        columns = []
        
        # Add regular fields
        for field in analysis['regular_fields']:
            field_name = self._sanitize_field_name(field.name)
            sqlite_type = self._get_sqlite_type(field.type)
            columns.append(f'"{field_name}" {sqlite_type}')
        
        # Add array fields as JSON
        for array_info in analysis['array_fields']:
            field_name = self._sanitize_field_name(array_info.base_name)
            columns.append(f'"{field_name}" TEXT')  # JSON stored as TEXT
        
        return f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
    
    def _sanitize_field_name(self, field_name: str) -> str:
        """Sanitize field name for SQLite"""
        # Remove table prefix (e.g., "DAT:LSE_ID" -> "LSE_ID")
        if ':' in field_name:
            sanitized = field_name.split(':', 1)[1]  # Take everything after the first colon
        else:
            sanitized = field_name
            
        # Replace problematic characters
        sanitized = sanitized.replace(' ', '_')  # Replace spaces with underscores
        sanitized = sanitized.replace('-', '_')  # Replace hyphens with underscores
        sanitized = sanitized.replace('.', '_')  # Replace dots with underscores
        sanitized = sanitized.replace('/', '_')  # Replace forward slashes with underscores
        sanitized = sanitized.replace('\\', '_')  # Replace backslashes with underscores
        
        # Ensure name starts with letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = '_' + sanitized
            
        # Limit length (SQLite doesn't have strict limits, but 64 chars is reasonable)
        if len(sanitized) > 64:
            sanitized = sanitized[:64]
            
        return sanitized
    
    def _get_sqlite_type(self, field_type: str) -> str:
        """Map TopSpeed field type to SQLite type"""
        type_mapping = {
            'STRING': 'TEXT',
            'DOUBLE': 'REAL',
            'SHORT': 'INTEGER',
            'LONG': 'INTEGER',
            'BYTE': 'INTEGER'
        }
        return type_mapping.get(field_type, 'TEXT')
    
    def prepare_record_for_sqlite(self, parsed_data: Dict[str, Any]) -> Tuple[List[str], List[Any]]:
        """Prepare parsed record data for SQLite insertion"""
        columns = []
        values = []
        
        for field_name, value in parsed_data.items():
            columns.append(f'"{self._sanitize_field_name(field_name)}"')
            
            if isinstance(value, list):
                # Array field - store as JSON
                values.append(json.dumps(value))
            else:
                values.append(value)
        
        return columns, values
