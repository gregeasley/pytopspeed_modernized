"""
Record codec: maps a TopSpeed table definition to SQLite columns, decodes DATA record
payloads into column values, and encodes column values back into payloads.

Schema creation, data migration and reverse conversion all use the same codec, so a
value read out of a TopSpeed file can be written back to exactly the bytes it came from.

Column layout:
- a scalar field becomes one column named after the field (table prefix removed)
- a field declared with DIM(n) becomes one JSON array column
- members of a GROUP declared with DIM(n) each become a JSON array column; the GROUP
  itself has no column because its bytes are exactly its members
- numbered scalar fields NAME1..NAMEn become one JSON array column NAME
  (the MultidimensionalHandler grouping rule)
- each memo becomes one column
"""

import json
import math
import struct
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Dict, List, Optional, Sequence

from .multidimensional_handler import MultidimensionalHandler

SQLITE_TYPES = {
    'BYTE': 'INTEGER', 'SHORT': 'INTEGER', 'USHORT': 'INTEGER', 'LONG': 'INTEGER', 'ULONG': 'INTEGER',
    'FLOAT': 'REAL', 'DOUBLE': 'REAL', 'DECIMAL': 'REAL',
    'STRING': 'TEXT', 'CSTRING': 'TEXT', 'PSTRING': 'TEXT', 'DATE': 'TEXT', 'TIME': 'TEXT',
}

_STRUCT_FORMATS = {
    'BYTE': '<B', 'SHORT': '<h', 'USHORT': '<H', 'LONG': '<i', 'ULONG': '<I', 'FLOAT': '<f', 'DOUBLE': '<d',
}

MEMO_CHUNK_SIZE = 256


@dataclass
class Column:
    """One SQLite column and where its value lives in the record"""
    name: str
    sqlite_type: str
    field_type: str
    element_size: int
    offsets: List[int]
    is_array: bool
    decimal_count: int = 0
    source_fields: List[str] = dataclass_field(default_factory=list)
    memo_index: Optional[int] = None

    @property
    def is_memo(self) -> bool:
        return self.memo_index is not None


class RecordCodec:
    """Column mapping plus payload decode/encode for one table definition"""

    def __init__(self, table_def, encoding: str = 'cp1251', sanitize=None):
        self.table_def = table_def
        self.encoding = encoding
        self.record_size = table_def.record_size
        self._sanitize = sanitize or MultidimensionalHandler()._sanitize_field_name
        self.columns: List[Column] = []
        self._field_columns: Dict[int, Column] = {}  # definition field index -> column holding it
        self._build_columns()

    # ------------------------------------------------------------------ layout

    def _build_columns(self):
        fields = list(self.table_def.fields)
        group_members = self._group_members(fields)

        # Numbered scalar fields (CUM:PROD1..CUM:PROD5) are presented as one array
        handler = MultidimensionalHandler()
        candidates = {}
        for index, f in enumerate(fields):
            if self._type(f) == 'GROUP' or index in group_members or f.array_element_count > 1:
                continue
            base = handler._get_base_field_name(str(f.name), f.size)
            candidates.setdefault(base, []).append((index, f))
        numbered = {}
        for base, members in candidates.items():
            if len(members) < 2 or len({self._type(f) for _, f in members}) != 1:
                continue
            info = handler._analyze_array_pattern(base, [f for _, f in members])
            if info:
                numbered[members[0][0]] = (base, [i for i, _ in members])

        skip = {i for _, indexes in numbered.values() for i in indexes[1:]}
        for index, f in enumerate(fields):
            ftype = self._type(f)
            if ftype == 'GROUP' or index in skip:
                continue
            if index in numbered:
                base, indexes = numbered[index]
                members = sorted((fields[i] for i in indexes), key=lambda m: m.offset)
                column = self._add(base, ftype, members[0], [m.offset for m in members], True,
                                   [str(m.name) for m in members])
                for i in indexes:
                    self._field_columns[i] = column
                continue
            count = f.array_element_count
            element_size = f.size // count if count > 1 else f.size
            offsets = [f.offset + i * element_size for i in range(count)]
            if index in group_members:
                group_offset, group_count, stride = group_members[index]
                offsets = [o + g * stride for g in range(group_count) for o in offsets]
            column = self._add(str(f.name), ftype, f, offsets, len(offsets) > 1, [str(f.name)],
                               element_size=element_size)
            self._field_columns[index] = column

        for memo_index, memo in enumerate(self.table_def.memos):
            column = Column(name=self._unique(self._sanitize(str(memo.name))), sqlite_type='BLOB',
                            field_type='MEMO', element_size=0, offsets=[], is_array=False,
                            source_fields=[str(memo.name)], memo_index=memo_index)
            self.columns.append(column)

    def _group_members(self, fields) -> Dict[int, tuple]:
        """Map member field index -> (group offset, group DIM count, group element size)"""
        members = {}
        for index, f in enumerate(fields):
            if self._type(f) != 'GROUP' or f.array_element_count <= 1:
                continue
            stride = f.size // f.array_element_count
            for member_index in range(index + 1, len(fields)):
                m = fields[member_index]
                if not (f.offset <= m.offset < f.offset + stride):
                    break
                if member_index not in members:
                    members[member_index] = (f.offset, f.array_element_count, stride)
        return members

    def _add(self, name, ftype, f, offsets, is_array, sources, element_size=None) -> Column:
        sqlite_type = 'TEXT' if is_array else SQLITE_TYPES.get(ftype, 'BLOB')
        column = Column(name=self._unique(self._sanitize(name)), sqlite_type=sqlite_type, field_type=ftype,
                        element_size=element_size if element_size is not None else f.size,
                        offsets=offsets, is_array=is_array,
                        decimal_count=getattr(f, 'decimal_count', None) or 0, source_fields=sources)
        self.columns.append(column)
        return column

    def _unique(self, name: str) -> str:
        existing = {c.name.upper() for c in self.columns}
        candidate, n = name, 2
        while candidate.upper() in existing:
            candidate = f'{name}_{n}'
            n += 1
        return candidate

    @staticmethod
    def _type(f) -> str:
        return str(f.type)

    @property
    def data_columns(self) -> List[Column]:
        return [c for c in self.columns if not c.is_memo]

    @property
    def memo_columns(self) -> List[Column]:
        return [c for c in self.columns if c.is_memo]

    def index_columns(self, index_def) -> Optional[List[str]]:
        """Column names for an index, or None if a key field isn't a standalone column"""
        names = []
        for index_field in index_def.fields:
            column = self._field_columns.get(index_field.field_number)
            if column is None or column.is_array:
                return None
            names.append(column.name)
        return names

    # ------------------------------------------------------------------ decode

    def decode(self, payload: bytes, memos: Optional[Dict[int, bytes]] = None) -> tuple:
        """Payload bytes (+ raw memo bytes by memo index) -> column values in column order"""
        values = []
        for column in self.columns:
            if column.is_memo:
                raw = (memos or {}).get(column.memo_index or 0)
                values.append(self.decode_memo(raw))
                continue
            elements = [self.decode_element(column, payload[o:o + column.element_size]) for o in column.offsets]
            # NaN/inf aren't valid JSON, and SQLite stores a NaN REAL as NULL anyway
            elements = [None if isinstance(e, float) and not math.isfinite(e) else e for e in elements]
            values.append(json.dumps(elements) if column.is_array else elements[0])
        return tuple(values)

    def decode_element(self, column: Column, data: bytes) -> Any:
        ftype = column.field_type
        if len(data) < column.element_size:
            return None
        if ftype in _STRUCT_FORMATS:
            return struct.unpack(_STRUCT_FORMATS[ftype], data)[0]
        if ftype == 'DECIMAL':
            return _decode_bcd(data, column.decimal_count)
        if ftype == 'STRING':
            return data.decode(self.encoding, errors='replace').rstrip(' \x00')
        if ftype == 'CSTRING':
            return data.split(b'\x00', 1)[0].decode(self.encoding, errors='replace').rstrip(' ')
        if ftype == 'PSTRING':
            return data[1:1 + data[0]].decode(self.encoding, errors='replace')
        if ftype == 'DATE':
            day, month, year = data[0], data[1], struct.unpack('<H', data[2:4])[0]
            return None if year == 0 else f'{year:04d}-{month:02d}-{day:02d}'
        if ftype == 'TIME':
            centisecond, second, minute, hour = data[0], data[1], data[2], data[3]
            return f'{hour:02d}:{minute:02d}:{second:02d}.{centisecond:02d}'
        return bytes(data)

    def decode_memo(self, raw: Optional[bytes]) -> Optional[str]:
        if raw is None:
            return None
        return raw.rstrip(b'\x00').decode(self.encoding, errors='replace')

    # ------------------------------------------------------------------ encode

    def encode(self, values: Sequence[Any], template: Optional[bytes] = None,
               columns: Optional[Sequence[str]] = None) -> bytes:
        """
        Column values -> payload bytes.

        Args:
            values: values in column order (memo columns are ignored)
            template: payload to start from; bytes not covered by an encoded column are kept
            columns: names of the columns to encode (default: all data columns)
        """
        payload = bytearray(template if template is not None else b'\x00' * self.record_size)
        if len(payload) < self.record_size:
            payload.extend(b'\x00' * (self.record_size - len(payload)))
        wanted = set(columns) if columns is not None else None
        for column, value in zip(self.columns, values):
            if column.is_memo or (wanted is not None and column.name not in wanted):
                continue
            if column.is_array:
                elements = json.loads(value) if isinstance(value, str) else value
                if elements is None:
                    elements = []
                if not isinstance(elements, list) or len(elements) > len(column.offsets):
                    raise ValueError(f'{column.name}: expected a JSON array of up to '
                                     f'{len(column.offsets)} elements')
                elements = list(elements) + [None] * (len(column.offsets) - len(elements))
            else:
                elements = [value]
            for offset, element in zip(column.offsets, elements):
                payload[offset:offset + column.element_size] = self.encode_element(column, element)
        return bytes(payload)

    def encode_element(self, column: Column, value: Any) -> bytes:
        ftype = column.field_type
        size = column.element_size
        try:
            if ftype in _STRUCT_FORMATS:
                if value is None:
                    value = 0
                if ftype in ('FLOAT', 'DOUBLE'):
                    return struct.pack(_STRUCT_FORMATS[ftype], float(value))
                return struct.pack(_STRUCT_FORMATS[ftype], int(value))
            if ftype == 'DECIMAL':
                return _encode_bcd(value or 0, size, column.decimal_count)
            if ftype in ('STRING', 'CSTRING', 'PSTRING'):
                text = '' if value is None else str(value)
                data = text.encode(self.encoding, errors='replace')
                if ftype == 'STRING':
                    return data[:size].ljust(size, b' ')
                if ftype == 'CSTRING':
                    return data[:size - 1].ljust(size, b'\x00')
                data = data[:size - 1]
                return (bytes([len(data)]) + data).ljust(size, b' ')
            if ftype == 'DATE':
                if not value:
                    return b'\x00' * 4
                year, month, day = (int(part) for part in str(value)[:10].split('-'))
                return bytes([day, month]) + struct.pack('<H', year)
            if ftype == 'TIME':
                if not value:
                    return b'\x00' * 4
                clock, _, fraction = str(value).partition('.')
                hour, minute, second = (int(part) for part in clock.split(':'))
                centisecond = int((fraction + '00')[:2]) if fraction else 0
                return bytes([centisecond, second, minute, hour])
            if isinstance(value, (bytes, bytearray)):
                return bytes(value[:size]).ljust(size, b'\x00')
        except (ValueError, TypeError, struct.error) as e:
            raise ValueError(f'{column.name}: cannot store {value!r} as {ftype}: {e}') from e
        raise ValueError(f'{column.name}: cannot store {value!r} as {ftype}')

    def encode_memo(self, value: Any) -> Optional[bytes]:
        """Memo column value -> raw memo bytes (None means no memo records)"""
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            data = bytes(value)
        else:
            data = str(value).encode(self.encoding, errors='replace')
        # TopSpeed stores an empty memo as a single zero byte
        return data or b'\x00'


def memo_chunks(data: bytes) -> List[bytes]:
    """Split raw memo bytes into the chunks TopSpeed stores as separate records"""
    return [data[i:i + MEMO_CHUNK_SIZE] for i in range(0, len(data), MEMO_CHUNK_SIZE)] or [b'\x00']


def _decode_bcd(data: bytes, decimal_count: int) -> float:
    # The leading nibble is 0xF for negative values
    digits = data.hex()
    negative = digits[0] == 'f'
    digits = '0' + digits[1:] if negative else digits
    return (-1 if negative else 1) * int(digits) / 10 ** decimal_count


def _encode_bcd(value, size: int, decimal_count: int) -> bytes:
    scaled = int(round(abs(float(value)) * 10 ** decimal_count))
    digits = str(scaled).rjust(size * 2, '0')
    if len(digits) > size * 2 or (value < 0 and digits[0] != '0'):
        raise ValueError(f'{value} does not fit in DECIMAL({size * 2 - 1},{decimal_count})')
    if value < 0:
        digits = 'f' + digits[1:]
    return bytes.fromhex(digits)
