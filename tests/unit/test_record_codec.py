#!/usr/bin/env python3
"""
Unit tests for RecordCodec: column layout plus payload decode/encode
"""

import json
import struct
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed.tpstable import TABLE_DEFINITION_STRUCT
from converter.record_codec import RecordCodec, memo_chunks
from tps_builder import field, index, memo, table_definition


def codec_for(fields, record_size, memos=(), indexes=()):
    definition = TABLE_DEFINITION_STRUCT.parse(
        TABLE_DEFINITION_STRUCT.build(table_definition(record_size, fields, memos, indexes)))
    return RecordCodec(definition)


class TestColumnLayout:

    def test_scalars_arrays_and_memos(self):
        codec = codec_for([field('LONG', 0, 'T:ID', 4),
                           field('DOUBLE', 4, 'T:RATE', 24, count=3, number=1)],
                          record_size=28, memos=[memo('T:NOTES')])

        assert [(c.name, c.sqlite_type, c.is_array) for c in codec.columns] == [
            ('ID', 'INTEGER', False), ('RATE', 'TEXT', True), ('NOTES', 'BLOB', False)]
        assert codec.columns[1].offsets == [4, 12, 20]

    def test_group_members_become_arrays(self):
        # GROUP of 2 elements, 3 bytes each: CODE (BYTE) + AMOUNT (SHORT)
        codec = codec_for([field('SHORT', 0, 'T:ID', 2),
                           field('GROUP', 2, 'T:ITEMS', 6, count=2, number=1),
                           field('BYTE', 2, 'T:CODE', 1, number=2),
                           field('SHORT', 3, 'T:AMOUNT', 2, number=3)],
                          record_size=8)

        names = [c.name for c in codec.columns]
        assert names == ['ID', 'CODE', 'AMOUNT']
        assert codec.columns[1].offsets == [2, 5]
        assert codec.columns[2].offsets == [3, 6]

    def test_numbered_fields_become_one_array(self):
        codec = codec_for([field('DOUBLE', 0, 'C:PROD1', 8), field('DOUBLE', 8, 'C:PROD2', 8, number=1),
                           field('DOUBLE', 16, 'C:VALUE', 8, number=2), field('DOUBLE', 24, 'C:VALUE2', 8, number=3)],
                          record_size=32)

        assert [c.name for c in codec.columns] == ['PROD', 'VALUE', 'VALUE2']

    def test_index_columns(self):
        codec = codec_for([field('LONG', 0, 'T:ID', 4), field('DOUBLE', 4, 'T:RATE', 16, count=2, number=1)],
                          record_size=20, indexes=[index('T:BY_ID', [0]), index('T:BY_RATE', [1])])

        assert codec.index_columns(codec.table_def.indexes[0]) == ['ID']
        assert codec.index_columns(codec.table_def.indexes[1]) is None


class TestValues:

    @pytest.fixture
    def codec(self):
        return codec_for([
            field('BYTE', 0, 'T:B', 1),
            field('SHORT', 1, 'T:S', 2, number=1),
            field('USHORT', 3, 'T:US', 2, number=2),
            field('LONG', 5, 'T:L', 4, number=3),
            field('ULONG', 9, 'T:UL', 4, number=4),
            field('FLOAT', 13, 'T:F', 4, number=5),
            field('DOUBLE', 17, 'T:D', 8, number=6),
            field('DECIMAL', 25, 'T:DEC', 4, number=7, decimal_count=2),
            field('STRING', 29, 'T:STR', 8, number=8),
            field('CSTRING', 37, 'T:CS', 6, number=9),
            field('PSTRING', 43, 'T:PS', 6, number=10),
            field('DATE', 49, 'T:DT', 4, number=11),
            field('TIME', 53, 'T:TM', 4, number=12),
        ], record_size=57)

    def test_round_trip(self, codec):
        values = (200, -3, 65000, -100000, 4000000000, 1.5, -2.25, -12.34, 'abc', 'xyz', 'pq',
                  '2024-02-29', '13:45:07.50')
        payload = codec.encode(values)

        assert len(payload) == 57
        assert codec.decode(payload) == values

    def test_encode_only_listed_columns_keeps_other_bytes(self, codec):
        template = bytes(range(57))
        payload = codec.encode((0,) * 13, template=template, columns=['S'])

        assert payload[1:3] == b'\x00\x00'
        assert payload[:1] + payload[3:] == template[:1] + template[3:]

    def test_nan_reads_as_null(self):
        codec = codec_for([field('DOUBLE', 0, 'T:D', 8), field('DOUBLE', 8, 'T:A', 16, count=2, number=1)],
                          record_size=24)
        payload = struct.pack('<3d', float('nan'), 1.0, float('nan'))

        assert codec.decode(payload) == (None, json.dumps([1.0, None]))

    def test_errors_name_the_column(self):
        codec = codec_for([field('DOUBLE', 0, 'T:A', 16, count=2), field('DECIMAL', 16, 'T:DEC', 2, number=1,
                                                                            decimal_count=0)],
                          record_size=18)

        with pytest.raises(ValueError, match='A'):
            codec.encode(('[1, 2, 3]', 0))
        with pytest.raises(ValueError, match='DEC'):
            codec.encode(('[1, 2]', 123456))


class TestMemos:

    def test_memo_chunks(self):
        assert memo_chunks(b'a' * 600) == [b'a' * 256, b'a' * 256, b'a' * 88]
        assert memo_chunks(b'') == [b'\x00']

    def test_memo_values(self):
        codec = codec_for([field('LONG', 0, 'T:ID', 4)], record_size=4, memos=[memo('T:NOTES')])

        assert codec.encode_memo(None) is None
        assert codec.encode_memo('') == b'\x00'
        assert codec.decode_memo(codec.encode_memo('note')) == 'note'
        assert codec.decode((struct.pack('<i', 1)), {0: b'\x00'}) == (1, '')
