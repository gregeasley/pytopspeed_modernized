#!/usr/bin/env python3
"""
Unit tests for the TopSpeed file writer (pytopspeed.tpswriter)
"""

import random
import struct
import sys
import warnings
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed import TPS
from pytopspeed import tpswriter
from pytopspeed.tpswriter import (build_tps, encode_records, index_key_bytes, read_raw_records,
                                  rle_compress, rle_decompress)
from tps_builder import build_file, wells_table

pytestmark = pytest.mark.real_topspeed


class TestRle:

    @pytest.mark.parametrize('data', [
        b'',
        b'a',
        b'abc',
        b'\x00' * 10,
        b'ab' + b'\x00' * 300 + b'cd',
        b'x' * 40000,                       # run longer than one count can hold
        bytes(range(256)) * 200,            # literal longer than one count can hold
        b'aab' * 50,
    ], ids=['empty', 'one', 'literal', 'run', 'mixed', 'long-run', 'long-literal', 'short-runs'])
    def test_round_trip(self, data):
        assert rle_decompress(rle_compress(data)) == data

    def test_random_round_trip(self):
        rng = random.Random(7)
        for _ in range(200):
            data = bytes(rng.choice(b'\x00\x00\x00 ab') for _ in range(rng.randint(0, 2000)))
            assert rle_decompress(rle_compress(data)) == data

    def test_runs_shrink(self):
        assert len(rle_compress(b'\x00' * 1000)) < 10


class TestRecordEncoding:

    def test_first_record_states_both_sizes(self):
        encoded = encode_records([(0, b'')])
        assert encoded == b'\xc0\x00\x00\x00\x00'

    def test_shared_prefix_and_carried_sizes(self):
        first = b'\x00\x00\x00\x01\xf3\x00\x00\x00\x01' + b'AAAA'
        second = b'\x00\x00\x00\x01\xf3\x00\x00\x00\x02' + b'BBBB'
        encoded = encode_records([(9, first), (9, second)])
        # Second record: 8 shared bytes, sizes unchanged, so only the flag byte and the tail
        assert encoded[len(encode_records([(9, first)])):] == bytes([8]) + second[8:]


class TestBuildTps:

    def test_reader_reads_back_every_record(self, tmp_path):
        records = [(9, struct.pack('>IBI', 1, 0xF3, n) + bytes([n % 256]) * 20) for n in range(1, 3001)]
        path = tmp_path / 'many.tps'
        path.write_bytes(build_tps(records, last_issued_row=3000))

        tps = TPS(str(path), check=False)
        assert sorted(read_raw_records(tps)) == sorted(records + [(0, b'')])
        assert tps.header.last_issued_row == 3000

    def test_multi_level_tree(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tpswriter, 'MAX_PAGE_SIZE', 300)
        records = [(9, struct.pack('>IBI', 1, 0xF3, n) + b'payload') for n in range(1, 2001)]
        path = tmp_path / 'deep.tps'
        path.write_bytes(build_tps(records))

        with warnings.catch_warnings():
            warnings.simplefilter('error')
            tps = TPS(str(path), check=True)
        levels = {tps.pages[ref].hierarchy_level for ref in tps.pages.list()}
        assert max(levels) >= 2
        assert len(read_raw_records(tps)) == 2001

    def test_duplicate_keys_rejected(self):
        record = (9, struct.pack('>IBI', 1, 0xF3, 1) + b'x')
        with pytest.raises(ValueError, match='Duplicate record key'):
            build_tps([record, record])

    def test_synthetic_table_is_readable(self, tmp_path):
        path = build_file(tmp_path / 'Sample.PHD', [wells_table()])

        tps = TPS(str(path), encoding='cp1251', check=True)
        tps.set_current_table('WELLS')
        rows = list(tps)

        assert [r['WEL:NAME'] for r in rows] == ['Alpha 1', 'Bravo 2', 'Charlie 3']


class TestIndexKeys:

    @pytest.mark.parametrize('field_type, fmt, values', [
        ('SHORT', '<h', [-32768, -5, 0, 7, 32767]),
        ('LONG', '<i', [-2 ** 31, -1, 0, 1, 2 ** 31 - 1]),
        ('DOUBLE', '<d', [-1e300, -2.5, -0.5, 0.0, 0.25, 3.0, 1e300]),
        ('BYTE', '<B', [0, 1, 128, 255]),
    ])
    def test_byte_order_matches_value_order(self, field_type, fmt, values):
        keys = [index_key_bytes(field_type, struct.pack(fmt, v)) for v in values]
        assert keys == sorted(keys)

    def test_descending_reverses_order(self):
        keys = [index_key_bytes('LONG', struct.pack('<i', v), descending=True) for v in (1, 2, 3)]
        assert keys == sorted(keys, reverse=True)

    def test_nocase_strings_are_lowercased(self):
        assert index_key_bytes('STRING', b'ABC  ', nocase=True) == b'abc  '
        assert index_key_bytes('STRING', b'ABC  ') == b'ABC  '
