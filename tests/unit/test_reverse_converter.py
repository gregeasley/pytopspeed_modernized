#!/usr/bin/env python3
"""
Unit tests for reverse conversion (SQLite back to TopSpeed)

Each test builds a synthetic TopSpeed file, converts it to SQLite, optionally edits the
database, converts it back, and reads the result with pytopspeed.
"""

import json
import os
import sqlite3
import struct
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pytopspeed import TPS
from pytopspeed.tpswriter import index_records, read_raw_records
from converter.reverse_converter import ReverseConverter
from converter.sqlite_converter import SqliteConverter
from tps_builder import build_file, wells_table

pytestmark = pytest.mark.real_topspeed


def convert(source, db_path):
    results = SqliteConverter().convert(str(source), str(db_path))
    assert results['success'], results['errors']
    return db_path


def reverse(db_path, out_dir):
    return ReverseConverter().convert_sqlite_to_topspeed(str(db_path), str(out_dir))


def file_records(path):
    return sorted(read_raw_records(TPS(str(path), encoding='cp1251', cached=True, check=False)))


def wells_rows(path):
    tps = TPS(str(path), encoding='cp1251', cached=True, check=False)
    tps.set_current_table('WELLS')
    return sorted(tps, key=lambda row: row['WEL:ID'])


def assert_indexes_match_data(path):
    tps = TPS(str(path), encoding='cp1251', cached=True, check=False)
    definition = tps.tables.get_definition(tps.tables.get_number('WELLS'))
    actual, expected = [], []
    for header_size, record in read_raw_records(tps):
        if len(record) > 4 and record[0] != 0xFE and struct.unpack('>I', record[:4])[0] == 1:
            if record[4] < 0xF0:
                actual.append((header_size, record))
            elif record[4] == 0xF3:
                expected.extend(index_records(1, definition, record[9:], struct.unpack('>I', record[5:9])[0]))
    assert sorted(actual) == sorted(expected)


@pytest.fixture
def source(tmp_path):
    return build_file(tmp_path / 'Sample.PHD', [wells_table()])


class TestUnchangedRoundTrip:

    def test_rebuilds_every_record(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')

        results = reverse(db, tmp_path / 'out')

        assert results['success'], results['errors']
        rebuilt = tmp_path / 'out' / 'Sample.PHD'
        assert results['files_created'] == [str(rebuilt)]
        assert results['records_processed'] == 3
        assert file_records(rebuilt) == file_records(source)

    def test_keeps_original_file_names_for_combined_databases(self, tmp_path):
        phd = build_file(tmp_path / 'Model.PHD', [wells_table()])
        mod = build_file(tmp_path / 'Model.mod', [wells_table()])
        db = tmp_path / 'combined.sqlite'
        assert SqliteConverter().convert_multiple([str(phd), str(mod)], str(db))['success']

        results = reverse(db, tmp_path / 'out')

        assert results['success'], results['errors']
        assert sorted(os.path.basename(f) for f in results['files_created']) == ['Model.PHD', 'Model.mod']
        assert file_records(tmp_path / 'out' / 'Model.mod') == file_records(mod)


class TestEdits:

    def test_changed_value_is_written_and_indexes_follow(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE WELLS SET NAME = 'Zulu 1', RATE = '[1.5, 2.5]' WHERE ID = 1")

        results = reverse(db, tmp_path / 'out')

        assert results['success'], results['errors']
        rebuilt = tmp_path / 'out' / 'Sample.PHD'
        first = wells_rows(rebuilt)[0]
        assert first['WEL:NAME'] == 'Zulu 1'
        assert_indexes_match_data(rebuilt)
        # Bytes of untouched rows are unchanged
        original = {r for r in file_records(source) if r[1][4:5] == b'\xf3' and r[1][5:9] != struct.pack('>I', 1)}
        assert original <= set(file_records(rebuilt))

    def test_array_column_edit(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE WELLS SET RATE = '[7.0, 8.0]' WHERE ID = 2")

        reverse(db, tmp_path / 'out')
        again = convert(tmp_path / 'out' / 'Sample.PHD', tmp_path / 'again.sqlite')

        with sqlite3.connect(again) as conn:
            assert json.loads(conn.execute('SELECT RATE FROM WELLS WHERE ID = 2').fetchone()[0]) == [7.0, 8.0]

    def test_insert_and_delete(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute('DELETE FROM WELLS WHERE ID = 2')
            conn.execute("INSERT INTO WELLS (ID, NAME, RATE, STATUS, NOTES) VALUES (4, 'Delta 4', '[3.0, 4.0]', 1, 'new')")

        results = reverse(db, tmp_path / 'out')

        assert results['success'], results['errors']
        rebuilt = tmp_path / 'out' / 'Sample.PHD'
        rows = wells_rows(rebuilt)
        assert [r['WEL:ID'] for r in rows] == [1, 3, 4]
        assert rows[-1]['WEL:NAME'] == 'Delta 4'
        # The new row gets a fresh record number
        assert rows[-1]["b':RecNo'"] == 4
        assert_indexes_match_data(rebuilt)
        # The deleted row's memo is gone with it
        memo_owners = {struct.unpack('>I', r[5:9])[0] for _, r in file_records(rebuilt)
                       if len(r) > 4 and r[0] != 0xFE and r[4] == 0xFC}
        assert memo_owners == {1, 4}

    def test_memo_edit_round_trips(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        long_text = 'Revised notes ' * 40
        with sqlite3.connect(db) as conn:
            conn.execute('UPDATE WELLS SET NOTES = ? WHERE ID = 1', (long_text,))

        reverse(db, tmp_path / 'out')
        again = convert(tmp_path / 'out' / 'Sample.PHD', tmp_path / 'again.sqlite')

        with sqlite3.connect(again) as conn:
            assert conn.execute('SELECT NOTES FROM WELLS WHERE ID = 1').fetchone()[0] == long_text
            assert conn.execute('SELECT NOTES FROM WELLS WHERE ID = 2').fetchone()[0] == 'x' * 600

    def test_rows_still_match_after_vacuum_renumbers_them(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute('DELETE FROM WELLS WHERE ID = 1')
        with sqlite3.connect(db) as conn:
            conn.execute('VACUUM')

        reverse(db, tmp_path / 'out')

        rebuilt = tmp_path / 'out' / 'Sample.PHD'
        kept = {r for r in file_records(source) if r[1][4:5] == b'\xf3' and r[1][5:9] != struct.pack('>I', 1)}
        assert kept <= set(file_records(rebuilt))
        assert [r["b':RecNo'"] for r in wells_rows(rebuilt)] == [2, 3]


class TestErrors:

    def test_duplicate_unique_key_is_reported(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute('UPDATE WELLS SET ID = 1 WHERE ID = 2')

        results = reverse(db, tmp_path / 'out')

        assert results['success'] is False
        assert 'unique key WEL:BY_ID' in results['errors'][0]
        assert not (tmp_path / 'out' / 'Sample.PHD').exists()

    def test_value_that_does_not_fit_is_reported(self, source, tmp_path):
        db = convert(source, tmp_path / 'sample.sqlite')
        with sqlite3.connect(db) as conn:
            conn.execute("UPDATE WELLS SET RATE = '[1, 2, 3]' WHERE ID = 1")

        results = reverse(db, tmp_path / 'out')

        assert results['success'] is False
        assert 'RATE' in results['errors'][0]

    def test_database_without_metadata(self, tmp_path):
        db = tmp_path / 'plain.sqlite'
        with sqlite3.connect(db) as conn:
            conn.execute('CREATE TABLE T (A INTEGER)')

        results = ReverseConverter().convert_sqlite_to_topspeed(str(db), str(tmp_path / 'out'))

        assert results['success'] is False
        assert 'no TopSpeed metadata' in results['errors'][0]

    def test_missing_file(self, tmp_path):
        results = ReverseConverter().convert_sqlite_to_topspeed(str(tmp_path / 'nope.sqlite'), str(tmp_path))

        assert results['success'] is False
        assert 'not found' in results['errors'][0]
