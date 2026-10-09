"""
Unit tests keeping the SQLite schema and the data migration in agreement for array fields

These use synthetic table definitions so they run without any sample database.
"""

import os
import sqlite3
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from converter.multidimensional_handler import MultidimensionalHandler
from converter.sqlite_converter import SqliteConverter


def make_field(name, field_type, offset, size):
    return SimpleNamespace(name=name, type=field_type, offset=offset, size=size, array_element_count=1)


class FakeTPS:
    """Minimal stand-in for TPS: one table whose records come back as dicts"""

    def __init__(self, table_name, table_def, records):
        self.current_table_number = 1
        self.tables = SimpleNamespace(
            get_definition=lambda number: table_def,
            _TpsTablesList__tables={1: SimpleNamespace(name=table_name)},
        )
        self._table_name = table_name
        self._records = records

    def set_current_table(self, table_name):
        assert table_name == self._table_name

    def __iter__(self):
        return iter(self._records)


def migrate(table_name, table_def, records):
    """Create the schema the converter would create, migrate the records, and return the rows"""
    converter = SqliteConverter()
    conn = sqlite3.connect(':memory:')
    analysis = converter.schema_mapper.multidimensional_handler.analyze_table_structure(table_def)
    schema = converter.schema_mapper.map_table_schema_with_multidimensional(table_name, table_def, analysis)
    conn.execute(schema['create_table'])

    count = converter._migrate_table_data(FakeTPS(table_name, table_def, records), table_name,
                                          schema['table_name'], conn)
    cursor = conn.execute(f'SELECT * FROM "{schema["table_name"]}"')
    columns = [d[0] for d in cursor.description]
    return count, [dict(zip(columns, row)) for row in cursor.fetchall()]


class TestArrayGrouping:
    """Which numbered fields the handler groups into a JSON array"""

    def test_numbered_series_is_grouped(self):
        fields = [make_field(f'CUM:PROD{i}', 'DOUBLE', 4 + (i - 1) * 16, 8) for i in range(1, 4)]
        analysis = MultidimensionalHandler().analyze_table_structure(SimpleNamespace(fields=fields))

        assert [a.base_name for a in analysis['array_fields']] == ['CUM:PROD']
        assert analysis['array_fields'][0].element_names == ['CUM:PROD1', 'CUM:PROD2', 'CUM:PROD3']
        assert analysis['array_fields'][0].element_offsets == [4, 20, 36]

    def test_unsuffixed_field_and_numbered_sibling_stay_separate(self):
        # OMSG:VALUE / OMSG:VALUE2 are two distinct fields, not a two-element array
        fields = [make_field('OMSG:VALUE', 'DOUBLE', 8, 8), make_field('OMSG:VALUE2', 'DOUBLE', 16, 8)]
        analysis = MultidimensionalHandler().analyze_table_structure(SimpleNamespace(fields=fields))

        assert analysis['has_arrays'] is False
        assert [f.name for f in analysis['regular_fields']] == ['OMSG:VALUE', 'OMSG:VALUE2']

    def test_series_not_starting_at_one_stays_separate(self):
        fields = [make_field('X:LEVEL2', 'SHORT', 0, 2), make_field('X:LEVEL3', 'SHORT', 2, 2)]
        analysis = MultidimensionalHandler().analyze_table_structure(SimpleNamespace(fields=fields))

        assert analysis['has_arrays'] is False


class TestMigrationMatchesSchema:
    """Rows must land in the columns that _create_schema created"""

    def test_distinct_numbered_fields_migrate(self):
        table_def = SimpleNamespace(
            fields=[make_field('OMSG:TYPE', 'SHORT', 0, 2), make_field('OMSG:VALUE', 'DOUBLE', 2, 8),
                    make_field('OMSG:VALUE2', 'DOUBLE', 10, 8)],
            memos=[], indexes=[])
        records = [{'OMSG:TYPE': 16, 'OMSG:VALUE': 0.0, 'OMSG:VALUE2': 0.2}]

        count, rows = migrate('MODSEGMENT', table_def, records)

        assert count == 1
        assert rows == [{'TYPE': 16, 'VALUE': 0.0, 'VALUE2': 0.2}]

    def test_interleaved_multi_field_arrays_migrate_as_json(self):
        fields = [make_field('CUM:LSE_ID', 'LONG', 0, 4)]
        for i in range(1, 4):
            fields.append(make_field(f'CUM:PROD{i}', 'DOUBLE', 4 + (i - 1) * 16, 8))
            fields.append(make_field(f'CUM:PRE{i}', 'DOUBLE', 12 + (i - 1) * 16, 8))
        table_def = SimpleNamespace(fields=fields, memos=[], indexes=[])
        records = [{'CUM:LSE_ID': 2, 'CUM:PROD1': 554160.0, 'CUM:PRE1': 0.0, 'CUM:PROD2': 0.0,
                    'CUM:PRE2': 1.5, 'CUM:PROD3': 73.0, 'CUM:PRE3': 0.0}]

        count, rows = migrate('CUMVOL', table_def, records)

        assert count == 1
        assert rows == [{'LSE_ID': 2, 'PROD': '[554160.0, 0.0, 73.0]', 'PRE': '[0.0, 1.5, 0.0]'}]

    def test_arrays_with_memo_migrate(self):
        table_def = SimpleNamespace(
            fields=[make_field('TIT:OUTPUTREVCOL1', 'SHORT', 0, 2), make_field('TIT:OUTPUTREVCOL2', 'SHORT', 2, 2)],
            memos=[SimpleNamespace(name='TIT:PROJ_NOTES')], indexes=[])
        records = [{'TIT:OUTPUTREVCOL1': 3, 'TIT:OUTPUTREVCOL2': 4, 'TIT:PROJ_NOTES': 'notes'}]

        count, rows = migrate('TITLES', table_def, records)

        assert count == 1
        assert rows == [{'OUTPUTREVCOL': '[3, 4]', 'PROJ_NOTES': 'notes'}]

    def test_mock_definition_is_not_treated_as_multidimensional(self):
        # Arbitrary attribute lookups on a Mock are truthy; only an explicit True should switch paths
        from unittest.mock import Mock
        table_def = Mock(fields=[make_field('T:NAME', 'STRING', 0, 10)], memos=[], indexes=[])
        records = [{'T:NAME': 'abc'}]

        count, rows = migrate('PLAIN', table_def, records)

        assert count == 1
        assert rows == [{'NAME': 'abc'}]
