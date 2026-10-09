"""
Build small synthetic TopSpeed files for tests

Tests can't ship real PHDWin databases, so these helpers assemble a file from a table
definition and rows using the same record layout TopSpeed uses.
"""

import os
import struct
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pytopspeed.tpstable import TABLE_DEFINITION_STRUCT
from pytopspeed.tpswriter import (DATA_TYPE, TABLE_DEFINITION_TYPE, build_tps, data_record, index_records,
                                  memo_records, metadata_record)


def field(field_type, offset, name, size, count=1, number=0, decimal_count=None):
    definition = dict(type=field_type, offset=offset, name=name, array_element_count=count, size=size,
                      overlaps=0, number=number, array_element_size=None, template=None,
                      decimal_count=None, decimal_size=None)
    if field_type in ('STRING', 'CSTRING', 'PSTRING'):
        definition.update(array_element_size=size // count, template=0)
    if field_type == 'DECIMAL':
        definition.update(decimal_count=decimal_count, decimal_size=size)
    return definition


def memo(name, size=1000):
    return dict(external_filename='', memo_mark=1, name=name, size=size,
                flags=dict(memo_type='MEMO', BINARY=False, Flag=False))


def index(name, field_numbers, nocase=False, dup=False, opt=False, descending=()):
    return dict(external_filename='', index_mark=1, name=name,
                flags=dict(type='KEY', NOCASE=nocase, OPT=opt, DUP=dup),
                field_count=len(field_numbers),
                fields=[dict(field_number=n, order_type='DESCENDING' if n in descending else 'ASCENDING')
                        for n in field_numbers])


def table_definition(record_size, fields, memos=(), indexes=()):
    return dict(min_version_driver=1, record_size=record_size, field_count=len(fields),
                memo_count=len(memos), index_count=len(indexes),
                fields=list(fields), memos=list(memos), indexes=list(indexes))


def build_file(path, tables, last_issued_row=None):
    """
    Write a TopSpeed file.

    Args:
        tables: list of dicts with keys number, name, definition (dict as above),
                rows [(record number, payload bytes)], and optional memos
                {(record number, memo index): bytes}
    """
    records = []
    highest = 0
    for table in tables:
        number = table['number']
        name = table['name'].encode('ascii')
        records.append((1 + len(name), b'\xfe' + name + struct.pack('>I', number)))

        definition_bytes = TABLE_DEFINITION_STRUCT.build(table['definition'])
        key = struct.pack('>IB', number, TABLE_DEFINITION_TYPE) + struct.pack('<H', 0)
        records.append((len(key), key + definition_bytes))

        parsed = TABLE_DEFINITION_STRUCT.parse(definition_bytes)
        counts = {}
        for record_number, payload in table['rows']:
            highest = max(highest, record_number)
            records.append(data_record(number, record_number, payload))
            for header_size, record in index_records(number, parsed, payload, record_number):
                records.append((header_size, record))
                counts[record[4]] = counts.get(record[4], 0) + 1
        for (record_number, memo_index), data in table.get('memos', {}).items():
            chunks = [data[i:i + 256] for i in range(0, len(data), 256)] or [b'\x00']
            records.extend(memo_records(number, record_number, memo_index, chunks))
        records.append(metadata_record(number, DATA_TYPE, len(table['rows'])))
        for index_number, count in counts.items():
            records.append(metadata_record(number, index_number, count))

    with open(path, 'wb') as f:
        f.write(build_tps(records, last_issued_row=last_issued_row or highest))
    return path


def wells_table(number=1, rows=None):
    """A small table with a unique NOCASE string key, a DOUBLE array and a memo"""
    definition = table_definition(
        record_size=34,
        fields=[field('LONG', 0, 'WEL:ID', 4, number=0),
                field('STRING', 4, 'WEL:NAME', 12, number=1),
                field('DOUBLE', 16, 'WEL:RATE', 16, count=2, number=2),
                field('SHORT', 32, 'WEL:STATUS', 2, number=3)],
        memos=[memo('WEL:NOTES')],
        indexes=[index('WEL:BY_ID', [0]), index('WEL:BY_NAME', [1], nocase=True, dup=True)])
    if rows is None:
        rows = [(1, well_payload(1, 'Alpha 1', [10.5, 9.25], 1)),
                (2, well_payload(2, 'Bravo 2', [20.0, 18.0], 0)),
                (3, well_payload(3, 'Charlie 3', [0.0, 0.0], 2))]
    return dict(number=number, name='WELLS', definition=definition, rows=rows,
                memos={(1, 0): b'First well notes', (2, 0): b'x' * 600})


def well_payload(well_id, name, rates, status):
    return (struct.pack('<i', well_id) + name.encode('cp1251').ljust(12, b' ') +
            struct.pack('<2d', *rates) + struct.pack('<h', status))
