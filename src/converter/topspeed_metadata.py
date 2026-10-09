"""
TopSpeed metadata kept inside a converted SQLite database

Rebuilding a TopSpeed file needs more than the table contents: the exact table
definitions and table numbers, the records SQLite has no place for (table names,
definitions, metadata, file-level records) and the original bytes of every row so
that values nobody edited are written back unchanged. Forward conversion stores
these in tables prefixed with ``_topspeed_``; reverse conversion reads them back.

Tables:
    _topspeed_file    one row per converted TopSpeed file
    _topspeed_table   table number/name and the SQLite table holding its rows
    _topspeed_record  records copied back verbatim (everything except the rows,
                      index entries and memos of tables converted with a codec)
    _topspeed_row     original record number and payload for each SQLite row
    _topspeed_memo    original memo bytes per record
"""

import os
import struct
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

from pytopspeed.tpswriter import DATA_TYPE, MEMO_TYPE, TABLE_NAME_MARK, read_raw_records

FORMAT_VERSION = 1
METADATA_PREFIX = '_topspeed_'

SCHEMA = [
    '''CREATE TABLE IF NOT EXISTS _topspeed_file (
        file_prefix TEXT PRIMARY KEY,
        source_file TEXT NOT NULL,
        last_issued_row INTEGER NOT NULL,
        change_count INTEGER NOT NULL,
        encoding TEXT NOT NULL,
        format_version INTEGER NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS _topspeed_table (
        file_prefix TEXT NOT NULL,
        table_number INTEGER NOT NULL,
        table_name TEXT NOT NULL,
        sqlite_table TEXT,
        uses_codec INTEGER NOT NULL,
        PRIMARY KEY (file_prefix, table_number))''',
    '''CREATE TABLE IF NOT EXISTS _topspeed_record (
        file_prefix TEXT NOT NULL,
        header_size INTEGER NOT NULL,
        record BLOB NOT NULL)''',
    '''CREATE TABLE IF NOT EXISTS _topspeed_row (
        file_prefix TEXT NOT NULL,
        table_number INTEGER NOT NULL,
        row_id INTEGER NOT NULL,
        record_number INTEGER NOT NULL,
        payload BLOB NOT NULL,
        PRIMARY KEY (file_prefix, table_number, row_id))''',
    '''CREATE TABLE IF NOT EXISTS _topspeed_memo (
        file_prefix TEXT NOT NULL,
        table_number INTEGER NOT NULL,
        record_number INTEGER NOT NULL,
        memo_index INTEGER NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (file_prefix, table_number, record_number, memo_index))''',
]


def is_metadata_table(name: str) -> bool:
    return name.lower().startswith(METADATA_PREFIX)


def ensure_schema(conn):
    for statement in SCHEMA:
        conn.execute(statement)


def table_number_of(record: bytes) -> Optional[int]:
    """Table number of a record, or None for table-name and file-level records"""
    if len(record) < 5 or record[0] == TABLE_NAME_MARK:
        return None
    return struct.unpack('>I', record[:4])[0]


class FileCapture:
    """
    Raw records of one TopSpeed file, indexed for forward conversion, plus the
    bookkeeping needed to store the reverse-conversion metadata.
    """

    def __init__(self, tps, file_prefix: str, source_file: str, encoding: str = 'cp1251'):
        self.file_prefix = file_prefix
        self.source_file = os.path.basename(source_file)
        self.encoding = encoding
        self.last_issued_row = tps.header.last_issued_row
        self.change_count = tps.header.change_count
        self.records = read_raw_records(tps)
        self.table_numbers = {str(t.name): n for n, t in tps.tables._TpsTablesList__tables.items() if t.name}
        self.codecs = {}            # table name -> RecordCodec
        self.sqlite_tables = {}     # table name -> SQLite table name

        self.rows: Dict[int, List[Tuple[int, bytes]]] = defaultdict(list)
        self.memos: Dict[Tuple[int, int], Dict[int, bytes]] = defaultdict(dict)
        memo_chunks = defaultdict(list)
        for header_size, record in self.records:
            table_number = table_number_of(record)
            if table_number is None:
                continue
            if record[4] == DATA_TYPE and header_size == 9:
                self.rows[table_number].append((struct.unpack('>I', record[5:9])[0], record[9:]))
            elif record[4] == MEMO_TYPE and header_size == 12:
                record_number = struct.unpack('>I', record[5:9])[0]
                sequence = struct.unpack('>H', record[10:12])[0]
                memo_chunks[(table_number, record_number, record[9])].append((sequence, record[12:]))
        for (table_number, record_number, memo_index), chunks in memo_chunks.items():
            self.memos[(table_number, record_number)][memo_index] = b''.join(c for _, c in sorted(chunks))
        for rows in self.rows.values():
            rows.sort()

    def register(self, table_name: str, sqlite_table: str, codec=None):
        self.sqlite_tables[table_name] = sqlite_table
        if codec is not None:
            self.codecs[table_name] = codec

    def store_rows(self, conn, table_name: str, linked_rows: Iterable[Tuple[int, int, bytes]]):
        """Record (SQLite rowid, record number, payload) for rows migrated with a codec"""
        table_number = self.table_numbers[table_name]
        conn.executemany(
            'INSERT INTO _topspeed_row (file_prefix, table_number, row_id, record_number, payload) VALUES (?, ?, ?, ?, ?)',
            [(self.file_prefix, table_number, row_id, record_number, payload)
             for row_id, record_number, payload in linked_rows])
        conn.executemany(
            'INSERT INTO _topspeed_memo (file_prefix, table_number, record_number, memo_index, data) VALUES (?, ?, ?, ?, ?)',
            [(self.file_prefix, table_number, record_number, memo_index, data)
             for _, record_number, _ in linked_rows
             for memo_index, data in self.memos.get((table_number, record_number), {}).items()])

    def finish(self, conn):
        """Store file, table and verbatim-record metadata once all tables are migrated"""
        ensure_schema(conn)
        conn.execute('DELETE FROM _topspeed_file WHERE file_prefix = ?', (self.file_prefix,))
        conn.execute('INSERT INTO _topspeed_file VALUES (?, ?, ?, ?, ?, ?)',
                     (self.file_prefix, self.source_file, self.last_issued_row, self.change_count,
                      self.encoding, FORMAT_VERSION))
        codec_numbers = {self.table_numbers[name] for name in self.codecs}
        conn.executemany('INSERT INTO _topspeed_table VALUES (?, ?, ?, ?, ?)',
                         [(self.file_prefix, number, name, self.sqlite_tables.get(name), int(name in self.codecs))
                          for name, number in self.table_numbers.items()])
        verbatim = []
        for header_size, record in self.records:
            table_number = table_number_of(record)
            # Rows, index entries and memos of codec tables are rebuilt from SQLite
            if table_number in codec_numbers and (record[4] in (DATA_TYPE, MEMO_TYPE) or record[4] < 0xF0):
                continue
            verbatim.append((self.file_prefix, header_size, record))
        conn.executemany('INSERT INTO _topspeed_record VALUES (?, ?, ?)', verbatim)
