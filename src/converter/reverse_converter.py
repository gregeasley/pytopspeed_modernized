#!/usr/bin/env python3
"""
Reverse Converter - Convert SQLite databases back to TopSpeed files

Rebuilds the original TopSpeed file(s) from a database created by SqliteConverter or
PhzConverter. The forward conversion stores what SQLite can't represent (table
definitions, table numbers, file-level records and the original bytes of every row)
in ``_topspeed_*`` tables; this converter combines that with the current table
contents:

- rows nobody changed are written back byte for byte
- edited rows keep their record number; only the changed columns are re-encoded
- new rows get new record numbers; deleted rows (and their memos) are dropped
- index entries and record counts are regenerated from the resulting rows
"""

import logging
import os
import sqlite3
import struct
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Tuple

# Add the src directory to the path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pytopspeed.tpstable import TABLE_DEFINITION_STRUCT
from pytopspeed.tpswriter import (DATA_TYPE, METADATA_TYPE, TABLE_DEFINITION_TYPE, VERIFIED_KEY_TYPES,
                                  build_tps, data_record, index_records, memo_records, metadata_record)
from converter.record_codec import RecordCodec, memo_chunks
from converter.topspeed_metadata import table_number_of


class ReverseConversionError(Exception):
    """The database can't be turned back into a valid TopSpeed file"""


class ReverseConverter:
    """
    Converter for creating TopSpeed files from SQLite databases
    """

    def __init__(self, progress_callback=None):
        """
        Initialize reverse converter

        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback
        self.logger = logging.getLogger(__name__)

    def convert_sqlite_to_topspeed(self, sqlite_file: str, output_dir: str) -> Dict[str, Any]:
        """
        Convert SQLite database back to TopSpeed files

        Args:
            sqlite_file: Path to a SQLite file created by SqliteConverter or PhzConverter
            output_dir: Directory to write output files (named after the original files)

        Returns:
            Dictionary with conversion results
        """
        start_time = datetime.now()
        results: Dict[str, Any] = {
            'success': False,
            'files_created': [],
            'tables_processed': 0,
            'records_processed': 0,
            'duration': 0,
            'errors': [],
            'warnings': [],
        }

        try:
            if not os.path.exists(sqlite_file):
                raise ReverseConversionError(f"SQLite file not found: {sqlite_file}")

            conn = sqlite3.connect(sqlite_file)
            try:
                files = self._source_files(conn)
                os.makedirs(output_dir, exist_ok=True)
                for file_prefix, source_file, last_issued_row, change_count, encoding in files:
                    output_file = os.path.join(output_dir, source_file)
                    self.logger.info(f"Rebuilding {source_file}")
                    try:
                        blob, stats = self._rebuild_file(conn, file_prefix, last_issued_row, change_count, encoding)
                    except ReverseConversionError as e:
                        results['errors'].append(f"{source_file}: {e}")
                        continue
                    with open(output_file, 'wb') as f:
                        f.write(blob)
                    results['files_created'].append(output_file)
                    results['tables_processed'] += stats['tables']
                    results['records_processed'] += stats['rows']
                    results['warnings'].extend(f"{source_file}: {w}" for w in stats['warnings'])
            finally:
                conn.close()

            results['success'] = bool(results['files_created']) and not results['errors']
            for warning in results['warnings']:
                self.logger.warning(warning)

        except ReverseConversionError as e:
            results['errors'].append(str(e))
        except Exception as e:
            self.logger.error(f"Reverse conversion failed: {e}")
            results['errors'].append(str(e))

        finally:
            results['duration'] = (datetime.now() - start_time).total_seconds()

        return results

    def _source_files(self, conn) -> List[Tuple]:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_topspeed_file'").fetchone()
        if not exists:
            raise ReverseConversionError(
                "This database has no TopSpeed metadata (_topspeed_* tables). Convert the original "
                "TopSpeed file again with this version, make your changes in that database, then reverse it.")
        return conn.execute('SELECT file_prefix, source_file, last_issued_row, change_count, encoding '
                            'FROM _topspeed_file ORDER BY file_prefix').fetchall()

    def _rebuild_file(self, conn, file_prefix: str, last_issued_row: int, change_count: int,
                      encoding: str) -> Tuple[bytes, Dict[str, Any]]:
        stats: Dict[str, Any] = {'tables': 0, 'rows': 0, 'warnings': []}
        verbatim = conn.execute('SELECT header_size, record FROM _topspeed_record WHERE file_prefix = ?',
                                (file_prefix,)).fetchall()
        tables = conn.execute('SELECT table_number, table_name, sqlite_table, uses_codec FROM _topspeed_table '
                              'WHERE file_prefix = ? ORDER BY table_number', (file_prefix,)).fetchall()
        definitions = self._definitions(verbatim)
        codec_numbers = {number for number, _, _, uses_codec in tables if uses_codec}

        # Metadata of codec tables is regenerated below; keep which types existed and their access stamps
        metadata = defaultdict(dict)
        records = []
        for header_size, record in verbatim:
            number = table_number_of(record)
            if number in codec_numbers and record[4] == METADATA_TYPE:
                metadata[number][record[5]] = struct.unpack('<I', record[10:14])[0]
                continue
            records.append((header_size, bytes(record)))

        next_record_number = last_issued_row
        for i, (table_number, table_name, sqlite_table, uses_codec) in enumerate(tables):
            if self.progress_callback:
                self.progress_callback(i, len(tables), f"Rebuilding table: {table_name}")
            if not uses_codec:
                self._check_untracked_table(conn, sqlite_table, table_name, table_number, verbatim, stats)
                continue
            definition = definitions.get(table_number)
            if definition is None:
                raise ReverseConversionError(f"table {table_name}: definition record missing")
            table_records, row_count, next_record_number = self._table_records(
                conn, file_prefix, table_number, table_name, sqlite_table, definition, encoding,
                next_record_number, metadata[table_number], stats)
            records.extend(table_records)
            stats['tables'] += 1
            stats['rows'] += row_count

        try:
            blob = build_tps(records, last_issued_row=next_record_number, change_count=change_count + 1)
        except ValueError as e:
            raise ReverseConversionError(str(e)) from e
        return blob, stats

    @staticmethod
    def _definitions(verbatim) -> Dict[int, Any]:
        portions = defaultdict(dict)
        for header_size, record in verbatim:
            number = table_number_of(record)
            if number is not None and record[4] == TABLE_DEFINITION_TYPE:
                portions[number][struct.unpack('<H', record[5:7])[0]] = bytes(record[7:])
        return {number: TABLE_DEFINITION_STRUCT.parse(b''.join(parts[k] for k in sorted(parts)))
                for number, parts in portions.items()}

    def _table_records(self, conn, file_prefix, table_number, table_name, sqlite_table, definition, encoding,
                       next_record_number, metadata, stats):
        codec = RecordCodec(definition, encoding=encoding)
        names = [c.name for c in codec.columns]

        current = []
        if sqlite_table and self._table_exists(conn, sqlite_table):
            present = {row[1].upper() for row in conn.execute(f'PRAGMA table_info("{sqlite_table}")')}
            missing = [n for n in names if n.upper() not in present]
            if missing:
                raise ReverseConversionError(f"table {sqlite_table} is missing columns {missing}")
            column_list = ", ".join(f'"{n}"' for n in names)
            current = conn.execute(f'SELECT rowid, {column_list} FROM "{sqlite_table}" ORDER BY rowid').fetchall()
        else:
            stats['warnings'].append(f"table {sqlite_table or table_name} not found; writing it with no rows")

        originals = {row_id: (record_number, bytes(payload)) for row_id, record_number, payload in conn.execute(
            'SELECT row_id, record_number, payload FROM _topspeed_row WHERE file_prefix = ? AND table_number = ?',
            (file_prefix, table_number))}
        memos = defaultdict(dict)
        for record_number, memo_index, data in conn.execute(
                'SELECT record_number, memo_index, data FROM _topspeed_memo WHERE file_prefix = ? AND table_number = ?',
                (file_prefix, table_number)):
            memos[record_number][memo_index] = bytes(data)

        plan, next_record_number, changed = self._match_rows(codec, current, originals, memos, next_record_number)
        if changed:
            self._warn_unverified_keys(definition, table_name, stats)

        records = []
        index_counts = defaultdict(int)
        unique_keys = {}  # unique index key -> record number holding it
        for values, record_number, template, changed_columns, original_memos in plan:
            try:
                if template is None or changed_columns:
                    payload = codec.encode(values, template=template,
                                           columns=None if template is None else changed_columns)
                else:
                    payload = template
            except ValueError as e:
                raise ReverseConversionError(f"table {sqlite_table}, record {record_number}: {e}") from e
            records.append(data_record(table_number, record_number, payload))
            for header_size, index_record in index_records(table_number, definition, payload, record_number, encoding):
                index_number = index_record[4]
                index = definition.indexes[index_number]
                if not index.flags.DUP:
                    key = index_record[:header_size]
                    if key in unique_keys:
                        raise ReverseConversionError(
                            f"table {sqlite_table}: two rows (record numbers {unique_keys[key]} and {record_number}) "
                            f"have the same value for unique key {index.name} "
                            f"({', '.join(codec.index_columns(index) or [])})")
                    unique_keys[key] = record_number
                records.append((header_size, index_record))
                index_counts[index_number] += 1
            for column, value in zip(codec.columns, values):
                if not column.is_memo:
                    continue
                if template is not None and column.name not in changed_columns:
                    data = original_memos.get(column.memo_index)
                else:
                    data = codec.encode_memo(value)
                if data is not None:
                    records.extend(memo_records(table_number, record_number, column.memo_index, memo_chunks(data)))

        records.append(metadata_record(table_number, DATA_TYPE, len(plan), metadata.get(DATA_TYPE, 0)))
        for index_number in range(len(definition.indexes)):
            if index_number in metadata or index_counts[index_number]:
                records.append(metadata_record(table_number, index_number, index_counts[index_number],
                                               metadata.get(index_number, 0)))
        return records, len(plan), next_record_number

    @staticmethod
    def _match_rows(codec, current, originals, memos, next_record_number):
        """
        Pair each SQLite row with the TopSpeed record it came from.

        Returns (plan, next record number, whether anything changed), where plan holds
        (values, record number, original payload or None, changed column names, original memos).
        Rows are paired by rowid; rows whose rowid link no longer holds (e.g. after VACUUM
        renumbered them) are paired by content; anything left is new.
        """
        names = [c.name for c in codec.columns]
        original_values = {}
        for row_id, (record_number, payload) in originals.items():
            original_values[row_id] = _normalize(codec.decode(payload, memos.get(record_number)))

        used = set()
        plan_by_position: Dict[int, Tuple[Any, tuple, Any]] = {}
        pending = []
        for position, row in enumerate(current):
            row_id, values = row[0], _normalize(row[1:])
            if row_id in original_values and row_id not in used and original_values[row_id] == values:
                used.add(row_id)
                plan_by_position[position] = (row_id, values, [])
            else:
                pending.append((position, row_id, values))

        by_content = defaultdict(list)
        for row_id, values in original_values.items():
            if row_id not in used:
                by_content[values].append(row_id)
        still_pending = []
        for position, row_id, values in pending:
            candidates = [r for r in by_content.get(values, []) if r not in used]
            if candidates:
                used.add(candidates[0])
                plan_by_position[position] = (candidates[0], values, [])
            else:
                still_pending.append((position, row_id, values))

        changed = bool(still_pending) or len(used) != len(originals)
        for position, row_id, values in still_pending:
            if row_id in original_values and row_id not in used:
                used.add(row_id)
                diff = [n for n, a, b in zip(names, values, original_values[row_id]) if a != b]
                plan_by_position[position] = (row_id, values, diff)
            else:
                plan_by_position[position] = (None, values, None)

        plan: List[Tuple[tuple, int, Any, Any, Dict[int, bytes]]] = []
        for position in range(len(current)):
            row_id, values, diff = plan_by_position[position]
            if row_id is None:
                next_record_number += 1
                plan.append((values, next_record_number, None, None, {}))
            else:
                record_number, payload = originals[row_id]
                plan.append((values, record_number, payload, diff, memos.get(record_number, {})))
        return plan, next_record_number, changed

    @staticmethod
    def _warn_unverified_keys(definition, table_name, stats):
        for index in definition.indexes:
            types = {str(definition.fields[f.field_number].type) for f in index.fields}
            unverified = sorted(types - VERIFIED_KEY_TYPES)
            if unverified:
                stats['warnings'].append(
                    f"table {table_name}, index {index.name}: key encoding for {', '.join(unverified)} fields "
                    f"has not been checked against TopSpeed; verify the file opens correctly")

    def _check_untracked_table(self, conn, sqlite_table, table_name, table_number, verbatim, stats):
        """Tables converted without a codec are written back as they were; flag edits that will be lost"""
        if not sqlite_table or not self._table_exists(conn, sqlite_table):
            return
        original_rows = sum(1 for _, record in verbatim
                            if table_number_of(record) == table_number and record[4] == DATA_TYPE)
        current_rows = conn.execute(f'SELECT COUNT(*) FROM "{sqlite_table}"').fetchone()[0]
        if current_rows != original_rows:
            stats['warnings'].append(
                f"table {sqlite_table} was converted without a field layout, so changes to it can't be written "
                f"back; the original {original_rows} records were kept")

    @staticmethod
    def _table_exists(conn, name: str) -> bool:
        return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (name,)).fetchone() is not None


def _normalize(values) -> tuple:
    """Make SQLite values and freshly decoded values comparable (and hashable)"""
    normalized = []
    for value in values:
        if isinstance(value, float) and value != value:
            value = None
        elif isinstance(value, memoryview):
            value = bytes(value)
        normalized.append(value)
    return tuple(normalized)
