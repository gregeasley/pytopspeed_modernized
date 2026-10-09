"""
TPS File Writer

A TopSpeed file keeps every record of every table (data rows, index entries, table
names and definitions, metadata, memos) in a single B-tree ordered by the record's key.
A record's key is its header portion: table number, record type, then type-specific
fields such as the record number or the encoded index key.

This module turns a set of raw records into a complete file:

- leaf pages hold runs of records, prefix-compressed against the previous record
- control pages hold child page refs followed by the first key of each child
- page bodies are RLE-compressed when that makes them smaller
- the file header points at the root page and describes the allocated blocks
"""

import struct
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

HEADER_SIZE = 0x200
PAGE_UNIT = 0x100
PAGE_HEADER_SIZE = 13
BLOCK_COUNT = 60

# Largest uncompressed page we build. Pages written by TopSpeed reach ~12.6KB; staying
# below that keeps every page within sizes the driver is known to produce.
MAX_PAGE_SIZE = 0x2000

# Longest prefix a record can share with the previous one (6-bit field)
MAX_SHARED_PREFIX = 0x3F

# Largest count that fits the RLE encoding (15 bits)
MAX_RLE_COUNT = 0x7FFF

# A run must be at least this long before it is worth a repeat code
MIN_RLE_RUN = 3

TOP_SPEED_MARK = b'tOpS\x00\x00'

Record = Tuple[int, bytes]  # (header/key size, full record bytes)


def rle_decompress(data: bytes) -> bytes:
    """Expand TopSpeed page RLE: [literal count][literal bytes][repeat count of last byte]..."""
    pos = 0
    out = bytearray()
    while pos < len(data):
        count, pos = _read_count(data, pos)
        out += data[pos:pos + count]
        pos += count
        if pos < len(data):
            repeat, pos = _read_count(data, pos)
            out += out[-1:] * repeat
    return bytes(out)


def rle_compress(data: bytes) -> bytes:
    """Compress with the TopSpeed page RLE scheme (inverse of rle_decompress)"""
    out = bytearray()
    pos = 0
    length = len(data)
    while pos < length:
        # Literal bytes run up to and including the first byte of the next worthwhile run
        literal_end = pos
        run = 0
        while literal_end < length:
            run = _run_length(data, literal_end)
            if run >= MIN_RLE_RUN or literal_end - pos + 1 >= MAX_RLE_COUNT:
                break
            literal_end += 1
        literal_end = min(literal_end + 1, length)
        if literal_end - pos > MAX_RLE_COUNT:
            literal_end = pos + MAX_RLE_COUNT
        out += _write_count(literal_end - pos)
        out += data[pos:literal_end]
        pos = literal_end
        if pos >= length:
            break
        # Repeat the last literal byte while the run continues
        repeat = 0
        last = data[pos - 1]
        while pos + repeat < length and data[pos + repeat] == last and repeat < MAX_RLE_COUNT:
            repeat += 1
        out += _write_count(repeat)
        pos += repeat
    return bytes(out)


def _run_length(data: bytes, pos: int) -> int:
    end = pos
    while end < len(data) and data[end] == data[pos] and end - pos < MAX_RLE_COUNT:
        end += 1
    return end - pos


def _read_count(data: bytes, pos: int) -> Tuple[int, int]:
    count = data[pos]
    pos += 1
    if count > 0x7F:
        count = (count & 0x7F) | (data[pos] << 7)
        pos += 1
    return count, pos


def _write_count(count: int) -> bytes:
    if count < 0x80:
        return bytes([count])
    return bytes([0x80 | (count & 0x7F), count >> 7])


def encode_records(records: Sequence[Record]) -> bytes:
    """
    Encode a run of records the way a page stores them.

    Each record starts with a flag byte: bit 7 means a 2-byte record size follows,
    bit 6 means a 2-byte header size follows, and the low 6 bits give how many leading
    bytes are shared with the previous record. Sizes carry over when unchanged, except
    that the first record on a page always states both.
    """
    out = bytearray()
    previous = b''
    previous_size = previous_header_size = None
    for header_size, record in records:
        shared = 0
        limit = min(len(previous), len(record), MAX_SHARED_PREFIX)
        while shared < limit and previous[shared] == record[shared]:
            shared += 1
        flags = shared
        sizes = b''
        if len(record) != previous_size:
            flags |= 0x80
            sizes += struct.pack('<H', len(record))
        if header_size != previous_header_size:
            flags |= 0x40
            sizes += struct.pack('<H', header_size)
        out += bytes([flags]) + sizes + record[shared:]
        previous, previous_size, previous_header_size = record, len(record), header_size
    return bytes(out)


class _Page:
    def __init__(self, level: int, records: List[Record], children: Optional[List['_Page']] = None):
        self.level = level
        self.records = records  # leaf: records; control: (key size, key) per child
        self.children = children or []
        self.ref = 0  # assigned when pages are laid out

    @property
    def first_key(self) -> Record:
        header_size, record = self.records[0]
        return header_size, record[:header_size]

    def body(self) -> Tuple[bytes, bytes]:
        """Return (uncompressed prefix kept as-is, compressible part)"""
        # Control pages keep the child ref array uncompressed ahead of the keys
        refs = b''.join(struct.pack('<I', child.ref) for child in self.children)
        return refs, encode_records(self.records)

    def serialize(self, offset: int) -> bytes:
        refs, records = self.body()
        compressed = rle_compress(records)
        stored = compressed if len(compressed) < len(records) else records
        uncompressed_size = PAGE_HEADER_SIZE + len(refs) + len(records)
        size = PAGE_HEADER_SIZE + len(refs) + len(stored)
        # Size the page would have without prefix compression (full 5-byte flags per record)
        unabridged_size = PAGE_HEADER_SIZE + len(refs) + sum(len(r) + 5 for _, r in self.records)
        header = struct.pack('<IHHHHB', offset, size, uncompressed_size,
                             unabridged_size & 0xFFFF, len(self.records), self.level)
        return header + refs + stored


def _paginate(items: List[Record], level: int, child_pages: Optional[List[_Page]] = None) -> List[_Page]:
    """Split items into pages no larger than MAX_PAGE_SIZE"""
    ref_size = 4 if child_pages else 0
    pages = []
    start = 0
    while start < len(items):
        # A record's encoding depends only on the record before it, so sizes add up
        size = PAGE_HEADER_SIZE + ref_size + len(encode_records(items[start:start + 1]))
        end = start + 1
        while end < len(items):
            pair = encode_records(items[end - 1:end + 1])
            step = ref_size + len(pair) - len(encode_records(items[end - 1:end]))
            if size + step > MAX_PAGE_SIZE:
                break
            size += step
            end += 1
        pages.append(_Page(level, items[start:end], child_pages[start:end] if child_pages else None))
        start = end
    return pages


def build_tps(records: Iterable[Record], last_issued_row: int = 0, change_count: int = 1) -> bytes:
    """
    Build a complete TopSpeed file from raw records.

    Args:
        records: (header size, record bytes) pairs; the key is record[:header size].
                 Order does not matter. The empty record that starts every file is
                 added if missing.
        last_issued_row: highest record number handed out so far
        change_count: file change counter stored in the header

    Returns:
        The file contents
    """
    by_key: Dict[bytes, Record] = {}
    for header_size, record in records:
        key = record[:header_size]
        if key in by_key:
            raise ValueError(f'Duplicate record key {key.hex()}')
        by_key[key] = (header_size, bytes(record))
    if b'' not in by_key:
        by_key[b''] = (0, b'')
    ordered = [by_key[key] for key in sorted(by_key)]

    # Leaves first, then each control level, so every child ref is known before its parent
    leaves = _paginate(ordered, 0)
    levels = [leaves]
    while len(levels[-1]) > 1:
        below = levels[-1]
        keys = [page.first_key for page in below]
        levels.append(_paginate(keys, len(levels), below))

    # Lay pages out on disk in level order; children always get their refs before their parent
    blobs = []
    ref = 0
    for level_pages in levels:
        for page in level_pages:
            page.ref = ref
            blob = page.serialize(HEADER_SIZE + ref * PAGE_UNIT)
            padded = len(blob) + (-len(blob)) % PAGE_UNIT
            blobs.append(blob.ljust(padded, b'\x00'))
            ref += padded // PAGE_UNIT
    root = levels[-1][0]
    total_refs = ref

    body = b''.join(blobs)
    file_size = HEADER_SIZE + len(body)
    block_start = [0] + [total_refs] * (BLOCK_COUNT - 1)
    block_end = [total_refs] * BLOCK_COUNT
    header = struct.pack('<IHII', 0, HEADER_SIZE, file_size, file_size) + TOP_SPEED_MARK
    header += struct.pack('>I', last_issued_row)
    header += struct.pack('<II', change_count, root.ref)
    header += struct.pack(f'<{BLOCK_COUNT}I', *block_start)
    header += struct.pack(f'<{BLOCK_COUNT}I', *block_end)
    assert len(header) == HEADER_SIZE
    return header + body


#: Record type bytes (the byte after the table number)
DATA_TYPE = 0xF3
METADATA_TYPE = 0xF6
TABLE_DEFINITION_TYPE = 0xFA
MEMO_TYPE = 0xFC
TABLE_NAME_MARK = 0xFE

#: Field types whose index key encoding has been checked against files written by TopSpeed
VERIFIED_KEY_TYPES = {'BYTE', 'SHORT', 'LONG', 'DOUBLE', 'STRING'}


def data_record(table_number: int, record_number: int, payload: bytes) -> Record:
    key = struct.pack('>IBI', table_number, DATA_TYPE, record_number)
    return len(key), key + payload


def memo_records(table_number: int, record_number: int, memo_index: int, chunks: Sequence[bytes]) -> List[Record]:
    records = []
    for sequence, chunk in enumerate(chunks):
        key = struct.pack('>IBIBH', table_number, MEMO_TYPE, record_number, memo_index, sequence)
        records.append((len(key), key + chunk))
    return records


def metadata_record(table_number: int, metadata_type: int, count: int, last_access: int = 0) -> Record:
    """Record count for the table's data (metadata_type 0xF3) or for one index (its number)"""
    key = struct.pack('>IBB', table_number, METADATA_TYPE, metadata_type)
    return len(key), key + struct.pack('<II', count, last_access)


def index_records(table_number: int, definition, payload: bytes, record_number: int,
                  encoding: str = 'cp1251') -> List[Record]:
    """
    Index entries for one data record, one per index in the definition.

    The key is the table number, the index number, then each key field encoded so that
    a plain byte comparison sorts like the field's values. Indexes that allow duplicates
    include the record number in the key; unique indexes store it after the key.
    """
    records = []
    for index_number, index in enumerate(definition.indexes):
        if index.external_filename:
            continue
        parts = []
        all_blank = True
        for index_field in index.fields:
            field = definition.fields[index_field.field_number]
            raw = payload[field.offset:field.offset + field.size]
            all_blank = all_blank and _is_blank(str(field.type), raw)
            parts.append(index_key_bytes(str(field.type), raw, str(index_field.order_type) == 'DESCENDING',
                                         index.flags.NOCASE, encoding))
        if index.flags.OPT and all_blank:
            # OPT keys leave out records whose key fields are all blank or zero
            continue
        key = struct.pack('>IB', table_number, index_number) + b''.join(parts)
        record = key + struct.pack('>I', record_number)
        records.append((len(record) if index.flags.DUP else len(key), record))
    return records


def index_key_bytes(field_type: str, raw: bytes, descending: bool = False, nocase: bool = False,
                    encoding: str = 'cp1251') -> bytes:
    """Encode one key field so that byte order matches value order"""
    if field_type in ('BYTE',):
        key = raw[:1]
    elif field_type == 'SHORT':
        key = struct.pack('>H', struct.unpack('<H', raw[:2])[0] ^ 0x8000)
    elif field_type == 'LONG':
        key = struct.pack('>I', struct.unpack('<I', raw[:4])[0] ^ 0x80000000)
    elif field_type in ('USHORT', 'ULONG', 'DATE', 'TIME'):
        key = raw[::-1]
    elif field_type in ('DOUBLE', 'FLOAT'):
        key = bytearray(raw[::-1])
        if key[0] & 0x80:
            key = bytearray(b ^ 0xFF for b in key)
        else:
            key[0] ^= 0x80
        key = bytes(key)
    elif field_type in ('STRING', 'CSTRING', 'PSTRING'):
        key = raw.decode(encoding, errors='replace').lower().encode(encoding, errors='replace') if nocase else raw
        if len(key) != len(raw):
            key = raw
    else:
        # DECIMAL BCD and anything else: raw bytes
        key = raw
    return bytes(b ^ 0xFF for b in key) if descending else bytes(key)


def _is_blank(field_type: str, raw: bytes) -> bool:
    if field_type in ('STRING', 'CSTRING', 'PSTRING'):
        return raw.strip(b' \x00') == b''
    return not any(raw)


def read_raw_records(tps) -> List[Record]:
    """Return every record in an open TPS file as (header size, record bytes)"""
    from .tpsrecord import TpsRecordsList

    records = []
    for page_ref in tps.pages.list():
        page = tps.pages[page_ref]
        if page.hierarchy_level == 0:
            for record in TpsRecordsList(tps, page, encoding=tps.encoding):
                records.append((record.header_size, bytes(record.data_bytes[2:])))
    return records
