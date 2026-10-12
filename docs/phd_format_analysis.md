# TopSpeed / PHD File Format Reference

PHDWin `.phd` and `.mod` files are ordinary Clarion TopSpeed files, the same format as `.tps`. A `.phz` is
a zip archive containing a `.phd` and a `.mod`. This document describes the format as implemented by the
reader (`src/pytopspeed/`) and the writer (`src/pytopspeed/tpswriter.py`). The writer's output has been
checked by an unedited round trip of the TxWells sample (every record reproduced), by PHDWin 2.11, and by
the SoftVelocity TopSpeed ODBC driver.

All multi-byte integers are little-endian unless stated otherwise. Strings are cp1251.

## Sample Files

| File | Tables | Rows converted |
|------|--------|----------------|
| `assets/TxWells.PHD` | 55 | 9,492 |
| `assets/TxWells.mod` | 21 | 5,267 |

Raw record counts (all record types, as reproduced by an unedited round trip): 21,986 in TxWells.PHD and
16,770 in TxWells.mod. Field names carry a Clarion prefix (for example `TIT:PROJ_DESCR`); the converter drops
the prefix in column names.

## File Header (0x200 bytes)

| Field | Size | Notes |
|-------|------|-------|
| offset | 4 | always 0 |
| header size | 2 | 0x200 |
| file size | 4 | |
| allocated file size | 4 | |
| signature | 6 | `tOpS\0\0` |
| last issued row | 4 | **big-endian**; highest record number handed out |
| change count | 4 | |
| page root ref | 4 | page ref of the B-tree root |
| block start | 60 × 4 | page refs |
| block end | 60 × 4 | page refs |

### Block Table = Size Classes

The 60 block entries are not arbitrary extents: entry N (1..59) covers the contiguous run of pages that each
occupy exactly N 256-byte units. Entry 0 is always (0, 0); an empty class is (pos, pos). The writer therefore
lays out pages grouped by size in units. The TopSpeed ODBC driver crashes on files that mix page sizes
within one block.

## Pages

A page ref maps to file offset `ref * 0x100 + 0x200`. Each page starts with a 13-byte header:

| Field | Size | Notes |
|-------|------|-------|
| offset | 4 | file offset of the page |
| size | 2 | stored size, including the header |
| uncompressed size | 2 | size after RLE expansion, including the header |
| uncompressed unabridged size | 2 | `13 + child refs + sum(len(record) + 5)`: the size with no prefix sharing |
| record count | 2 | records (leaf) or children (control page) |
| hierarchy level | 1 | 0 = leaf |

The page body is RLE-compressed only when that makes it smaller.

### B-tree

All records of all tables live in one B-tree ordered by key bytes, where a record's key is its header
portion (see record layouts below).

- **Leaf pages** (level 0) hold runs of records.
- **Control pages** hold `record_count` child page refs (uint32 each, stored uncompressed) followed by the
  first key of each child, encoded like leaf records; only the key part is RLE-compressed.

Page limits the writer uses (TopSpeed's own maxima seen in TxWells in parentheses): leaf unabridged size
≤ 8,192 (12,723), control unabridged size ≤ 4,096 (3,962), ≤ 180 records per page (~210).

### Record Encoding in a Page (prefix compression)

Each record starts with a flag byte:

- `0x80`: a 2-byte record size follows
- `0x40`: a 2-byte header (key) size follows
- low 6 bits: number of leading bytes shared with the previous record (max 63)

Sizes carry over from the previous record when not stated; the first record on a page always states both.
The shared prefix is followed by the remaining bytes of the record.

### RLE

The body is a sequence of `[count][literal bytes][repeat count]` groups: `count` literal bytes are copied,
then the last byte is repeated `repeat count` more times. A count above 127 takes two bytes:
`(b1 & 0x7F) | (b2 << 7)`.

TopSpeed only uses a repeat for runs of 4 or more identical bytes (the literal keeps the run's first byte)
and closes a stream that ends with a repeat with a 0 literal count. Following these rules, the writer
reproduces every TopSpeed-compressed page of the TxWells files byte for byte.

## Record Key Layouts

Table numbers and record numbers in keys are **big-endian** so that keys sort correctly.

| Record type | Key | Payload |
|-------------|-----|---------|
| DATA | table(4) `F3` recno(4) | record bytes (`record_size` from the definition) |
| METADATA | table(4) `F6` type(1) | count(4 LE) + last access(4). Type `F3` = row count; type n = entry count of index n |
| TABLE_DEFINITION | table(4) `FA` portion(2 LE) | one portion of the definition; join portions in portion order (not page order) |
| (character table) | table(4) `FB` | 1,153-byte per-table character table |
| MEMO | table(4) `FC` recno(4) memo#(1) seq(2 BE) | one chunk of ≤ 256 bytes |
| TABLE_NAME | `FE` name | table number (4, big-endian) |
| (file-level) | `FF` | PHDWin install path |
| INDEX | table(4) index#(1) encoded key fields | see below |

Every file also starts with an empty record (zero-length key).

### Memos

A memo is split into chunks of at most 256 bytes, stored as separate MEMO records with increasing sequence
numbers. An empty memo is stored as one `0x00` byte.

### Index Records

The key is the table number, the index number, then each key field encoded so that a plain byte
comparison sorts like the field's values. For indexes that allow duplicates (`DUP`) the record number
(4 bytes, big-endian) is part of the key; for unique indexes it follows the key as the record's data.
`OPT` indexes skip records whose key fields are all blank (spaces/zeros).

Key field encoding:

| Field type | Encoding |
|------------|----------|
| BYTE | raw byte |
| SHORT, LONG | big-endian with the sign bit flipped |
| DOUBLE | big-endian; negative values have all bits inverted, others have the sign bit flipped |
| STRING | raw bytes; `NOCASE` keys are lowercased (cp1251) |
| any, `DESCENDING` | all bytes of that field inverted |

These encodings are verified against TopSpeed-written files for BYTE, SHORT, LONG, DOUBLE and STRING. The
writer also encodes USHORT/ULONG/DATE/TIME (byte-reversed), FLOAT (as DOUBLE) and DECIMAL (raw), but these
are unverified, and the reverse converter warns when an edited table has an index on such a field.

## Table Definitions

A definition (after joining its portions) is:

| Field | Size |
|-------|------|
| min driver version | 2 |
| record size | 2 |
| field count | 2 |
| memo count | 2 |
| index count | 2 |
| fields | per field: type(1), offset(2), name (C string), array element count(2), size(2), overlaps(2), number(2), then for STRING/CSTRING/PSTRING/PICTURE element size(2) + template(2), for DECIMAL decimal count(1) + decimal size(1) |
| memos | per memo: external filename (C string), `0x01` mark if no external file, name, size(2), flags(2) (MEMO/BLOB, BINARY) |
| indexes | per index: external filename, `0x01` mark if none, name, flags(1) (type KEY/INDEX/DYNAMIC, NOCASE, OPT, DUP), field count(2), then field number(2) + order(2: 0 ascending, 1 descending) per field |

Field type codes: BYTE 0x01, SHORT 0x02, USHORT 0x03, DATE 0x04, TIME 0x05, LONG 0x06, ULONG 0x07,
FLOAT 0x08, DOUBLE 0x09, DECIMAL 0x0A, STRING 0x12, CSTRING 0x13, PSTRING 0x14, GROUP 0x16.

### Field Values in Data Records

- DATE: `0xYYYYMMDD` as a little-endian uint32 (zero = no date)
- TIME: `0xHHMMSSHS` (hundredths)
- DECIMAL: packed BCD; a leading `0xF` nibble marks a negative value
- STRING: space-padded; CSTRING null-terminated; PSTRING length-prefixed
- Arrays (`DIM(n)`): `array_element_count` elements of `size / count` bytes each, stored contiguously

## SQLite Mapping (summary)

| TopSpeed | SQLite |
|----------|--------|
| BYTE, SHORT, USHORT, LONG, ULONG | INTEGER (BYTE values stay integers, not booleans) |
| FLOAT, DOUBLE, DECIMAL | REAL (NaN/inf → NULL) |
| STRING, CSTRING, PSTRING | TEXT, right-trimmed |
| DATE | TEXT `YYYY-MM-DD`, NULL if zero |
| TIME | TEXT `HH:MM:SS.cc` |
| arrays, dimensioned GROUP members, `NAME1..NAMEn` runs | TEXT holding a JSON array |
| MEMO | TEXT (full text) |

Hidden tables `_topspeed_file`, `_topspeed_table`, `_topspeed_record`, `_topspeed_row` and `_topspeed_memo`
keep the original records needed for reverse conversion. See [DEVELOPER.md](DEVELOPER.md) for the column
layout rules.
