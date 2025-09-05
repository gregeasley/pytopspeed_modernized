"""
Class to read TPS files

http://www.clarionlife.net/content/view/41/29/
http://www.softvelocity.com/clarion/pdf/languagereferencemanual.pdf
http://www.softvelocity.com/clarion/pdf/databasedrivers.pdf
"""

import os.path
import mmap
from datetime import date
import time
from typing import Any, Dict, List, Union
from warnings import warn
from binascii import hexlify

# Removed six dependency - using native Python 3 str type
from construct import (
    Array,
    Byte,
    Bytes,
    Const,
    Float32l,
    Float64l,
    Struct,
    Int16sl,
    Int32sl,
    Int32ub,
    Int8ul,
    Int16ul,
    Int32ul,
)

from .tpscrypt import TpsDecryptor
from .tpstable import TpsTablesList
from .tpspage import TpsPagesList
from .tpsrecord import TpsRecordsList
from .utils import check_value


# Date structure
DATE_STRUCT = Struct(
    "day" / Byte,
    "month" / Byte,
    "year" / Int16ul,
)

# Time structure
TIME_STRUCT = Struct(
    "centisecond" / Byte,
    "second" / Byte,
    "minute" / Byte,
    "hour" / Byte
)


class TPS:
    """
    TPS file
    """

    def __init__(
        self,
        filename,
        encoding=None,
        password=None,
        cached=True,
        check=False,
        current_tablename=None,
        date_fieldname=None,
        time_fieldname=None,
        decryptor_class=TpsDecryptor,
    ):
        self.filename = filename
        self.encoding = encoding
        self.password = password
        self.cached = cached
        self.check = check
        self.current_table_number = None
        # Name part before .tps/.phd
        self.name = os.path.basename(filename)
        self.name = str(os.path.splitext(self.name)[0]).lower()
        if date_fieldname is not None:
            self.date_fieldname = date_fieldname
        else:
            self.date_fieldname = []
        if time_fieldname is not None:
            self.time_fieldname = time_fieldname
        else:
            self.time_fieldname = []
        self.cache_pages = {}

        if not os.path.isfile(self.filename):
            raise FileNotFoundError(self.filename)

        self.file_size = os.path.getsize(self.filename)

        # Check file size
        if check:
            if self.file_size & 0x3F != 0:
                # TODO check translate
                warn("File size is not a multiple of 64 bytes.", RuntimeWarning)

        with open(self.filename, mode="r+b") as tpsfile:
            self.tps_file = mmap.mmap(tpsfile.fileno(), 0)

            self.decryptor = decryptor_class(self.tps_file, self.password)

            try:
                # TPS file header
                header = Struct(
                    "offset" / Int32ul,
                    "size" / Int16ul,
                    "file_size" / Int32ul,
                    "allocated_file_size" / Int32ul,
                    "top_speed_mark" / Const(b"tOpS\x00\x00"),
                    "last_issued_row" / Int32ub,
                    "change_count" / Int32ul,
                    "page_root_ref" / Int32ul,
                    "block_start_ref" / Array(lambda ctx: (ctx["size"] - 0x20) // 2 // 4, Int32ul),
                    "block_end_ref" / Array(lambda ctx: (ctx["size"] - 0x20) // 2 // 4, Int32ul),
                )

                self.header = header.parse(self.read(0x200))
                self.pages = TpsPagesList(
                    self, self.header.page_root_ref, check=self.check
                )
                self.tables = TpsTablesList(
                    self, encoding=self.encoding, check=self.check
                )
                self.set_current_table(current_tablename)
            except Exception as e:
                if "ConstError" in str(type(e)):
                    print("Bad cryptographic keys.")
                else:
                    raise

    def block_contains(self, start_ref, end_ref):
        for i in range(len(self.header.block_start_ref)):
            if (
                self.header.block_start_ref[i] <= start_ref
                and end_ref <= self.header.block_end_ref[i]
            ):
                return True
        return False

    def read(self, size, pos=None):
        if pos is not None:
            self.seek(pos)
        else:
            pos = self.tps_file.tell()
        if self.decryptor.is_encrypted():
            return self.decryptor.decrypt(size, pos)
        else:
            return self.tps_file.read(size)

    def seek(self, pos):
        self.tps_file.seek(pos)

    def __iter__(self):
        table_definition = self.tables.get_definition(self.current_table_number)
        for page_ref in self.pages.list():
            if self.pages[page_ref].hierarchy_level == 0:
                for record in TpsRecordsList(
                    self, self.pages[page_ref], encoding=self.encoding, check=self.check
                ):
                    if (
                        record.type == "DATA"
                        and record.data.table_number == self.current_table_number
                    ):
                        check_value(
                            "table_record_size",
                            len(record.data.data.data),
                            table_definition.record_size,
                        )
                        # TODO convert name to string
                        fields = {"b':RecNo'": record.data.data.record_number}
                        for field in table_definition.fields:
                            field_data = record.data.data.data[
                                field.offset : field.offset + field.size
                            ]
                            value = ""
                            if field.type == "BYTE":
                                value = Int8ul.parse(field_data)
                            elif field.type == "SHORT":
                                value = Int16sl.parse(field_data)
                            elif field.type == "USHORT":
                                value = Int16ul.parse(field_data)
                            elif field.type == "DATE":
                                value = self.to_date(field_data)
                            elif field.type == "TIME":
                                value = self.to_time(field_data)
                            elif field.type == "LONG":
                                # TODO
                                if (
                                    field.name.split(":")[1]
                                    .lower()
                                    in self.date_fieldname
                                ):
                                    if Int32sl.parse(field_data) == 0:
                                        value = None
                                    else:
                                        value = date.fromordinal(
                                            657433 + Int32sl.parse(field_data)
                                        )
                                elif (
                                    field.name.split(":")[1]
                                    .lower()
                                    in self.time_fieldname
                                ):
                                    s, ms = divmod(
                                        Int32sl.parse(field_data), 100
                                    )
                                    value = str(
                                        "{}.{:03d}".format(
                                            time.strftime(
                                                "%Y-%m-%d %H:%M:%S", time.gmtime(s)
                                            ),
                                            ms,
                                        )
                                    )
                                else:
                                    value = Int32sl.parse(field_data)
                            elif field.type == "ULONG":
                                value = Int32ul.parse(field_data)
                            elif field.type == "FLOAT":
                                value = Float32l.parse(field_data)
                            elif field.type == "DOUBLE":
                                value = Float64l.parse(field_data)
                            elif field.type == "DECIMAL":
                                # TODO BCD
                                if field_data[0] & 0xF0 == 0xF0:
                                    sign = -1
                                    field_data = bytearray(field_data)
                                    field_data[0] &= 0x0F
                                else:
                                    sign = 1
                                value = (
                                    sign
                                    * int(hexlify(field_data))
                                    / 10**field.decimal_count
                                )
                            elif field.type == "STRING":
                                value = str(
                                    field_data, encoding=self.encoding
                                ).strip()
                            elif field.type == "CSTRING":
                                value = str(
                                    field_data, encoding=self.encoding
                                ).strip()
                            elif field.type == "PSTRING":
                                value = str(
                                    field_data[1 : field_data[0] + 1],
                                    encoding=self.encoding,
                                ).strip()
                            else:
                                # GROUP=0x16
                                # raise ValueError
                                # TODO
                                pass

                            fields[str(field.name)] = value
                        
                        # Process memo fields
                        for memo in table_definition.memos:
                            # Memo fields are stored separately from regular record data
                            # They need to be retrieved from memo pages or external files
                            # For now, we'll set them to None and implement proper retrieval later
                            memo_value = self._get_memo_data(record.data.data.record_number, memo)
                            fields[str(memo.name)] = memo_value
                        
                        # print(fields)
                        yield fields

    def set_current_table(self, tablename):
        self.current_table_number = self.tables.get_number(tablename)

    def to_date(self, value):
        value_date = DATE_STRUCT.parse(value)
        if value_date.year == 0:
            return None
        else:
            return date(value_date.year, value_date.month, value_date.day)

    def to_time(self, value):
        value_time = TIME_STRUCT.parse(value)
        return time(
            value_time.hour,
            value_time.minute,
            value_time.second,
            value_time.centisecond * 10000,
        )

    def _get_memo_data(self, record_number, memo):
        """
        Retrieve memo data for a given record and memo field.
        
        In TopSpeed databases, memo fields are stored separately from regular record data.
        They can be stored in:
        1. External memo files (if external_filename is specified)
        2. Internal memo pages within the main database file
        
        This implementation searches for MEMO records (type 0xFC) that contain the actual memo data.
        """
        try:
            # Check if memo has external file
            if memo.external_filename:
                # TODO: Implement external memo file reading
                return f"[External memo file: {memo.external_filename}]"
            
            # For internal memos, search for memo records (type 0xFC)
            # Memo records contain the actual memo data and reference the parent record
            for page_ref in self.pages.list():
                if self.pages[page_ref].hierarchy_level == 0:
                    for record in TpsRecordsList(
                        self, self.pages[page_ref], encoding=self.encoding, check=self.check
                    ):
                        # Look for memo records that reference this record
                        if (record.type == "MEMO" and 
                            hasattr(record.data.data, 'record_number') and 
                            record.data.data.record_number == record_number):
                            
                            # Found memo data for this record
                            memo_data = record.data.data.memo_data
                            
                            # Handle different memo types
                            if memo.flags.memo_type == "BLOB":
                                # Binary data - return as bytes or hex representation
                                if len(memo_data) > 0:
                                    return memo_data.hex() if len(memo_data) < 100 else f"[BLOB: {len(memo_data)} bytes]"
                                else:
                                    return None
                            else:
                                # Text memo - decode as string
                                if len(memo_data) > 0:
                                    try:
                                        # Try to decode as text, removing null bytes
                                        text_data = memo_data.rstrip(b'\x00')
                                        if text_data:
                                            return text_data.decode(self.encoding, errors='replace').strip()
                                        else:
                                            return None
                                    except:
                                        return f"[Memo data: {len(memo_data)} bytes]"
                                else:
                                    return None
            
            # If no memo data found, return None
            return None
            
        except Exception as e:
            # If memo retrieval fails, return a placeholder
            return f"[Memo retrieval error: {str(e)}]"

        # metadata
        # ?header
        # tables (+ record count from metadata)
        # fields
        # longname fields
        # indexes (and key)
        # memos (and blob)

        # records
        # data
        # index

        # other
        # dimension
        # group


# utils
# convert date
# convert time


def topread(filename: str, password: Union[str, None]) -> List[Dict[str, Any]]:
    tps = (
        TPS(
            filename,
            encoding="cp1251",
            cached=True,
            check=True,
            current_tablename="UNNAMED",
        )
        if password is None
        else TPS(
            filename,
            password=password,
            encoding="cp1251",
            cached=True,
            check=True,
            current_tablename="UNNAMED",
        )
    )

    first_column_name = (
        list(next(iter(tps)).keys())[1].replace("b'", "").replace("'", "")
    )
    position = first_column_name.find(":")
    prefix = first_column_name[: position + 1] if position != -1 else None

    def replace_key(tpskey: str):
        key = tpskey.replace("b'", "").replace("'", "")
        if prefix != None:
            key = key.replace(prefix, "")
        if ":" in key:
            key = key.replace(":", "")

        return key

    keys = {tps_key: replace_key(tps_key) for tps_key in next(iter(tps))}
    return [
        {
            keys[k]: strip_nulls(record[k]) if hasattr(record[k], "find") else record[k]
            for k in record
        }
        for record in tps
    ]


def strip_nulls(row: str):
    first_null = row.find("\x00")
    new_row = row
    return new_row if first_null == -1 else new_row[:first_null]
