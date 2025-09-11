from construct import Byte, Bytes, Enum, IfThenElse, Peek, PaddedString, Struct, Switch, Int32ub, Int16ul, Int32ul, If

from .tpspage import PAGE_HEADER_STRUCT
from .utils import check_value

record_encoding = None

RECORD_TYPE = Enum(Byte,
                   NULL=None,
                   DATA=0xF3,
                   METADATA=0xF6,
                   TABLE_DEFINITION=0xFA,
                   TABLE_NAME=0xFE,
                   MEMO=0xFC,
                   _default_='INDEX', )

DATA_RECORD_DATA = Struct(
                          "record_number" / Int32ub,
                          "data" / Bytes(lambda ctx: ctx._.data_size - 9))

METADATA_RECORD_DATA = Struct(
                              "metadata_type" / Byte,
                              "metadata_record_count" / Int32ul,
                              "metadata_record_last_access" / Int32ul)

TABLE_DEFINITION_RECORD_DATA = Struct(
                                      "table_definition_bytes" / Bytes(lambda ctx: ctx._.data_size - 5))

INDEX_RECORD_DATA = Struct(
                           "data" / Bytes(lambda ctx: ctx._.data_size - 10),
                           "record_number" / Int32ul)

MEMO_RECORD_DATA = Struct(
                         "record_number" / Int32ub,
                         "memo_data" / Bytes(lambda ctx: ctx._.data_size - 9))

RECORD_STRUCT = Struct(
                       "data_size" / Int16ul,
                       "first_byte" / Peek(Byte),
                       "table_number" / Int32ub,
                       "type" / RECORD_TYPE,
                       "data" / Switch(lambda ctx: ctx.type,
                                {
                                    'DATA': DATA_RECORD_DATA,
                                    'METADATA': METADATA_RECORD_DATA,
                                    'TABLE_DEFINITION': TABLE_DEFINITION_RECORD_DATA,
                                    'INDEX': INDEX_RECORD_DATA,
                                    'MEMO': MEMO_RECORD_DATA,
                                    'TABLE_NAME': Struct("table_name" / PaddedString(lambda ctx: ctx._.data_size - 9, 'ascii'))
                                }))


class TpsRecord:
    def __init__(self, header_size, data):
        self.header_size = header_size
        self.data_bytes = data
        # print(data)

        data_size = len(self.data_bytes) - 2

        # print('data_size', data_size, header_size)

        if data_size == 0:
            self.type = 'NULL'
        else:
            # Check if this is a TABLE_NAME record (starts with 0xFE) before parsing with RECORD_STRUCT
            if len(self.data_bytes) > 2 and self.data_bytes[2] == 0xFE:
                # Handle TABLE_NAME records separately - they have a different structure
                self.type = 'TABLE_NAME'
                # Manually parse the table name from the raw bytes
                # Structure: data_size(2) + first_byte(1) + table_name(variable) + table_number(1)
                if len(self.data_bytes) > 3:
                    # Calculate the exact name length: data_size - 4 (header bytes: data_size + first_byte + table_number)
                    data_size = int.from_bytes(self.data_bytes[0:2], 'little')
                    name_length = data_size - 4
                    if name_length > 0:
                        # Extract table name starting from byte 3
                        name_bytes = self.data_bytes[3:3+name_length]
                        try:
                            table_name = name_bytes.decode('ascii', errors='replace').rstrip('\x00')
                            # Extract table number from the last byte
                            table_number = self.data_bytes[-1] if len(self.data_bytes) > 0 else 0
                            # Create a mock data structure for TABLE_NAME
                            self.data = type('MockData', (), {
                                'table_name': table_name,
                                'table_number': table_number,  # Extract from raw data
                                'data_size': data_size
                            })()
                        except:
                            self.data = type('MockData', (), {
                                'table_name': '',
                                'table_number': 0,
                                'data_size': data_size
                            })()
                    else:
                        # Extract table number from the last byte even if no name
                        table_number = self.data_bytes[-1] if len(self.data_bytes) > 0 else 0
                        self.data = type('MockData', (), {
                            'table_name': '',
                            'table_number': table_number,
                            'data_size': data_size
                        })()
            else:
                # Parse other record types with RECORD_STRUCT
                self.data = RECORD_STRUCT.parse(self.data_bytes)
                self.type = self.data.type


class TpsRecordsList:
    def __init__(self, tps, tps_page, encoding=None, check=False):
        self.tps = tps
        self.check = check
        self.tps_page = tps_page
        self.encoding = encoding
        global record_encoding
        record_encoding = encoding
        self.__records = []

        if self.tps_page.hierarchy_level == 0:
            if self.tps_page.ref in self.tps.cache_pages:
                self.__records = tps.cache_pages[self.tps_page.ref]
            else:
                data = self.tps.read(self.tps_page.size - PAGE_HEADER_STRUCT.sizeof(),
                                     self.tps_page.ref * 0x100 + self.tps.header.size + PAGE_HEADER_STRUCT.sizeof())

                if self.tps_page.uncompressed_size > self.tps_page.size:
                    data = self.__uncompress(data)

                    if self.check:
                        check_value('record_data.size', len(data) + PAGE_HEADER_STRUCT.sizeof(),
                                    tps_page.uncompressed_size)

                record_data = b''
                pos = 0
                record_size = 0
                record_header_size = 0

                while pos < len(data):
                    byte_counter = data[pos]
                    pos += 1
                    if (byte_counter & 0x80) == 0x80:
                        record_size = data[pos + 1] * 0x100 + data[pos]
                        pos += 2
                    if (byte_counter & 0x40) == 0x40:
                        record_header_size = data[pos + 1] * 0x100 + data[pos]
                        pos += 2
                    byte_counter &= 0x3F
                    new_data_size = record_size - byte_counter
                    record_data = record_data[:byte_counter] + data[pos:pos + new_data_size]
                    self.__records.append(TpsRecord(record_header_size, Int16ul.build(record_size)
                                                    + record_data))
                    pos += new_data_size

                if self.tps.cached and self.tps_page.ref not in tps.cache_pages:
                    tps.cache_pages[self.tps_page.ref] = self.__records

    def __uncompress(self, data):
        pos = 0
        result = b''
        while pos < len(data):
            repeat_rel_offset = data[pos]
            pos += 1

            if repeat_rel_offset > 0x7F:
                # size repeat_count = 2 bytes
                repeat_rel_offset = ((data[pos] << 8) + ((repeat_rel_offset & 0x7F) << 1)) >> 1
                pos += 1

            result += data[pos:pos + repeat_rel_offset]
            pos += repeat_rel_offset

            if pos < len(data):
                repeat_byte = bytes(result[-1:])
                repeat_count = data[pos]
                pos += 1

                if repeat_count > 0x7F:
                    repeat_count = ((data[pos] << 8) + ((repeat_count & 0x7F) << 1)) >> 1
                    pos += 1

                result += repeat_byte * repeat_count
        return result

    def __getitem__(self, key):
        return self.__records[key]
