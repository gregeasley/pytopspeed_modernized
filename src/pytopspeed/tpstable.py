"""
TPS File Table
"""

from construct import Array, BitsInteger, BitStruct, Byte, Const, CString, Enum, Flag, If, Padding, Struct, Int16ul

from .tpsrecord import TpsRecordsList


FIELD_TYPE_STRUCT = Enum(Byte,
                         BYTE=0x1,
                         SHORT=0x2,
                         USHORT=0x3,
                         # date format 0xYYYYMMDD
                         DATE=0x4,
                         # time format 0xHHMMSSHS
                         TIME=0x5,
                         LONG=0x6,
                         ULONG=0x7,
                         FLOAT=0x8,
                         DOUBLE=0x9,
                         DECIMAL=0x0A,
                         STRING=0x12,
                         CSTRING=0x13,
                         PSTRING=0x14,
                         # compound data structure
                         GROUP=0x16,
                         # LIKE (inherited data type)
)

TABLE_DEFINITION_FIELD_STRUCT = Struct(
                                       "type" / FIELD_TYPE_STRUCT,
                                       # data offset in record
                                       "offset" / Int16ul,
                                       "name" / CString("ascii"),
                                       "array_element_count" / Int16ul,
                                       "size" / Int16ul,
                                       # 1, if fields overlap (OVER attribute), or 0
                                       "overlaps" / Int16ul,
                                       # record number
                                       "number" / Int16ul,
                                       "array_element_size" / If(lambda x: x['type'] == 'STRING', Int16ul),
                                       "template" / If(lambda x: x['type'] == 'STRING', Int16ul),
                                       "array_element_size" / If(lambda x: x['type'] == 'CSTRING', Int16ul),
                                       "template" / If(lambda x: x['type'] == 'CSTRING', Int16ul),
                                       "array_element_size" / If(lambda x: x['type'] == 'PSTRING', Int16ul),
                                       "template" / If(lambda x: x['type'] == 'PSTRING', Int16ul),
                                       "array_element_size" / If(lambda x: x['type'] == 'PICTURE', Int16ul),
                                       "template" / If(lambda x: x['type'] == 'PICTURE', Int16ul),
                                       "decimal_count" / If(lambda x: x['type'] == 'DECIMAL', Byte),
                                       "decimal_size" / If(lambda x: x['type'] == 'DECIMAL', Byte),
                                       )

INDEX_TYPE_STRUCT = Enum(BitsInteger(2),
                         KEY=0,
                         INDEX=1,
                         DYNAMIC_INDEX=2)

INDEX_FIELD_ORDER_TYPE_STRUCT = Enum(Int16ul,
                                     ASCENDING=0,
                                     DESCENDING=1,
                                     _default_='DESCENDING')

TABLE_DEFINITION_INDEX_STRUCT = Struct(
                                       # May be external_filename
                                       # if external_filename == 0, no external file index
                                       "external_filename" / CString("ascii"),
                                       "index_mark" / If(lambda x: len(x['external_filename']) == 0, Const(1, Byte)),
                                       "name" / CString("ascii"),
                                       "flags" / BitStruct(
                                                       Padding(1),
                                                       "type" / INDEX_TYPE_STRUCT,
                                                       Padding(2),
                                                       "NOCASE" / Flag,
                                                       "OPT" / Flag,
                                                       "DUP" / Flag),
                                       "field_count" / Int16ul,
                                       "fields" / Array(lambda x: x['field_count'],
                                             Struct(
                                                    "field_number" / Int16ul,
                                                    "order_type" / INDEX_FIELD_ORDER_TYPE_STRUCT)), )

MEMO_TYPE_STRUCT = Enum(Flag,
                        MEMO=0,
                        BLOB=1)

TABLE_DEFINITION_MEMO_STRUCT = Struct(
                                      # May be external_filename
                                      # if external_filename == 0, no external file index
                                      "external_filename" / CString("ascii"),
                                      "memo_mark" / If(lambda x: len(x['external_filename']) == 0, Const(1, Byte)),
                                      "name" / CString("ascii"),
                                      "size" / Int16ul,
                                      "flags" / BitStruct(
                                                      Padding(5),
                                                      "memo_type" / MEMO_TYPE_STRUCT,
                                                      "BINARY" / Flag,
                                                      "Flag" / Flag,
                                                      Padding(8)), )

TABLE_DEFINITION_STRUCT = Struct(
                                 "min_version_driver" / Int16ul,
                                 # sum all fields sizes in record
                                 "record_size" / Int16ul,
                                 "field_count" / Int16ul,
                                 "memo_count" / Int16ul,
                                 "index_count" / Int16ul,
                                 "fields" / Array(lambda x: x['field_count'], TABLE_DEFINITION_FIELD_STRUCT),
                                 "memos" / Array(lambda x: x['memo_count'], TABLE_DEFINITION_MEMO_STRUCT),
                                 "indexes" / Array(lambda x: x['index_count'], TABLE_DEFINITION_INDEX_STRUCT), )


class TpsTable:
    def __init__(self, number):
        self.number = number
        self.name = ''
        self.definition_bytes = {}
        self.definition = ''
        self.statistics = {}

    @property
    def iscomplete(self):
        # TODO check all parts complete
        if self.name != '':
            self.get_definition()
            return True
        else:
            return False

    def add_definition(self, definition):
        portion_number = Int16ul.parse(definition[:2])
        self.definition_bytes[portion_number] = definition[2:]

    def add_statistics(self, statistics_struct):
        # TODO remove metadatatype from staticstics_struct
        self.statistics[statistics_struct.metadata_type] = statistics_struct

    def get_definition(self):
        definition_bytes = b''
        for value in self.definition_bytes.values():
            definition_bytes += value
        self.definition = TABLE_DEFINITION_STRUCT.parse(definition_bytes)
        return self.definition

    def set_name(self, name):
        self.name = name


class TpsTablesList:
    def __init__(self, tps, encoding=None, check=False):
        self.__tps = tps
        self.encoding = encoding
        self.check = check
        self.__tables = {}

        # First pass: collect TABLE_NAME records and TABLE_DEFINITION table numbers in order
        table_names = []
        table_def_numbers = []
        seen_table_nums = set()

        for page_ref in self.__tps.pages.list():
            if self.__tps.pages[page_ref].hierarchy_level == 0:
                for record in TpsRecordsList(self.__tps, self.__tps.pages[page_ref],
                                             encoding=self.encoding, check=self.check):
                    if record.type == 'TABLE_NAME':
                        table_names.append(record)
                    elif record.type == 'TABLE_DEFINITION':
                        table_num = record.data.table_number
                        if table_num not in seen_table_nums:
                            table_def_numbers.append(table_num)
                            seen_table_nums.add(table_num)

        # Now that TABLE_NAME records have the correct table numbers from raw data,
        # we can use direct mapping instead of positional mapping
        table_name_mapping = {}
        
        # Map TABLE_NAME records to their correct table numbers (from raw data)
        for table_name_record in table_names:
            table_number = table_name_record.data.table_number
            table_name = table_name_record.data.table_name
            if table_number > 0:  # Valid table number
                table_name_mapping[table_number] = table_name

        # Second pass: process all records with correct table numbers
        for page_ref in reversed(self.__tps.pages.list()):
            if self.__tps.pages[page_ref].hierarchy_level == 0:
                for record in TpsRecordsList(self.__tps, self.__tps.pages[page_ref],
                                             encoding=self.encoding, check=self.check):
                    if record.type != 'NULL' and record.data.table_number not in self.__tables.keys():
                        self.__tables[record.data.table_number] = TpsTable(record.data.table_number)
                    if record.type == 'TABLE_NAME':
                        # Use the correct table name from the mapping
                        if record.data.table_number in table_name_mapping:
                            self.__tables[record.data.table_number].set_name(table_name_mapping[record.data.table_number])
                    if record.type == 'TABLE_DEFINITION':
                        self.__tables[record.data.table_number].add_definition(record.data.data.table_definition_bytes)
                    if record.type == 'METADATA':
                        self.__tables[record.data.table_number].add_statistics(record.data.data)

    def __iscomplete(self):
        for i in self.__tables:
            if not self.__tables[i].iscomplete:
                return False
        if len(self.__tables) == 0:
            return False
        else:
            return True

    def get_definition(self, number):
        return self.__tables[number].get_definition()

    def get_number(self, name):
        for i in self.__tables:
            if self.__tables[i].name == name:
                return i
