# encoding:UTF-8
import sys
import ctypes
from ctypes import *
from datetime import datetime, timezone
from typing import List, Optional

from taos.cinterface import IS_V3
from taos.constants import FieldType
from taos.precision import PrecisionEnum, PrecisionError
from taos import log
from taos import utils
from taos.field import get_tz
from taos.bind_base import datetime_to_timestamp


IS_NULL_TYPE_FALSE = 0
IS_NULL_TYPE_TRUE = 1
IS_NULL_TYPE_IGNORE = 2


class IgnoreUpdateType:
    def __init__(self, value=None):
        self.value = value

    def __repr__(self):
        return f"IgnoreUpdateType({self.value})"


IGNORE = IgnoreUpdateType()


def get_is_null_type(value) -> int:
    if value == IGNORE:
        return IS_NULL_TYPE_IGNORE
    elif value is None:
        return IS_NULL_TYPE_TRUE
    else:
        return IS_NULL_TYPE_FALSE


class TaosStmt2Bind(ctypes.Structure):
    _fields_ = [
        ("buffer_type", ctypes.c_int),
        ("buffer", ctypes.c_void_p),
        ("length", ctypes.POINTER(ctypes.c_int32)),
        ("is_null", ctypes.c_char_p),
        ("num", ctypes.c_int),
    ]

    #
    # set bind value with field type
    #
    def set_value(self, buffer_type, values, precision=PrecisionEnum.Milliseconds):
        log.debug(f"set_value type={buffer_type} precision={precision} values={values}\n")
        if values is not None:
            utils.checkTypeValid(buffer_type, values)
        if buffer_type == FieldType.C_BOOL:
            self.bool(values)
        elif buffer_type == FieldType.C_TINYINT:
            self.tinyint(values)
        elif buffer_type == FieldType.C_SMALLINT:
            self.smallint(values)
        elif buffer_type == FieldType.C_INT:
            self.int(values)
        elif buffer_type == FieldType.C_BIGINT:
            self.bigint(values)
        elif buffer_type == FieldType.C_FLOAT:
            self.float(values)
        elif buffer_type == FieldType.C_DOUBLE:
            self.double(values)
        elif buffer_type == FieldType.C_VARCHAR:
            self.varchar(values)
        elif buffer_type == FieldType.C_BINARY:
            self.binary(values)
        elif buffer_type == FieldType.C_TIMESTAMP:
            self.timestamp(values, precision)
        elif buffer_type == FieldType.C_NCHAR:
            self.nchar(values)
        elif buffer_type == FieldType.C_TINYINT_UNSIGNED:
            self.tinyint_unsigned(values)
        elif buffer_type == FieldType.C_SMALLINT_UNSIGNED:
            self.smallint_unsigned(values)
        elif buffer_type == FieldType.C_INT_UNSIGNED:
            self.int_unsigned(values)
        elif buffer_type == FieldType.C_BIGINT_UNSIGNED:
            self.bigint_unsigned(values)
        elif buffer_type == FieldType.C_JSON:
            self.json(values)
        elif buffer_type == FieldType.C_VARBINARY:
            self.varbinary(values)
        elif buffer_type == FieldType.C_GEOMETRY:
            self.geometry(values)
        elif buffer_type == FieldType.C_BLOB:
            self.blob(values)

    def numeric_common(self, values, ctypes_type, buffer_null_type, buffer_value_type):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        #
        is_null = [get_is_null_type(value) for value in values]
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = ctypes_type * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(
                    *[buffer_null_type if is_null[idx] > 0 else value for idx, value in enumerate(values)]
                )

        self.buffer = cast(buffer, c_void_p)
        self.buffer_type = buffer_value_type
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*is_null), c_char_p)

    def bool(self, values):
        cnt = len(values)
        for i in range(cnt):
            if type(values[i]) is float:
                values[i] = 0 if values[i] == 0 else 1

        # print(f"after values={values}")
        self.numeric_common(values, c_int8, FieldType.C_BOOL_NULL, FieldType.C_BOOL)

    def tinyint(self, values):
        self.numeric_common(values, c_int8, FieldType.C_TINYINT_NULL, FieldType.C_TINYINT)

    def smallint(self, values):
        self.numeric_common(values, c_int16, FieldType.C_SMALLINT_NULL, FieldType.C_SMALLINT)

    def int(self, values):
        self.numeric_common(values, c_int32, FieldType.C_INT_NULL, FieldType.C_INT)

    def bigint(self, values):
        self.numeric_common(values, c_int64, FieldType.C_BIGINT_NULL, FieldType.C_BIGINT)

    def float(self, values):
        self.numeric_common(values, c_float, FieldType.C_FLOAT_NULL, FieldType.C_FLOAT)

    def double(self, values):
        self.numeric_common(values, c_double, FieldType.C_DOUBLE_NULL, FieldType.C_DOUBLE)

    def timestamp(self, values, precision=PrecisionEnum.Milliseconds):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        #
        is_null = [get_is_null_type(value) for value in values]
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int64 * len(values)
            buffer = buffer_type(
                *[datetime_to_timestamp(value, precision, is_null[idx]) for idx, value in enumerate(values)]
            )

        self.buffer = cast(buffer, c_void_p)
        self.buffer_type = FieldType.C_TIMESTAMP
        self.num = len(values)
        self.is_null = cast((c_char * len(values))(*is_null), c_char_p)

    def _str_to_buffer(self, values, encode=True):
        if type(values) is not tuple and type(values) is not list:
            values = tuple([values])
        #
        is_null = [get_is_null_type(value) for value in values]
        self.num = len(values)
        self.is_null = cast((c_byte * self.num)(*is_null), c_char_p)

        if sum([1 if v > 0 else 0 for v in is_null]) == self.num:
            self.length = (c_int32 * len(values))(0 * self.num)
            return

        _bytes_list = []
        if sys.version_info < (3, 0):
            _bytes_list = [None if is_null[idx] > 0 else bytes(value) for idx, value in enumerate(values)]
        else:
            if encode:
                _bytes_list = [None if is_null[idx] > 0 else value.encode("utf-8") for idx, value in enumerate(values)]
            else:
                _bytes_list = [None if is_null[idx] > 0 else bytes(value) for idx, value in enumerate(values)]
            #

        _bytes = b"".join([b for b in _bytes_list if b is not None])
        self.buffer = cast(create_string_buffer(_bytes), c_void_p)
        self.length = (c_int32 * len(values))(*[len(b) if b is not None else 0 for b in _bytes_list])

    def binary(self, values):
        self.buffer_type = FieldType.C_BINARY
        self._str_to_buffer(values)

    def nchar(self, values):
        # type: (list[str]) -> None
        self.buffer_type = FieldType.C_NCHAR
        self._str_to_buffer(values)

    def json(self, values):
        # type: (list[str]) -> None
        self.buffer_type = FieldType.C_JSON
        self._str_to_buffer(values)

    def tinyint_unsigned(self, values):
        self.numeric_common(values, c_uint8, FieldType.C_TINYINT_UNSIGNED_NULL, FieldType.C_TINYINT_UNSIGNED)

    def smallint_unsigned(self, values):
        self.numeric_common(values, c_uint16, FieldType.C_SMALLINT_UNSIGNED_NULL, FieldType.C_SMALLINT_UNSIGNED)

    def int_unsigned(self, values):
        self.numeric_common(values, c_uint32, FieldType.C_INT_UNSIGNED_NULL, FieldType.C_INT_UNSIGNED)

    def bigint_unsigned(self, values):
        self.numeric_common(values, c_uint64, FieldType.C_BIGINT_UNSIGNED_NULL, FieldType.C_BIGINT_UNSIGNED)

    def varchar(self, values):
        self.buffer_type = FieldType.C_VARCHAR
        self._str_to_buffer(values)

    def varbinary(self, values):
        self.buffer_type = FieldType.C_VARBINARY
        self._str_to_buffer(values, encode=False)

    def geometry(self, values):
        self.buffer_type = FieldType.C_GEOMETRY
        self._str_to_buffer(values, encode=False)

    def blob(self, values):
        self.buffer_type = FieldType.C_BLOB
        self._str_to_buffer(values, encode=False)


class TaosStmt2BindV(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_int),
        ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
        ("tags", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind))),
        ("bind_cols", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind))),
    ]

    def init(
        self,
        count: int,
        tbnames,  # List[str],
        tags,  # Optional[List[Array[TaosStmt2Bind]]],
        bind_cols,  # Optional[List[Array[TaosStmt2Bind]]]
    ):
        self.count = count
        if tbnames is not None:
            self.tbnames = (ctypes.c_char_p * count)()
            for i, tbname in enumerate(tbnames):
                self.tbnames[i] = cast(self.str_to_buffer(tbname), c_char_p)
        else:
            self.tbnames = None

        if tags is not None:
            self.tags = (ctypes.POINTER(TaosStmt2Bind) * len(tags))()
            for i, tag_list in enumerate(tags):
                self.tags[i] = tag_list
        else:
            self.tags = None

        if bind_cols is not None:
            self.bind_cols = (ctypes.POINTER(TaosStmt2Bind) * count)()
            for i, col_list in enumerate(bind_cols):
                self.bind_cols[i] = col_list
        else:
            self.bind_cols = None
        #

    def str_to_buffer(self, value: str, encode=True):
        buffer = None
        if value is not None:
            _bytes = None
            if encode:
                _bytes = value.encode("utf-8")
            else:
                _bytes = bytes(value)

            buffer = cast(create_string_buffer(_bytes), c_void_p)
        else:
            buffer = None

        return buffer

    def get_address(self) -> ctypes.pointer:
        return c_void_p(ctypes.addressof(self))


def new_stmt2_binds(size: int):  # -> Array[TaosStmt2Bind]:
    return (TaosStmt2Bind * size)()


def new_bindv(
    count: int,
    tbnames,  # Optional[List[str]]
    tags,  # Optional[List[Array[TaosStmt2Bind]]],
    bind_cols,  # Optional[List[Array[TaosStmt2Bind]]]
) -> TaosStmt2BindV:
    bindv = TaosStmt2BindV()
    bindv.init(count, tbnames, tags, bind_cols)
    return bindv
