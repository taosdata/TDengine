# encoding:UTF-8
import ctypes
import math
import datetime
from ctypes import *

from .constants import FieldType
from .error import *

_datetime_epoch = datetime.datetime.fromtimestamp(0)

def _convert_millisecond_to_datetime(milli):
    return _datetime_epoch + datetime.timedelta(seconds=milli / 1000.0)


def _convert_microsecond_to_datetime(micro):
    return _datetime_epoch + datetime.timedelta(seconds=micro / 1000000.0)


def _convert_nanosecond_to_datetime(nanosec):
    return nanosec


def _crow_timestamp_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bool row to python row"""
    _timestamp_converter = _convert_millisecond_to_datetime
    if precision == FieldType.C_TIMESTAMP_MILLI:
        _timestamp_converter = _convert_millisecond_to_datetime
    elif precision == FieldType.C_TIMESTAMP_MICRO:
        _timestamp_converter = _convert_microsecond_to_datetime
    elif precision == FieldType.C_TIMESTAMP_NANO:
        _timestamp_converter = _convert_nanosecond_to_datetime
    else:
        raise DatabaseError("Unknown precision returned from database")

    return [
        None if ele == FieldType.C_BIGINT_NULL else _timestamp_converter(ele)
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[: abs(num_of_rows)]
    ]


def _crow_bool_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bool row to python row"""
    return [
        None if ele == FieldType.C_BOOL_NULL else bool(ele)
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_byte))[: abs(num_of_rows)]
    ]


def _crow_tinyint_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C tinyint row to python row"""
    return [
        None if ele == FieldType.C_TINYINT_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_byte))[: abs(num_of_rows)]
    ]


def _crow_tinyint_unsigned_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C tinyint row to python row"""
    return [
        None if ele == FieldType.C_TINYINT_UNSIGNED_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_ubyte))[: abs(num_of_rows)]
    ]


def _crow_smallint_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C smallint row to python row"""
    return [
        None if ele == FieldType.C_SMALLINT_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_short))[: abs(num_of_rows)]
    ]


def _crow_smallint_unsigned_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C smallint row to python row"""
    return [
        None if ele == FieldType.C_SMALLINT_UNSIGNED_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_ushort))[: abs(num_of_rows)]
    ]


def _crow_int_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C int row to python row"""
    return [
        None if ele == FieldType.C_INT_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_int))[: abs(num_of_rows)]
    ]


def _crow_int_unsigned_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C int row to python row"""
    return [
        None if ele == FieldType.C_INT_UNSIGNED_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_uint))[: abs(num_of_rows)]
    ]


def _crow_bigint_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bigint row to python row"""
    return [
        None if ele == FieldType.C_BIGINT_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[: abs(num_of_rows)]
    ]


def _crow_bigint_unsigned_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bigint row to python row"""
    return [
        None if ele == FieldType.C_BIGINT_UNSIGNED_NULL else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_uint64))[: abs(num_of_rows)]
    ]


def _crow_float_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C float row to python row"""
    return [
        None if math.isnan(ele) else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_float))[: abs(num_of_rows)]
    ]


def _crow_double_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C double row to python row"""
    return [
        None if math.isnan(ele) else ele
        for ele in ctypes.cast(data, ctypes.POINTER(ctypes.c_double))[: abs(num_of_rows)]
    ]


def _crow_binary_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row"""
    assert nbytes is not None
    return [
        None if ele.value[0:1] == FieldType.C_BINARY_NULL else ele.value.decode("utf-8")
        for ele in (ctypes.cast(data, ctypes.POINTER(ctypes.c_char * nbytes)))[: abs(num_of_rows)]
    ]


def _crow_nchar_to_python(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row"""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        try:
            if num_of_rows >= 0:
                tmpstr = ctypes.c_char_p(data)
                res.append(tmpstr.value.decode())
            else:
                res.append(
                    (
                        ctypes.cast(
                            data + nbytes * i,
                            ctypes.POINTER(ctypes.c_wchar * (nbytes // 4)),
                        )
                    )[0].value
                )
        except ValueError:
            res.append(None)

    return res


def _crow_binary_to_python_block(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row"""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        try:
            rbyte = ctypes.cast(data + nbytes * i, ctypes.POINTER(ctypes.c_short))[:1].pop()
            tmpstr = ctypes.c_char_p(data + nbytes * i + 2)
            res.append(tmpstr.value.decode()[0:rbyte])
        except ValueError:
            res.append(None)
    return res


def _crow_nchar_to_python_block(data, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row"""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        try:
            tmpstr = ctypes.c_char_p(data + nbytes * i + 2)
            res.append(tmpstr.value.decode())
        except ValueError:
            res.append(None)
    return res


CONVERT_FUNC = {
    FieldType.C_BOOL: _crow_bool_to_python,
    FieldType.C_TINYINT: _crow_tinyint_to_python,
    FieldType.C_SMALLINT: _crow_smallint_to_python,
    FieldType.C_INT: _crow_int_to_python,
    FieldType.C_BIGINT: _crow_bigint_to_python,
    FieldType.C_FLOAT: _crow_float_to_python,
    FieldType.C_DOUBLE: _crow_double_to_python,
    FieldType.C_BINARY: _crow_binary_to_python,
    FieldType.C_TIMESTAMP: _crow_timestamp_to_python,
    FieldType.C_NCHAR: _crow_nchar_to_python,
    FieldType.C_TINYINT_UNSIGNED: _crow_tinyint_unsigned_to_python,
    FieldType.C_SMALLINT_UNSIGNED: _crow_smallint_unsigned_to_python,
    FieldType.C_INT_UNSIGNED: _crow_int_unsigned_to_python,
    FieldType.C_BIGINT_UNSIGNED: _crow_bigint_unsigned_to_python,
}

CONVERT_FUNC_BLOCK = {
    FieldType.C_BOOL: _crow_bool_to_python,
    FieldType.C_TINYINT: _crow_tinyint_to_python,
    FieldType.C_SMALLINT: _crow_smallint_to_python,
    FieldType.C_INT: _crow_int_to_python,
    FieldType.C_BIGINT: _crow_bigint_to_python,
    FieldType.C_FLOAT: _crow_float_to_python,
    FieldType.C_DOUBLE: _crow_double_to_python,
    FieldType.C_BINARY: _crow_binary_to_python_block,
    FieldType.C_TIMESTAMP: _crow_timestamp_to_python,
    FieldType.C_NCHAR: _crow_nchar_to_python_block,
    FieldType.C_TINYINT_UNSIGNED: _crow_tinyint_unsigned_to_python,
    FieldType.C_SMALLINT_UNSIGNED: _crow_smallint_unsigned_to_python,
    FieldType.C_INT_UNSIGNED: _crow_int_unsigned_to_python,
    FieldType.C_BIGINT_UNSIGNED: _crow_bigint_unsigned_to_python,
}

# Corresponding TAOS_FIELD structure in C


class TaosField(ctypes.Structure):
    _fields_ = [
        ("_name", ctypes.c_char * 65),
        ("_type", ctypes.c_uint8),
        ("_bytes", ctypes.c_uint16),
    ]

    @property
    def name(self):
        return self._name.decode("utf-8")

    @property
    def length(self):
        """alias to self.bytes"""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def type(self):
        return self._type

    def __dict__(self):
        return {"name": self.name, "type": self.type, "bytes": self.length}

    def __str__(self):
        return "{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.length)

    def __getitem__(self, item):
        return getattr(self, item)


class TaosFields(object):
    def __init__(self, fields, count):
        if isinstance(fields, c_void_p):
            self._fields = cast(fields, POINTER(TaosField))
        if isinstance(fields, POINTER(TaosField)):
            self._fields = fields
        self._count = count
        self._iter = 0

    def as_ptr(self):
        return self._fields

    @property
    def count(self):
        return self._count

    @property
    def fields(self):
        return self._fields

    def __next__(self):
        return self._next_field()
    
    def next(self):
        return self._next_field()

    def _next_field(self):
        if self._iter < self.count:
            field = self._fields[self._iter]
            self._iter += 1
            return field
        else:
            raise StopIteration

    def __getitem__(self, item):
        return self._fields[item]

    def __iter__(self):
        return self

    def __len__(self):
        return self.count
