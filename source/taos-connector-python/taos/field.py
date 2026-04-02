# encoding:UTF-8
import ctypes
from ctypes import *
from datetime import datetime, timedelta
from decimal import Decimal

import pytz

from taos.constants import FieldType
from taos.error import DatabaseError

_priv_tz = None
_utc_tz = pytz.timezone("UTC")
try:
    _datetime_epoch = datetime.fromtimestamp(0)
except OSError:
    _datetime_epoch = datetime.fromtimestamp(86400) - timedelta(seconds=86400)
_utc_datetime_epoch = _utc_tz.localize(datetime.utcfromtimestamp(0))
_priv_datetime_epoch = None


def set_tz(tz):
    # type: (str) -> None
    global _priv_tz, _priv_datetime_epoch
    _priv_tz = tz
    _priv_datetime_epoch = _utc_datetime_epoch.astimezone(_priv_tz)


def get_tz():
    # type: (str) -> timezone
    return _priv_tz


def _convert_millisecond_to_datetime(milli):
    try:
        if _priv_tz is None:
            return _datetime_epoch + timedelta(seconds=milli / 1000.0)
        return _priv_datetime_epoch + timedelta(seconds=milli / 1000.0)
    except OverflowError:
        # catch OverflowError and pass
        print("WARN: datetime overflow!")
        pass


def _convert_microsecond_to_datetime(micro):
    try:
        if _priv_tz is None:
            return _datetime_epoch + timedelta(seconds=micro / 1000000.0)
        return _priv_datetime_epoch + timedelta(seconds=micro / 1000000.0)
    except OverflowError:
        # catch OverflowError and pass
        print("WARN: datetime overflow!")
        pass


def _convert_nanosecond_to_datetime(nanosec):
    return nanosec


def _crow_timestamp_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bool row to python row."""
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
        None if is_null[i] else _timestamp_converter(ele)
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[: abs(num_of_rows)])
    ]


def _crow_bool_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bool row to python row."""
    return [
        None if is_null[i] else bool(ele)
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_byte))[: abs(num_of_rows)])
    ]


def _crow_tinyint_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C tinyint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_byte))[: abs(num_of_rows)])
    ]


def _crow_tinyint_unsigned_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C unsigned tinyint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_ubyte))[: abs(num_of_rows)])
    ]


def _crow_smallint_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C smallint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_short))[: abs(num_of_rows)])
    ]


def _crow_smallint_unsigned_to_python(
    data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN
):
    """Function to convert C unsigned smallint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_ushort))[: abs(num_of_rows)])
    ]


def _crow_int_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C int row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_int))[: abs(num_of_rows)])
    ]


def _crow_int_unsigned_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C unsigned int row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_uint))[: abs(num_of_rows)])
    ]


def _crow_bigint_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C bigint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_int64))[: abs(num_of_rows)])
    ]


def _crow_bigint_unsigned_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C unsigned bigint row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_uint64))[: abs(num_of_rows)])
    ]


def _crow_float_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C float row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_float))[: abs(num_of_rows)])
    ]


def _crow_double_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C double row to python row."""
    return [
        None if is_null[i] else ele
        for i, ele in enumerate(ctypes.cast(data, ctypes.POINTER(ctypes.c_double))[: abs(num_of_rows)])
    ]


def _crow_decimal_to_python(data, is_null, num_of_rows, nbytes=None, precision=0):
    """Function to convert C Decimal row to python row."""
    ptr = ctypes.cast(data, ctypes.POINTER(ctypes.c_char * 64))
    return [
        None if is_null[i] else Decimal(cast(ptr[i], c_char_p).value.decode("utf-8")) for i in range(abs(num_of_rows))
    ]


def _crow_binary_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert nbytes is not None
    return [
        None if is_null[i] else ele.value.decode("utf-8")
        for i, ele in enumerate((ctypes.cast(data, ctypes.POINTER(ctypes.c_char * nbytes)))[: abs(num_of_rows)])
    ]


def _crow_varbinary_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert nbytes is not None
    return [
        None if is_null[i] else ele.value
        for i, ele in enumerate((ctypes.cast(data, ctypes.POINTER(ctypes.c_char * nbytes)))[: abs(num_of_rows)])
    ]


def _crow_nchar_to_python(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row."""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            try:
                if num_of_rows >= 0:
                    tmpstr = ctypes.c_char_p(data)
                    res.append(tmpstr.value.decode("utf-8"))
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


def _crow_binary_to_python_block(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + nbytes * i, ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(c_char_p(data + nbytes * i + 2), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value.decode("utf-8"))
    return res


def _crow_varbinary_to_python_block(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + nbytes * i, ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(c_char_p(data + nbytes * i + 2), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value)
    return res


def _crow_blob_to_python_block(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + nbytes * i, ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(c_char_p(data + nbytes * i + 4), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value)
    return res


def _crow_nchar_to_python_block(data, is_null, num_of_rows, nbytes=None, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row."""
    assert nbytes is not None
    res = []
    for i in range(abs(num_of_rows)):
        if is_null[i]:
            res.append(None)
        else:
            rbyte = ctypes.cast(data + nbytes * i, ctypes.POINTER(ctypes.c_uint16))[:1].pop()
            chars = ctypes.cast(c_char_p(data + nbytes * i + 2), ctypes.POINTER(c_char * rbyte))
            buffer = create_string_buffer(rbyte + 1)
            buffer[:rbyte] = chars[0][:rbyte]
            res.append(cast(buffer, c_char_p).value.decode("utf-8"))
    return res


def convert_func(field_type: FieldType, decode_binary=True):
    """Get convert func."""
    if (field_type == FieldType.C_VARCHAR or field_type == FieldType.C_BINARY) and not decode_binary:
        return _crow_varbinary_to_python
    return CONVERT_FUNC[field_type]


def convert_block_func(field_type: FieldType, decode_binary=True):
    """Get convert block func."""
    if (field_type == FieldType.C_VARCHAR or field_type == FieldType.C_BINARY) and not decode_binary:
        return _crow_varbinary_to_python_block
    return CONVERT_FUNC_BLOCK[field_type]


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
    FieldType.C_JSON: _crow_nchar_to_python,
    FieldType.C_VARBINARY: _crow_varbinary_to_python,
    FieldType.C_GEOMETRY: _crow_varbinary_to_python,
    FieldType.C_DECIMAL: _crow_decimal_to_python,
    FieldType.C_DECIMAL64: _crow_decimal_to_python,
    FieldType.C_BLOB: _crow_blob_to_python_block,
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
    FieldType.C_JSON: _crow_nchar_to_python_block,
    FieldType.C_VARBINARY: _crow_varbinary_to_python_block,
    FieldType.C_GEOMETRY: _crow_varbinary_to_python_block,
    FieldType.C_DECIMAL: _crow_decimal_to_python,
    FieldType.C_DECIMAL64: _crow_decimal_to_python,
    FieldType.C_BLOB: _crow_blob_to_python_block,
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
        return "{name: %s, type: %d, bytes: %d}" % (
            self.name,
            self.type,
            self.length,
        )

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
        self._iter = 0
        return self

    def __len__(self):
        return self.count
