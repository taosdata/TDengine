# encoding:UTF-8
import ctypes
from .constants import FieldType
from .error import *
from .precision import *
from datetime import datetime
from ctypes import *
import sys

_datetime_epoch = datetime.utcfromtimestamp(0)

def _is_not_none(obj):
    obj != None
class TaosBind(ctypes.Structure):
    _fields_ = [
        ("buffer_type", c_int),
        ("buffer", c_void_p),
        ("buffer_length", c_size_t),
        ("length", POINTER(c_size_t)),
        ("is_null", POINTER(c_int)),
        ("is_unsigned", c_int),
        ("error", POINTER(c_int)),
        ("u", c_int64),
        ("allocated", c_int),
    ]

    def null(self):
        self.buffer_type = FieldType.C_NULL
        self.is_null = pointer(c_int(1))

    def bool(self, value):
        self.buffer_type = FieldType.C_BOOL
        self.buffer = cast(pointer(c_bool(value)), c_void_p)
        self.buffer_length = sizeof(c_bool)

    def tinyint(self, value):
        self.buffer_type = FieldType.C_TINYINT
        self.buffer = cast(pointer(c_int8(value)), c_void_p)
        self.buffer_length = sizeof(c_int8)

    def smallint(self, value):
        self.buffer_type = FieldType.C_SMALLINT
        self.buffer = cast(pointer(c_int16(value)), c_void_p)
        self.buffer_length = sizeof(c_int16)

    def int(self, value):
        self.buffer_type = FieldType.C_INT
        self.buffer = cast(pointer(c_int32(value)), c_void_p)
        self.buffer_length = sizeof(c_int32)

    def bigint(self, value):
        self.buffer_type = FieldType.C_BIGINT
        self.buffer = cast(pointer(c_int64(value)), c_void_p)
        self.buffer_length = sizeof(c_int64)

    def float(self, value):
        self.buffer_type = FieldType.C_FLOAT
        self.buffer = cast(pointer(c_float(value)), c_void_p)
        self.buffer_length = sizeof(c_float)

    def double(self, value):
        self.buffer_type = FieldType.C_DOUBLE
        self.buffer = cast(pointer(c_double(value)), c_void_p)
        self.buffer_length = sizeof(c_double)

    def binary(self, value):
        buffer = None
        length = 0
        if isinstance(value, str):
            bytes = value.encode("utf-8")
            buffer = create_string_buffer(bytes)
            length = len(bytes)
        else:
            buffer = value
            length = len(value)
        self.buffer_type = FieldType.C_BINARY
        self.buffer = cast(buffer, c_void_p)
        self.buffer_length = length
        self.length = pointer(c_size_t(self.buffer_length))

    def timestamp(self, value, precision=PrecisionEnum.Milliseconds):
        if type(value) is datetime:
            if precision == PrecisionEnum.Milliseconds:
                ts = int(round((value - _datetime_epoch).total_seconds() * 1000))
            elif precision == PrecisionEnum.Microseconds:
                ts = int(round((value - _datetime_epoch).total_seconds() * 10000000))
            else:
                raise PrecisionError("datetime do not support nanosecond precision")
        elif type(value) is float:
            if precision == PrecisionEnum.Milliseconds:
                ts = int(round(value * 1000))
            elif precision == PrecisionEnum.Microseconds:
                ts = int(round(value * 10000000))
            else:
                raise PrecisionError("time float do not support nanosecond precision")
        elif isinstance(value, int) and not isinstance(value, bool):
            ts = value
        elif isinstance(value, str):
            value = datetime.fromisoformat(value)
            if precision == PrecisionEnum.Milliseconds:
                ts = int(round(value * 1000))
            elif precision == PrecisionEnum.Microseconds:
                ts = int(round(value * 10000000))
            else:
                raise PrecisionError("datetime do not support nanosecond precision")

        self.buffer_type = FieldType.C_TIMESTAMP
        self.buffer = cast(pointer(c_int64(ts)), c_void_p)
        self.buffer_length = sizeof(c_int64)

    def nchar(self, value):
        buffer = None
        length = 0
        if isinstance(value, str):
            bytes = value.encode("utf-8")
            buffer = create_string_buffer(bytes)
            length = len(bytes)
        else:
            buffer = value
            length = len(value)
        self.buffer_type = FieldType.C_NCHAR
        self.buffer = cast(buffer, c_void_p)
        self.buffer_length = length
        self.length = pointer(c_size_t(self.buffer_length))

    def tinyint_unsigned(self, value):
        self.buffer_type = FieldType.C_TINYINT_UNSIGNED
        self.buffer = cast(pointer(c_uint8(value)), c_void_p)
        self.buffer_length = sizeof(c_uint8)

    def smallint_unsigned(self, value):
        self.buffer_type = FieldType.C_SMALLINT_UNSIGNED
        self.buffer = cast(pointer(c_uint16(value)), c_void_p)
        self.buffer_length = sizeof(c_uint16)

    def int_unsigned(self, value):
        self.buffer_type = FieldType.C_INT_UNSIGNED
        self.buffer = cast(pointer(c_uint32(value)), c_void_p)
        self.buffer_length = sizeof(c_uint32)

    def bigint_unsigned(self, value):
        self.buffer_type = FieldType.C_BIGINT_UNSIGNED
        self.buffer = cast(pointer(c_uint64(value)), c_void_p)
        self.buffer_length = sizeof(c_uint64)


def _datetime_to_timestamp(value, precision):
    # type: (datetime | float | int | str | c_int64, PrecisionEnum) -> c_int64
    if value is None:
        return FieldType.C_BIGINT_NULL
    if type(value) is datetime:
        if precision == PrecisionEnum.Milliseconds:
            return int(round((value - _datetime_epoch).total_seconds() * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round((value - _datetime_epoch).total_seconds() * 10000000))
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif type(value) is float:
        if precision == PrecisionEnum.Milliseconds:
            return int(round(value * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round(value * 10000000))
        else:
            raise PrecisionError("time float do not support nanosecond precision")
    elif isinstance(value, int) and not isinstance(value, bool):
        return c_int64(value)
    elif isinstance(value, str):
        value = datetime.fromisoformat(value)
        if precision == PrecisionEnum.Milliseconds:
            return int(round(value * 1000))
        elif precision == PrecisionEnum.Microseconds:
            return int(round(value * 10000000))
        else:
            raise PrecisionError("datetime do not support nanosecond precision")
    elif isinstance(value, c_int64):
        return value
    return FieldType.C_BIGINT_NULL


class TaosMultiBind(ctypes.Structure):
    _fields_ = [
        ("buffer_type", c_int),
        ("buffer", c_void_p),
        ("buffer_length", c_size_t),
        ("length", POINTER(c_int32)),
        ("is_null", c_char_p),
        ("num", c_int),
    ]

    def null(self, num):
        self.buffer_type = FieldType.C_NULL
        self.is_null = cast((c_char * num)(*[1 for _ in range(num)]), c_char_p)
        self.buffer = c_void_p(None)
        self.num = num

    def bool(self, values):
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BOOL_NULL for v in values])

        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)
        self.buffer_type = FieldType.C_BOOL
        self.buffer_length = sizeof(c_bool)

    def tinyint(self, values):
        self.buffer_type = FieldType.C_TINYINT
        self.buffer_length = sizeof(c_int8)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_TINYINT_NULL for v in values])

        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def smallint(self, values):
        self.buffer_type = FieldType.C_SMALLINT
        self.buffer_length = sizeof(c_int16)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int16 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_SMALLINT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def int(self, values):
        self.buffer_type = FieldType.C_INT
        self.buffer_length = sizeof(c_int32)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int32 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_INT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def bigint(self, values):
        self.buffer_type = FieldType.C_BIGINT
        self.buffer_length = sizeof(c_int64)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int64 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BIGINT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def float(self, values):
        self.buffer_type = FieldType.C_FLOAT
        self.buffer_length = sizeof(c_float)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_float * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_FLOAT_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def double(self, values):
        self.buffer_type = FieldType.C_DOUBLE
        self.buffer_length = sizeof(c_double)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_double * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_DOUBLE_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def binary(self, values):
        self.num = len(values)
        self.buffer = cast(c_char_p("".join(filter(_is_not_none, values)).encode("utf-8")), c_void_p)
        self.length = (c_int * len(values))(*[len(value) if value is not None else 0 for value in values])
        self.buffer_type = FieldType.C_BINARY
        self.is_null = cast((c_byte * self.num)(*[1 if v == None else 0 for v in values]), c_char_p)

    def timestamp(self, values, precision=PrecisionEnum.Milliseconds):
        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_int64 * len(values)
            buffer = buffer_type(*[_datetime_to_timestamp(value, precision) for value in values])

        self.buffer_type = FieldType.C_TIMESTAMP
        self.buffer = cast(buffer, c_void_p)
        self.buffer_length = sizeof(c_int64)
        self.num = len(values)

    def nchar(self, values):
        # type: (list[str]) -> None
        if sys.version_info < (3, 0):
            _bytes = [bytes(value) if value is not None else None for value in values]
            buffer_length = max(len(b) + 1 for b in _bytes if b is not None)
            buffers = [
                create_string_buffer(b, buffer_length) if b is not None else create_string_buffer(buffer_length)
                for b in _bytes
            ]
            buffer_all = b''.join(v[:] for v in buffers)
            self.buffer = cast(c_char_p(buffer_all), c_void_p)
        else:
            _bytes = [value.encode("utf-8") if value is not None else None for value in values]
            buffer_length = max(len(b) for b in _bytes if b is not None)
            self.buffer = cast(
                c_char_p(
                    b"".join(
                        [
                            create_string_buffer(b, buffer_length)
                            if b is not None
                            else create_string_buffer(buffer_length)
                            for b in _bytes
                        ]
                    )
                ),
                c_void_p,
            )
        self.length = (c_int32 * len(values))(*[len(b) if b is not None else 0 for b in _bytes])
        self.buffer_length = buffer_length
        self.num = len(values)
        self.is_null = cast((c_byte * self.num)(*[1 if v == None else 0 for v in values]), c_char_p)
        self.buffer_type = FieldType.C_NCHAR

    def tinyint_unsigned(self, values):
        self.buffer_type = FieldType.C_TINYINT_UNSIGNED
        self.buffer_length = sizeof(c_uint8)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint8 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_TINYINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def smallint_unsigned(self, values):
        self.buffer_type = FieldType.C_SMALLINT_UNSIGNED
        self.buffer_length = sizeof(c_uint16)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint16 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_SMALLINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def int_unsigned(self, values):
        self.buffer_type = FieldType.C_INT_UNSIGNED
        self.buffer_length = sizeof(c_uint32)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint32 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_INT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)

    def bigint_unsigned(self, values):
        self.buffer_type = FieldType.C_BIGINT_UNSIGNED
        self.buffer_length = sizeof(c_uint64)

        try:
            buffer = cast(values, c_void_p)
        except:
            buffer_type = c_uint64 * len(values)
            try:
                buffer = buffer_type(*values)
            except:
                buffer = buffer_type(*[v if v is not None else FieldType.C_BIGINT_UNSIGNED_NULL for v in values])
        self.buffer = cast(buffer, c_void_p)
        self.num = len(values)


def new_bind_param():
    # type: () -> TaosBind
    return TaosBind()


def new_bind_params(size):
    # type: (int) -> Array[TaosBind]
    return (TaosBind * size)()


def new_multi_bind():
    # type: () -> TaosMultiBind
    return TaosMultiBind()


def new_multi_binds(size):
    # type: (int) -> Array[TaosMultiBind]
    return (TaosMultiBind * size)()
