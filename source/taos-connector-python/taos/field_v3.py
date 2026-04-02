# encoding:UTF-8
import ctypes

from taos.constants import FieldType

_RTYPE = ctypes.c_uint16
_RTYPE_SIZE = ctypes.sizeof(_RTYPE)

_BLOB_RTYPE = ctypes.c_uint32
_BLOB_RTYPE_SIZE = ctypes.sizeof(_BLOB_RTYPE)


def _crow_binary_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C binary row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = _RTYPE.from_address(data + offsets[i]).value
            chars = (ctypes.c_char * rbyte).from_address(data + offsets[i] + _RTYPE_SIZE).raw.decode("utf-8")
            res.append(chars)
    return res


def _crow_nchar_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C nchar row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = _RTYPE.from_address(data + offsets[i]).value
            chars = (ctypes.c_char * rbyte).from_address(data + offsets[i] + _RTYPE_SIZE).raw.decode("utf-8")
            res.append(chars)
    return res


def _crow_varbinary_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C varbinary row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = _RTYPE.from_address(data + offsets[i]).value
            chars = (ctypes.c_char * rbyte).from_address(data + offsets[i] + _RTYPE_SIZE).raw
            res.append(chars)
    return res


def _crow_blob_to_python_block_v3(data, is_null, num_of_rows, offsets, precision=FieldType.C_TIMESTAMP_UNKNOWN):
    """Function to convert C varbinary row to python row."""
    assert offsets is not None
    res = []
    for i in range(abs(num_of_rows)):
        if offsets[i] == -1:
            res.append(None)
        else:
            rbyte = _RTYPE.from_address(data + offsets[i]).value
            chars = (ctypes.c_char * rbyte).from_address(data + offsets[i] + _BLOB_RTYPE_SIZE).raw
            res.append(chars)
    return res


def convert_block_func_v3(field_type: FieldType, decode_binary=True):
    """Get convert block func."""
    if (field_type == FieldType.C_VARCHAR or field_type == FieldType.C_BINARY) and not decode_binary:
        return _crow_varbinary_to_python_block_v3
    return CONVERT_FUNC_BLOCK_v3[field_type]


CONVERT_FUNC_BLOCK_v3 = {
    FieldType.C_VARCHAR: _crow_binary_to_python_block_v3,
    FieldType.C_BINARY: _crow_binary_to_python_block_v3,
    FieldType.C_NCHAR: _crow_nchar_to_python_block_v3,
    FieldType.C_JSON: _crow_nchar_to_python_block_v3,
    FieldType.C_VARBINARY: _crow_varbinary_to_python_block_v3,
    FieldType.C_GEOMETRY: _crow_varbinary_to_python_block_v3,
    FieldType.C_BLOB: _crow_blob_to_python_block_v3,
}


# Corresponding TAOS_FIELD structure in C


class TaosField(ctypes.Structure):
    _fields_ = [
        ("_name", ctypes.c_char * 65),
        ("_type", ctypes.c_uint8),
        ("_bytes", ctypes.c_uint32),
    ]

    @property
    def name(self):
        return self._name.decode("utf-8")

    @property
    def length(self):
        """Alias to self.bytes."""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def type(self):
        return self._type

    def __dict__(self):
        """Construct dict."""
        return {"name": self.name, "type": self.type, "bytes": self.length}

    def __str__(self):
        """Construct str."""
        return "{name: %s, type: %d, bytes: %d}" % (self.name, self.type, self.length)

    def __getitem__(self, item):
        """Get attr."""
        return getattr(self, item)


class TaosFieldE(ctypes.Structure):
    _fields_ = [
        ("_name", ctypes.c_char * 65),
        ("_type", ctypes.c_int8),
        ("_precision", ctypes.c_uint8),
        ("_scale", ctypes.c_uint8),
        ("_bytes", ctypes.c_int32),
    ]

    @property
    def name(self):
        return self._name.decode("utf-8")

    @property
    def length(self):
        """Alias to self.bytes."""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def type(self):
        return self._type

    @property
    def precision(self):
        return self._precision

    @property
    def scale(self):
        return self._scale

    def __dict__(self):
        """Construct dict."""
        return {
            "name": self.name,
            "type": self.type,
            "precision": self.precision,
            "scale": self.scale,
            "bytes": self.length,
        }

    def __str__(self):
        """Construct str."""
        return "{name: %s, type: %d, precision: %d, scale: %d, bytes: %d}" % (
            self.name,
            self.type,
            self.precision,
            self.scale,
            self.length,
        )

    def __getitem__(self, item):
        """Get attr."""
        return getattr(self, item)


class TaosFields(object):
    def __init__(self, fields, count):
        """Init class."""
        if isinstance(fields, ctypes.c_void_p):
            self._fields = ctypes.cast(fields, ctypes.POINTER(TaosField))
        if isinstance(fields, ctypes.POINTER(TaosField)):
            self._fields = fields
        self._count = count
        self._iter = 0

    def as_ptr(self):
        """Return as ptr."""
        return self._fields

    @property
    def count(self):
        """Return count."""
        return self._count

    @property
    def fields(self):
        """Return fields."""
        return self._fields

    def __next__(self):
        """Next field."""
        return self._next_field()

    def next(self):
        """Next field."""
        return self._next_field()

    def _next_field(self):
        """Iter next_field."""
        if self._iter < self.count:
            field = self._fields[self._iter]
            self._iter += 1
        else:
            raise StopIteration
        return field

    def __getitem__(self, item):
        """Return field item."""
        return self._fields[item]

    def __iter__(self):
        """To iter."""
        self._iter = 0
        return self

    def __len__(self):
        """Get len."""
        return self.count

    def __str__(self):
        """Print"""
        return ",".join(str(f) for f in self)


class TaosFieldEs(object):
    def __init__(self, fields, count):
        """Init class."""
        if isinstance(fields, ctypes.c_void_p):
            self._fields = ctypes.cast(fields, ctypes.POINTER(TaosFieldE))
        if isinstance(fields, ctypes.POINTER(TaosField)):
            self._fields = fields
        self._count = count
        self._iter = 0

    def as_ptr(self):
        """Return as ptr."""
        return self._fields

    @property
    def count(self):
        """Return count."""
        return self._count

    @property
    def fields(self):
        """Return fields."""
        return self._fields

    def __next__(self):
        """Next field."""
        return self._next_field()

    def next(self):
        """Next field."""
        return self._next_field()

    def _next_field(self):
        """Iter next_field."""
        if self._iter < self.count:
            field = self._fields[self._iter]
            self._iter += 1
        else:
            raise StopIteration
        return field

    def __getitem__(self, item):
        """Return field item."""
        return self._fields[item]

    def __iter__(self):
        """To iter."""
        self._iter = 0
        return self

    def __len__(self):
        """Get len."""
        return self.count

    def __str__(self):
        """Print"""
        return ",".join(str(f) for f in self)


TAOS_FIELD_T = ctypes.c_int


class TaosFieldAll(ctypes.Structure):
    _fields_ = [
        ("_name", ctypes.c_char * 65),
        ("_type", ctypes.c_int8),
        ("_precision", ctypes.c_uint8),
        ("_scale", ctypes.c_uint8),
        ("_bytes", ctypes.c_int32),
        ("_field_type", ctypes.c_uint8),
    ]

    @property
    def name(self):
        return self._name.decode("utf-8")

    @property
    def type(self):
        return self._type

    @property
    def precision(self):
        return self._precision

    @property
    def scale(self):
        return self._scale

    @property
    def length(self):
        """Alias to self.bytes."""
        return self._bytes

    @property
    def bytes(self):
        return self._bytes

    @property
    def field_type(self):
        return self._field_type

    def __dict__(self):
        """Construct dict."""
        return {
            "name": self.name,
            "type": self.type,
            "precision": self.precision,
            "scale": self.scale,
            "bytes": self.length,
            "field_type": self.field_type,
        }

    def __str__(self):
        """Construct str."""
        return "{name: %s, type: %d, precision: %d, scale: %d, bytes: %d field_type:%d}" % (
            self.name,
            self.type,
            self.precision,
            self.scale,
            self.length,
            self.field_type,
        )

    def __getitem__(self, item):
        """Get attr."""
        return getattr(self, item)


class TaosFieldAllCls:
    def __init__(self, name, type, precision, scale, bytes_, field_type):
        self.name = name
        self.type = type
        self.precision = precision
        self.scale = scale
        self.bytes = bytes_
        self.field_type = field_type

    def __repr__(self):
        return f'TaosFieldAllCls(name="{self.name}", type={self.type}, precision={self.precision}, scale={self.scale}, bytes={self.bytes}, field_type={self.field_type})'
