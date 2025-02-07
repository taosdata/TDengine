#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Input/Output utilities, including:

 * i/o-specific constants
 * i/o-specific exceptions
 * schema validation
 * leaf value encoding and decoding
 * datum reader/writer stuff (?)

Also includes a generic representation for data, which
uses the following mapping:

  * Schema records are implemented as dict.
  * Schema arrays are implemented as list.
  * Schema maps are implemented as dict.
  * Schema strings are implemented as str.
  * Schema bytes are implemented as bytes.
  * Schema ints are implemented as int.
  * Schema longs are implemented as int.
  * Schema floats are implemented as float.
  * Schema doubles are implemented as float.
  * Schema booleans are implemented as bool.

Validation:

The validation of schema is performed using breadth-first graph
traversal. This allows validation exceptions to pinpoint the exact node
within a complex schema that is problematic, simplifying debugging
considerably. Because it is a traversal, it will also be less
resource-intensive, particularly when validating schema with deep
structures.

Components
==========

Nodes
-----
Avro schemas contain many different schema types. Data about the schema
types is used to validate the data in the corresponding part of a Python
body (the object to be serialized). A node combines a given schema type
with the corresponding Python data, as well as an optional "name" to
identify the specific node. Names are generally the name of a schema
(for named schema) or the name of a field (for child nodes of schema
with named children like maps and records), or None, for schema who's
children are not named (like Arrays).

Iterators
---------
Iterators are generator functions that take a node and return a
generator which will yield a node for each child datum in the data for
the current node. If a node is of a type which has no children, then the
default iterator will immediately exit.

Validators
----------
Validators are used to determine if the datum for a given node is valid
according to the given schema type. Validator functions take a node as
an argument and return a node if the node datum passes validation. If it
does not, the validator must return None.

In most cases, the node returned is identical to the node provided (is
in fact the same object). However, in the case of Union schema, the
returned "valid" node will hold the schema that is represented by the
datum contained. This allows iteration over the child nodes
in that datum, if there are any.
"""

import collections
import datetime
import decimal
import struct
import warnings
from typing import (
    BinaryIO,
    Deque,
    Generator,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import avro.constants
import avro.errors
import avro.schema
import avro.timezones

# TODO(hammer): shouldn't ! be < for little-endian (according to spec?)
STRUCT_FLOAT = struct.Struct("<f")  # big-endian float
STRUCT_DOUBLE = struct.Struct("<d")  # big-endian double
STRUCT_SIGNED_SHORT = struct.Struct(">h")  # big-endian signed short
STRUCT_SIGNED_INT = struct.Struct(">i")  # big-endian signed int
STRUCT_SIGNED_LONG = struct.Struct(">q")  # big-endian signed long

ValidationNode = collections.namedtuple("ValidationNode", ["schema", "datum", "name"])
ValidationNodeGeneratorType = Generator[ValidationNode, None, None]
JsonScalarFieldType = Union[None, bool, str, int, float]


def validate(expected_schema: avro.schema.Schema, datum: object, raise_on_error: bool = False) -> bool:
    """Return True if the provided datum is valid for the expected schema

    If raise_on_error is passed and True, then raise a validation error
    with specific information about the error encountered in validation.

    :param expected_schema: An avro schema type object representing the schema against
                            which the datum will be validated.
    :param datum: The datum to be validated, A python dictionary or some supported type
    :param raise_on_error: True if a AvroTypeException should be raised immediately when a
                           validation problem is encountered.
    :raises: AvroTypeException if datum is invalid and raise_on_error is True
    :returns: True if datum is valid for expected_schema, False if not.
    """
    # use a FIFO queue to process schema nodes breadth first.
    nodes = collections.deque([ValidationNode(expected_schema, datum, getattr(expected_schema, "name", None))])

    while nodes:
        current_node = nodes.popleft()

        # _validate_node returns the node for iteration if it is valid. Or it returns None
        validated_schema = current_node.schema.validate(current_node.datum)
        valid_node = ValidationNode(validated_schema, current_node.datum, current_node.name) if validated_schema else None

        if valid_node is None:
            if raise_on_error:
                raise avro.errors.AvroTypeException(current_node.schema, current_node.name, current_node.datum)
            return False  # preserve the prior validation behavior of returning false when there are problems.
        # if there are children of this node to append, do so.
        for child_node in _iterate_node(valid_node):
            nodes.append(child_node)

    return True


def _iterate_node(node: ValidationNode) -> ValidationNodeGeneratorType:
    for item in _ITERATORS.get(node.schema.type, _default_iterator)(node):
        yield ValidationNode(*item)


#############
# Iteration #
#############


def _default_iterator(_) -> ValidationNodeGeneratorType:
    """Immediately raise StopIteration.

    This exists to prevent problems with iteration over unsupported container types.
    """
    yield from ()


def _record_iterator(node: ValidationNode) -> ValidationNodeGeneratorType:
    """Yield each child node of the provided record node."""
    schema, datum, _ = node
    return (ValidationNode(field.type, datum.get(field.name), field.name) for field in schema.fields)


def _array_iterator(node: ValidationNode) -> ValidationNodeGeneratorType:
    """Yield each child node of the provided array node."""
    schema, datum, name = node
    return (ValidationNode(schema.items, item, name) for item in datum)


def _map_iterator(node: ValidationNode) -> ValidationNodeGeneratorType:
    """Yield each child node of the provided map node."""
    schema, datum, _ = node
    child_schema = schema.values
    return (ValidationNode(child_schema, child_datum, child_name) for child_name, child_datum in datum.items())


_ITERATORS = {
    "record": _record_iterator,
    "array": _array_iterator,
    "map": _map_iterator,
}
_ITERATORS["error"] = _ITERATORS["request"] = _ITERATORS["record"]


#
# Decoder/Encoder
#


class BinaryDecoder:
    """Read leaf values."""

    _reader: BinaryIO

    def __init__(self, reader: BinaryIO) -> None:
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self._reader = reader

    @property
    def reader(self) -> BinaryIO:
        return self._reader

    def read(self, n: int) -> bytes:
        """
        Read n bytes.
        """
        return self.reader.read(n)

    def read_null(self) -> None:
        """
        null is written as zero bytes
        """
        return None

    def read_boolean(self) -> bool:
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        return ord(self.read(1)) == 1

    def read_int(self) -> int:
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        return self.read_long()

    def read_long(self) -> int:
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        b = ord(self.read(1))
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0:
            b = ord(self.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
        datum = (n >> 1) ^ -(n & 1)
        return datum

    def read_float(self) -> float:
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        return float(STRUCT_FLOAT.unpack(self.read(4))[0])

    def read_double(self) -> float:
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        return float(STRUCT_DOUBLE.unpack(self.read(8))[0])

    def read_decimal_from_bytes(self, precision: int, scale: int) -> decimal.Decimal:
        """
        Decimal bytes are decoded as signed short, int or long depending on the
        size of bytes.
        """
        size = self.read_long()
        return self.read_decimal_from_fixed(precision, scale, size)

    def read_decimal_from_fixed(self, precision: int, scale: int, size: int) -> decimal.Decimal:
        """
        Decimal is encoded as fixed. Fixed instances are encoded using the
        number of bytes declared in the schema.
        """
        datum = self.read(size)
        unscaled_datum = 0
        msb = struct.unpack("!b", datum[0:1])[0]
        leftmost_bit = (msb >> 7) & 1
        if leftmost_bit == 1:
            modified_first_byte = ord(datum[0:1]) ^ (1 << 7)
            datum = bytearray([modified_first_byte]) + datum[1:]
            for offset in range(size):
                unscaled_datum <<= 8
                unscaled_datum += ord(datum[offset : 1 + offset])
            unscaled_datum += pow(-2, (size * 8) - 1)
        else:
            for offset in range(size):
                unscaled_datum <<= 8
                unscaled_datum += ord(datum[offset : 1 + offset])

        original_prec = decimal.getcontext().prec
        try:
            decimal.getcontext().prec = precision
            scaled_datum = decimal.Decimal(unscaled_datum).scaleb(-scale)
        finally:
            decimal.getcontext().prec = original_prec
        return scaled_datum

    def read_bytes(self) -> bytes:
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        return self.read(self.read_long())

    def read_utf8(self) -> str:
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self.read_bytes().decode("utf-8")

    def read_date_from_int(self) -> datetime.date:
        """
        int is decoded as python date object.
        int stores the number of days from
        the unix epoch, 1 January 1970 (ISO calendar).
        """
        days_since_epoch = self.read_int()
        return datetime.date(1970, 1, 1) + datetime.timedelta(days_since_epoch)

    def _build_time_object(self, value: int, scale_to_micro: int) -> datetime.time:
        value = value * scale_to_micro
        value, microseconds = divmod(value, 1000000)
        value, seconds = divmod(value, 60)
        value, minutes = divmod(value, 60)
        hours = value

        return datetime.time(hour=hours, minute=minutes, second=seconds, microsecond=microseconds)

    def read_time_millis_from_int(self) -> datetime.time:
        """
        int is decoded as python time object which represents
        the number of milliseconds after midnight, 00:00:00.000.
        """
        milliseconds = self.read_int()
        return self._build_time_object(milliseconds, 1000)

    def read_time_micros_from_long(self) -> datetime.time:
        """
        long is decoded as python time object which represents
        the number of microseconds after midnight, 00:00:00.000000.
        """
        microseconds = self.read_long()
        return self._build_time_object(microseconds, 1)

    def read_timestamp_millis_from_long(self) -> datetime.datetime:
        """
        long is decoded as python datetime object which represents
        the number of milliseconds from the unix epoch, 1 January 1970.
        """
        timestamp_millis = self.read_long()
        timedelta = datetime.timedelta(microseconds=timestamp_millis * 1000)
        unix_epoch_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        return unix_epoch_datetime + timedelta

    def read_timestamp_micros_from_long(self) -> datetime.datetime:
        """
        long is decoded as python datetime object which represents
        the number of microseconds from the unix epoch, 1 January 1970.
        """
        timestamp_micros = self.read_long()
        timedelta = datetime.timedelta(microseconds=timestamp_micros)
        unix_epoch_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        return unix_epoch_datetime + timedelta

    def skip_null(self) -> None:
        pass

    def skip_boolean(self) -> None:
        self.skip(1)

    def skip_int(self) -> None:
        self.skip_long()

    def skip_long(self) -> None:
        b = ord(self.read(1))
        while (b & 0x80) != 0:
            b = ord(self.read(1))

    def skip_float(self) -> None:
        self.skip(4)

    def skip_double(self) -> None:
        self.skip(8)

    def skip_bytes(self) -> None:
        self.skip(self.read_long())

    def skip_utf8(self) -> None:
        self.skip_bytes()

    def skip(self, n: int) -> None:
        self.reader.seek(self.reader.tell() + n)


class BinaryEncoder:
    """Write leaf values."""

    _writer: BinaryIO

    def __init__(self, writer: BinaryIO) -> None:
        """
        writer is a Python object on which we can call write.
        """
        self._writer = writer

    @property
    def writer(self) -> BinaryIO:
        return self._writer

    def write(self, datum: bytes) -> None:
        """Write an arbitrary datum."""
        self.writer.write(datum)

    def write_null(self, datum: None) -> None:
        """
        null is written as zero bytes
        """
        pass

    def write_boolean(self, datum: bool) -> None:
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        self.write(bytearray([bool(datum)]))

    def write_int(self, datum: int) -> None:
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        self.write_long(datum)

    def write_long(self, datum: int) -> None:
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        datum = (datum << 1) ^ (datum >> 63)
        while (datum & ~0x7F) != 0:
            self.write(bytearray([(datum & 0x7F) | 0x80]))
            datum >>= 7
        self.write(bytearray([datum]))

    def write_float(self, datum: float) -> None:
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        self.write(STRUCT_FLOAT.pack(datum))

    def write_double(self, datum: float) -> None:
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        self.write(STRUCT_DOUBLE.pack(datum))

    def write_decimal_bytes(self, datum: decimal.Decimal, scale: int) -> None:
        """
        Decimal in bytes are encoded as long. Since size of packed value in bytes for
        signed long is 8, 8 bytes are written.
        """
        sign, digits, exp = datum.as_tuple()
        if (-1 * exp) > scale:
            raise avro.errors.AvroOutOfScaleException(scale, datum, exp)

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum

        bytes_req = bits_req // 8
        padding_bits = ~((1 << bits_req) - 1) if sign else 0
        packed_bits = padding_bits | unscaled_datum

        bytes_req += 1 if (bytes_req << 3) < bits_req else 0
        self.write_long(bytes_req)
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = packed_bits >> (8 * index)
            self.write(bytearray([bits_to_write & 0xFF]))

    def write_decimal_fixed(self, datum: decimal.Decimal, scale: int, size: int) -> None:
        """
        Decimal in fixed are encoded as size of fixed bytes.
        """
        sign, digits, exp = datum.as_tuple()
        if (-1 * exp) > scale:
            raise avro.errors.AvroOutOfScaleException(scale, datum, exp)

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        size_in_bits = size * 8
        offset_bits = size_in_bits - bits_req

        mask = 2 ** size_in_bits - 1
        bit = 1
        for i in range(bits_req):
            mask ^= bit
            bit <<= 1

        if bits_req < 8:
            bytes_req = 1
        else:
            bytes_req = bits_req // 8
            if bits_req % 8 != 0:
                bytes_req += 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum
            unscaled_datum = mask | unscaled_datum
            for index in range(size - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xFF]))
        else:
            for i in range(offset_bits // 8):
                self.write(b"\x00")
            for index in range(bytes_req - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xFF]))

    def write_bytes(self, datum: bytes) -> None:
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        self.write_long(len(datum))
        self.write(struct.pack(f"{len(datum)}s", datum))

    def write_utf8(self, datum: str) -> None:
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        self.write_bytes(datum.encode("utf-8"))

    def write_date_int(self, datum: datetime.date) -> None:
        """
        Encode python date object as int.
        It stores the number of days from
        the unix epoch, 1 January 1970 (ISO calendar).
        """
        delta_date = datum - datetime.date(1970, 1, 1)
        self.write_int(delta_date.days)

    def write_time_millis_int(self, datum: datetime.time) -> None:
        """
        Encode python time object as int.
        It stores the number of milliseconds from midnight, 00:00:00.000
        """
        milliseconds = datum.hour * 3600000 + datum.minute * 60000 + datum.second * 1000 + datum.microsecond // 1000
        self.write_int(milliseconds)

    def write_time_micros_long(self, datum: datetime.time) -> None:
        """
        Encode python time object as long.
        It stores the number of microseconds from midnight, 00:00:00.000000
        """
        microseconds = datum.hour * 3600000000 + datum.minute * 60000000 + datum.second * 1000000 + datum.microsecond
        self.write_long(microseconds)

    def _timedelta_total_microseconds(self, timedelta_: datetime.timedelta) -> int:
        return timedelta_.microseconds + (timedelta_.seconds + timedelta_.days * 24 * 3600) * 10 ** 6

    def write_timestamp_millis_long(self, datum: datetime.datetime) -> None:
        """
        Encode python datetime object as long.
        It stores the number of milliseconds from midnight of unix epoch, 1 January 1970.
        """
        datum = datum.astimezone(tz=avro.timezones.utc)
        timedelta = datum - datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        milliseconds = self._timedelta_total_microseconds(timedelta) // 1000
        self.write_long(milliseconds)

    def write_timestamp_micros_long(self, datum: datetime.datetime) -> None:
        """
        Encode python datetime object as long.
        It stores the number of microseconds from midnight of unix epoch, 1 January 1970.
        """
        datum = datum.astimezone(tz=avro.timezones.utc)
        timedelta = datum - datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        microseconds = self._timedelta_total_microseconds(timedelta)
        self.write_long(microseconds)


#
# DatumReader/Writer
#
class DatumReader:
    """Deserialize Avro-encoded data into a Python data structure."""

    _writers_schema: Optional[avro.schema.Schema]
    _readers_schema: Optional[avro.schema.Schema]

    def __init__(self, writers_schema: Optional[avro.schema.Schema] = None, readers_schema: Optional[avro.schema.Schema] = None) -> None:
        """
        As defined in the Avro specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self._writers_schema = writers_schema
        self._readers_schema = readers_schema

    @property
    def writers_schema(self) -> Optional[avro.schema.Schema]:
        return self._writers_schema

    @writers_schema.setter
    def writers_schema(self, writers_schema: avro.schema.Schema) -> None:
        self._writers_schema = writers_schema

    @property
    def readers_schema(self) -> Optional[avro.schema.Schema]:
        return self._readers_schema

    @readers_schema.setter
    def readers_schema(self, readers_schema: avro.schema.Schema) -> None:
        self._readers_schema = readers_schema

    def read(self, decoder: "BinaryDecoder") -> object:
        if self.writers_schema is None:
            raise avro.errors.IONotReadyException("Cannot read without a writer's schema.")
        if self.readers_schema is None:
            self.readers_schema = self.writers_schema
        return self.read_data(self.writers_schema, self.readers_schema, decoder)

    def read_data(self, writers_schema: avro.schema.Schema, readers_schema: avro.schema.Schema, decoder: "BinaryDecoder") -> object:
        # schema matching
        if not readers_schema.match(writers_schema):
            raise avro.errors.SchemaResolutionException("Schemas do not match.", writers_schema, readers_schema)

        logical_type = getattr(writers_schema, "logical_type", None)

        # function dispatch for reading data based on type of writer's schema
        if isinstance(writers_schema, avro.schema.UnionSchema) and isinstance(readers_schema, avro.schema.UnionSchema):
            return self.read_union(writers_schema, readers_schema, decoder)

        if isinstance(readers_schema, avro.schema.UnionSchema):
            # schema resolution: reader's schema is a union, writer's schema is not
            for s in readers_schema.schemas:
                if s.match(writers_schema):
                    return self.read_data(writers_schema, s, decoder)

            # This shouldn't happen because of the match check at the start of this method.
            raise avro.errors.SchemaResolutionException("Schemas do not match.", writers_schema, readers_schema)

        if writers_schema.type == "null":
            return None
        if writers_schema.type == "boolean":
            return decoder.read_boolean()
        if writers_schema.type == "string":
            return decoder.read_utf8()
        if writers_schema.type == "int":
            if logical_type == avro.constants.DATE:
                return decoder.read_date_from_int()
            if logical_type == avro.constants.TIME_MILLIS:
                return decoder.read_time_millis_from_int()
            return decoder.read_int()
        if writers_schema.type == "long":
            if logical_type == avro.constants.TIME_MICROS:
                return decoder.read_time_micros_from_long()
            if logical_type == avro.constants.TIMESTAMP_MILLIS:
                return decoder.read_timestamp_millis_from_long()
            if logical_type == avro.constants.TIMESTAMP_MICROS:
                return decoder.read_timestamp_micros_from_long()
            return decoder.read_long()
        if writers_schema.type == "float":
            return decoder.read_float()
        if writers_schema.type == "double":
            return decoder.read_double()
        if writers_schema.type == "bytes":
            if logical_type == "decimal":
                precision = writers_schema.get_prop("precision")
                if not (isinstance(precision, int) and precision > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal precision {precision}. Must be a positive integer."))
                    return decoder.read_bytes()
                scale = writers_schema.get_prop("scale")
                if not (isinstance(scale, int) and scale > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Must be a positive integer."))
                    return decoder.read_bytes()
                return decoder.read_decimal_from_bytes(precision, scale)
            return decoder.read_bytes()
        if isinstance(writers_schema, avro.schema.FixedSchema) and isinstance(readers_schema, avro.schema.FixedSchema):
            if logical_type == "decimal":
                precision = writers_schema.get_prop("precision")
                if not (isinstance(precision, int) and precision > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal precision {precision}. Must be a positive integer."))
                    return self.read_fixed(writers_schema, readers_schema, decoder)
                scale = writers_schema.get_prop("scale")
                if not (isinstance(scale, int) and scale > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Must be a positive integer."))
                    return self.read_fixed(writers_schema, readers_schema, decoder)
                return decoder.read_decimal_from_fixed(precision, scale, writers_schema.size)
            return self.read_fixed(writers_schema, readers_schema, decoder)
        if isinstance(writers_schema, avro.schema.EnumSchema) and isinstance(readers_schema, avro.schema.EnumSchema):
            return self.read_enum(writers_schema, readers_schema, decoder)
        if isinstance(writers_schema, avro.schema.ArraySchema) and isinstance(readers_schema, avro.schema.ArraySchema):
            return self.read_array(writers_schema, readers_schema, decoder)
        if isinstance(writers_schema, avro.schema.MapSchema) and isinstance(readers_schema, avro.schema.MapSchema):
            return self.read_map(writers_schema, readers_schema, decoder)
        if isinstance(writers_schema, avro.schema.RecordSchema) and isinstance(readers_schema, avro.schema.RecordSchema):
            # .type in ["record", "error", "request"]:
            return self.read_record(writers_schema, readers_schema, decoder)
        raise avro.errors.AvroException(f"Cannot read unknown schema type: {writers_schema.type}")

    def skip_data(self, writers_schema: avro.schema.Schema, decoder: BinaryDecoder) -> None:
        if writers_schema.type == "null":
            return decoder.skip_null()
        if writers_schema.type == "boolean":
            return decoder.skip_boolean()
        if writers_schema.type == "string":
            return decoder.skip_utf8()
        if writers_schema.type == "int":
            return decoder.skip_int()
        if writers_schema.type == "long":
            return decoder.skip_long()
        if writers_schema.type == "float":
            return decoder.skip_float()
        if writers_schema.type == "double":
            return decoder.skip_double()
        if writers_schema.type == "bytes":
            return decoder.skip_bytes()
        if isinstance(writers_schema, avro.schema.FixedSchema):
            return self.skip_fixed(writers_schema, decoder)
        if isinstance(writers_schema, avro.schema.EnumSchema):
            return self.skip_enum(writers_schema, decoder)
        if isinstance(writers_schema, avro.schema.ArraySchema):
            return self.skip_array(writers_schema, decoder)
        if isinstance(writers_schema, avro.schema.MapSchema):
            return self.skip_map(writers_schema, decoder)
        if isinstance(writers_schema, avro.schema.UnionSchema):
            return self.skip_union(writers_schema, decoder)
        if isinstance(writers_schema, avro.schema.RecordSchema):
            return self.skip_record(writers_schema, decoder)
        raise avro.errors.AvroException(f"Unknown schema type: {writers_schema.type}")

    def read_fixed(self, writers_schema: avro.schema.FixedSchema, readers_schema: avro.schema.Schema, decoder: BinaryDecoder) -> bytes:
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        return decoder.read(writers_schema.size)

    def skip_fixed(self, writers_schema: avro.schema.FixedSchema, decoder: BinaryDecoder) -> None:
        return decoder.skip(writers_schema.size)

    def read_enum(self, writers_schema: avro.schema.EnumSchema, readers_schema: avro.schema.EnumSchema, decoder: BinaryDecoder) -> str:
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        # read data
        index_of_symbol = decoder.read_int()
        if index_of_symbol >= len(writers_schema.symbols):
            raise avro.errors.SchemaResolutionException(
                f"Can't access enum index {index_of_symbol} for enum with {len(writers_schema.symbols)} symbols", writers_schema, readers_schema
            )
        read_symbol = writers_schema.symbols[index_of_symbol]

        # schema resolution
        if read_symbol not in readers_schema.symbols:
            raise avro.errors.SchemaResolutionException(f"Symbol {read_symbol} not present in Reader's Schema", writers_schema, readers_schema)

        return read_symbol

    def skip_enum(self, writers_schema: avro.schema.EnumSchema, decoder: BinaryDecoder) -> None:
        return decoder.skip_int()

    def read_array(self, writers_schema: avro.schema.ArraySchema, readers_schema: avro.schema.ArraySchema, decoder: BinaryDecoder) -> List[object]:
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = []
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                read_items.append(self.read_data(writers_schema.items, readers_schema.items, decoder))
            block_count = decoder.read_long()
        return read_items

    def skip_array(self, writers_schema: avro.schema.ArraySchema, decoder: BinaryDecoder) -> None:
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    self.skip_data(writers_schema.items, decoder)
            block_count = decoder.read_long()

    def read_map(self, writers_schema: avro.schema.MapSchema, readers_schema: avro.schema.MapSchema, decoder: BinaryDecoder) -> Mapping[str, object]:
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = {}
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                key = decoder.read_utf8()
                read_items[key] = self.read_data(writers_schema.values, readers_schema.values, decoder)
            block_count = decoder.read_long()
        return read_items

    def skip_map(self, writers_schema: avro.schema.MapSchema, decoder: BinaryDecoder) -> None:
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    decoder.skip_utf8()
                    self.skip_data(writers_schema.values, decoder)
            block_count = decoder.read_long()

    def read_union(self, writers_schema: avro.schema.UnionSchema, readers_schema: avro.schema.UnionSchema, decoder: BinaryDecoder) -> object:
        """
        A union is encoded by first writing an int value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # schema resolution
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            raise avro.errors.SchemaResolutionException(
                f"Can't access branch index {index_of_schema} for union with {len(writers_schema.schemas)} branches", writers_schema, readers_schema
            )
        selected_writers_schema = writers_schema.schemas[index_of_schema]

        # read data
        return self.read_data(selected_writers_schema, readers_schema, decoder)

    def skip_union(self, writers_schema: avro.schema.UnionSchema, decoder: BinaryDecoder) -> None:
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            raise avro.errors.SchemaResolutionException(
                f"Can't access branch index {index_of_schema} for union with {len(writers_schema.schemas)} branches", writers_schema
            )
        return self.skip_data(writers_schema.schemas[index_of_schema], decoder)

    def read_record(
        self, writers_schema: avro.schema.RecordSchema, readers_schema: avro.schema.RecordSchema, decoder: BinaryDecoder
    ) -> Mapping[str, object]:
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.

        Schema Resolution:
         * the ordering of fields may be different: fields are matched by name.
         * schemas for fields with the same name in both records are resolved
           recursively.
         * if the writer's record contains a field with a name not present in the
           reader's record, the writer's value for that field is ignored.
         * if the reader's record schema has a field that contains a default value,
           and writer's schema does not have a field with the same name, then the
           reader should use the default value from its field.
         * if the reader's record schema has a field with no default value, and
           writer's schema does not have a field with the same name, then the
           field's value is unset.
        """
        # schema resolution
        readers_fields_dict = readers_schema.fields_dict
        read_record = {}
        for field in writers_schema.fields:
            readers_field = readers_fields_dict.get(field.name)
            if readers_field is not None:
                field_val = self.read_data(field.type, readers_field.type, decoder)
                read_record[field.name] = field_val
            else:
                self.skip_data(field.type, decoder)

        # fill in default values
        if len(readers_fields_dict) > len(read_record):
            writers_fields_dict = writers_schema.fields_dict
            for field_name, field in readers_fields_dict.items():
                if field_name not in writers_fields_dict:
                    if not field.has_default:
                        raise avro.errors.SchemaResolutionException(f"No default value for field {field_name}", writers_schema, readers_schema)
                    field_val = self._read_default_value(field.type, field.default)
                    read_record[field.name] = field_val
        return read_record

    def skip_record(self, writers_schema: avro.schema.RecordSchema, decoder: BinaryDecoder) -> None:
        for field in writers_schema.fields:
            self.skip_data(field.type, decoder)

    def _read_default_value(self, field_schema: avro.schema.Schema, default_value: object) -> object:
        """
        Basically a JSON Decoder?
        """
        if field_schema.type == "null":
            if default_value is None:
                return None
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if field_schema.type == "boolean":
            return bool(default_value)
        if field_schema.type in ("int", "long"):
            if isinstance(default_value, int):
                return default_value
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if field_schema.type in ("float", "double"):
            if isinstance(default_value, float):
                return default_value
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if field_schema.type in ("bytes", "fixed"):
            if isinstance(default_value, bytes):
                return default_value
            if isinstance(default_value, str):
                return default_value.encode()
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if field_schema.type in ("enum", "string"):
            if isinstance(default_value, str):
                return default_value
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if isinstance(field_schema, avro.schema.ArraySchema):
            if isinstance(default_value, Iterable):
                return [self._read_default_value(field_schema.items, json_val) for json_val in default_value]
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if isinstance(field_schema, avro.schema.MapSchema):
            if isinstance(default_value, Mapping):
                return {key: self._read_default_value(field_schema.values, json_val) for key, json_val in default_value.items()}
            raise avro.errors.InvalidDefaultException(field_schema, default_value)
        if isinstance(field_schema, avro.schema.UnionSchema):
            return self._read_default_value(field_schema.schemas[0], default_value)
        if isinstance(field_schema, avro.schema.RecordSchema):
            if not isinstance(default_value, Mapping):
                raise avro.errors.InvalidDefaultException(field_schema, default_value)
            read_record = {}
            for field in field_schema.fields:
                json_val = default_value.get(field.name)
                if json_val is None:
                    json_val = field.default
                field_val = self._read_default_value(field.type, json_val)
                read_record[field.name] = field_val
            return read_record
        raise avro.errors.AvroException(f"Unknown type: {field_schema.type}")


class DatumWriter:
    """DatumWriter for generic python objects."""

    _writers_schema: Optional[avro.schema.Schema]

    def __init__(self, writers_schema: Optional[avro.schema.Schema] = None) -> None:
        self._writers_schema = writers_schema

    @property
    def writers_schema(self) -> Optional[avro.schema.Schema]:
        return self._writers_schema

    @writers_schema.setter
    def writers_schema(self, writers_schema: avro.schema.Schema) -> None:
        self._writers_schema = writers_schema

    def write(self, datum: object, encoder: BinaryEncoder) -> None:
        if self.writers_schema is None:
            raise avro.errors.IONotReadyException("Cannot write without a writer's schema.")
        validate(self.writers_schema, datum, raise_on_error=True)
        self.write_data(self.writers_schema, datum, encoder)

    def write_data(self, writers_schema: avro.schema.Schema, datum: object, encoder: BinaryEncoder) -> None:
        # function dispatch to write datum
        logical_type = getattr(writers_schema, "logical_type", None)
        if writers_schema.type == "null":
            if datum is None:
                return encoder.write_null(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "boolean":
            if isinstance(datum, bool):
                return encoder.write_boolean(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "string":
            if isinstance(datum, str):
                return encoder.write_utf8(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "int":
            if logical_type == avro.constants.DATE:
                if isinstance(datum, datetime.date):
                    return encoder.write_date_int(datum)
                warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a date type"))
            elif logical_type == avro.constants.TIME_MILLIS:
                if isinstance(datum, datetime.time):
                    return encoder.write_time_millis_int(datum)
                warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a time type"))
            if isinstance(datum, int):
                return encoder.write_int(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "long":
            if logical_type == avro.constants.TIME_MICROS:
                if isinstance(datum, datetime.time):
                    return encoder.write_time_micros_long(datum)
                warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a time type"))
            elif logical_type == avro.constants.TIMESTAMP_MILLIS:
                if isinstance(datum, datetime.datetime):
                    return encoder.write_timestamp_millis_long(datum)
                warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a datetime type"))
            elif logical_type == avro.constants.TIMESTAMP_MICROS:
                if isinstance(datum, datetime.datetime):
                    return encoder.write_timestamp_micros_long(datum)
                warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a datetime type"))
            if isinstance(datum, int):
                return encoder.write_long(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "float":
            if isinstance(datum, (int, float)):
                return encoder.write_float(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "double":
            if isinstance(datum, (int, float)):
                return encoder.write_double(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if writers_schema.type == "bytes":
            if logical_type == "decimal":
                scale = writers_schema.get_prop("scale")
                if not (isinstance(scale, int) and scale > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Must be a positive integer."))
                elif not isinstance(datum, decimal.Decimal):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a decimal type"))
                else:
                    return encoder.write_decimal_bytes(datum, scale)
            if isinstance(datum, bytes):
                return encoder.write_bytes(datum)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if isinstance(writers_schema, avro.schema.FixedSchema):
            if logical_type == "decimal":
                scale = writers_schema.get_prop("scale")
                size = writers_schema.size
                if not (isinstance(scale, int) and scale > 0):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"Invalid decimal scale {scale}. Must be a positive integer."))
                elif not isinstance(datum, decimal.Decimal):
                    warnings.warn(avro.errors.IgnoredLogicalType(f"{datum} is not a decimal type"))
                else:
                    return encoder.write_decimal_fixed(datum, scale, size)
            if isinstance(datum, bytes):
                return self.write_fixed(writers_schema, datum, encoder)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if isinstance(writers_schema, avro.schema.EnumSchema):
            if isinstance(datum, str):
                return self.write_enum(writers_schema, datum, encoder)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if isinstance(writers_schema, avro.schema.ArraySchema):
            if isinstance(datum, Sequence):
                return self.write_array(writers_schema, datum, encoder)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if isinstance(writers_schema, avro.schema.MapSchema):
            if isinstance(datum, Mapping):
                return self.write_map(writers_schema, datum, encoder)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        if isinstance(writers_schema, avro.schema.UnionSchema):
            return self.write_union(writers_schema, datum, encoder)
        if isinstance(writers_schema, avro.schema.RecordSchema):
            if isinstance(datum, Mapping):
                return self.write_record(writers_schema, datum, encoder)
            raise avro.errors.AvroTypeException(writers_schema, datum)
        raise avro.errors.AvroException(f"Unknown type: {writers_schema.type}")

    def write_fixed(self, writers_schema: avro.schema.FixedSchema, datum: bytes, encoder: BinaryEncoder) -> None:
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        return encoder.write(datum)

    def write_enum(self, writers_schema: avro.schema.EnumSchema, datum: str, encoder: BinaryEncoder) -> None:
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        index_of_datum = writers_schema.symbols.index(datum)
        return encoder.write_int(index_of_datum)

    def write_array(self, writers_schema: avro.schema.ArraySchema, datum: Sequence[object], encoder: BinaryEncoder) -> None:
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for item in datum:
                self.write_data(writers_schema.items, item, encoder)
        return encoder.write_long(0)

    def write_map(self, writers_schema: avro.schema.MapSchema, datum: Mapping[str, object], encoder: BinaryEncoder) -> None:
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for key, val in datum.items():
                encoder.write_utf8(key)
                self.write_data(writers_schema.values, val, encoder)
        return encoder.write_long(0)

    def write_union(self, writers_schema: avro.schema.UnionSchema, datum: object, encoder: BinaryEncoder) -> None:
        """
        A union is encoded by first writing an int value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # resolve union
        index_of_schema = -1
        for i, candidate_schema in enumerate(writers_schema.schemas):
            if validate(candidate_schema, datum):
                index_of_schema = i
        if index_of_schema < 0:
            raise avro.errors.AvroTypeException(writers_schema, datum)

        # write data
        encoder.write_long(index_of_schema)
        return self.write_data(writers_schema.schemas[index_of_schema], datum, encoder)

    def write_record(self, writers_schema: avro.schema.RecordSchema, datum: Mapping[str, object], encoder: BinaryEncoder) -> None:
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.
        """
        for field in writers_schema.fields:
            self.write_data(field.type, datum.get(field.name), encoder)
