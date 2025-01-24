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

import binascii
import datetime
import decimal
import io
import itertools
import json
import unittest
import warnings
from typing import BinaryIO, Collection, Dict, List, Optional, Tuple, Union, cast

import avro.io
import avro.schema
import avro.timezones
from avro.utils import TypedDict


class DefaultValueTestCaseType(TypedDict):
    H: object


SCHEMAS_TO_VALIDATE = tuple(
    (json.dumps(schema), datum)
    for schema, datum in (
        ("null", None),
        ("boolean", True),
        ("string", "adsfasdf09809dsf-=adsf"),
        ("bytes", b"12345abcd"),
        ("int", 1234),
        ("long", 1234),
        ("float", 1234.0),
        ("double", 1234.0),
        ({"type": "fixed", "name": "Test", "size": 1}, b"B"),
        (
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "Test",
                "size": 8,
                "precision": 5,
                "scale": 4,
            },
            decimal.Decimal("3.1415"),
        ),
        (
            {
                "type": "fixed",
                "logicalType": "decimal",
                "name": "Test",
                "size": 8,
                "precision": 5,
                "scale": 4,
            },
            decimal.Decimal("-3.1415"),
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
            decimal.Decimal("3.1415"),
        ),
        (
            {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 4},
            decimal.Decimal("-3.1415"),
        ),
        ({"type": "enum", "name": "Test", "symbols": ["A", "B"]}, "B"),
        ({"type": "array", "items": "long"}, [1, 3, 2]),
        ({"type": "map", "values": "long"}, {"a": 1, "b": 3, "c": 2}),
        (["string", "null", "long"], None),
        ({"type": "int", "logicalType": "date"}, datetime.date(2000, 1, 1)),
        (
            {"type": "int", "logicalType": "time-millis"},
            datetime.time(23, 59, 59, 999000),
        ),
        ({"type": "int", "logicalType": "time-millis"}, datetime.time(0, 0, 0, 000000)),
        (
            {"type": "long", "logicalType": "time-micros"},
            datetime.time(23, 59, 59, 999999),
        ),
        (
            {"type": "long", "logicalType": "time-micros"},
            datetime.time(0, 0, 0, 000000),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-millis"},
            datetime.datetime(2000, 1, 18, 2, 2, 1, 100000, tzinfo=avro.timezones.tst),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(1000, 1, 1, 0, 0, 0, 000000, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=avro.timezones.utc),
        ),
        (
            {"type": "long", "logicalType": "timestamp-micros"},
            datetime.datetime(2000, 1, 18, 2, 2, 1, 123499, tzinfo=avro.timezones.tst),
        ),
        (
            {"type": "string", "logicalType": "uuid"},
            "a4818e1c-8e59-11eb-8dcd-0242ac130003",
        ),  # UUID1
        (
            {"type": "string", "logicalType": "uuid"},
            "570feebe-2bbc-4937-98df-285944e1dbbd",
        ),  # UUID4
        ({"type": "string", "logicalType": "unknown-logical-type"}, "12345abcd"),
        ({"type": "string", "logicalType": "timestamp-millis"}, "12345abcd"),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            {"f": 5},
        ),
        (
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"},
                                ],
                            },
                        ],
                    }
                ],
            },
            {"value": {"car": {"value": "head"}, "cdr": {"value": None}}},
        ),
    )
)

BINARY_ENCODINGS = (
    (0, b"00"),
    (-1, b"01"),
    (1, b"02"),
    (-2, b"03"),
    (2, b"04"),
    (-64, b"7f"),
    (64, b"80 01"),
    (8192, b"80 80 01"),
    (-8193, b"81 80 01"),
)

DEFAULT_VALUE_EXAMPLES = (
    ("null", None),
    ("boolean", True),
    ("string", "foo"),
    ("bytes", "\xff\xff"),
    ("int", 5),
    ("long", 5),
    ("float", 1.1),
    ("double", 1.1),
    ({"type": "fixed", "name": "F", "size": 2}, "\xff\xff"),
    ({"type": "enum", "name": "F", "symbols": ["FOO", "BAR"]}, "FOO"),
    ({"type": "array", "items": "int"}, [1, 2, 3]),
    ({"type": "map", "values": "int"}, {"a": 1, "b": 2}),
    (["int", "null"], 5),
    (
        {"type": "record", "name": "F", "fields": [{"name": "A", "type": "int"}]},
        {"A": 5},
    ),
)

LONG_RECORD_SCHEMA = avro.schema.parse(
    json.dumps(
        {
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "A", "type": "int"},
                {"name": "B", "type": "int"},
                {"name": "C", "type": "int"},
                {"name": "D", "type": "int"},
                {"name": "E", "type": "int"},
                {"name": "F", "type": "int"},
                {"name": "G", "type": "int"},
            ],
        }
    )
)

LONG_RECORD_DATUM = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7}


def avro_hexlify(reader: BinaryIO) -> bytes:
    """Return the hex value, as a string, of a binary-encoded int or long."""
    b = []
    current_byte = reader.read(1)
    b.append(binascii.hexlify(current_byte))
    while (ord(current_byte) & 0x80) != 0:
        current_byte = reader.read(1)
        b.append(binascii.hexlify(current_byte))
    return b" ".join(b)


def write_datum(datum: object, writers_schema: avro.schema.Schema) -> Tuple[io.BytesIO, avro.io.BinaryEncoder, avro.io.DatumWriter]:
    writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(writer)
    datum_writer = avro.io.DatumWriter(writers_schema)
    datum_writer.write(datum, encoder)
    return writer, encoder, datum_writer


def read_datum(buffer: io.BytesIO, writers_schema: avro.schema.Schema, readers_schema: Optional[avro.schema.Schema] = None) -> object:
    reader = io.BytesIO(buffer.getvalue())
    decoder = avro.io.BinaryDecoder(reader)
    datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
    return datum_reader.read(decoder)


class IoValidateTestCase(unittest.TestCase):
    def __init__(self, test_schema: str, test_datum: object) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("io_valid")
        self.test_schema = avro.schema.parse(test_schema)
        self.test_datum = test_datum
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def io_valid(self) -> None:
        """
        In these cases, the provided data should be valid with the given schema.
        """
        with warnings.catch_warnings(record=True) as actual_warnings:
            self.assertTrue(
                avro.io.validate(self.test_schema, self.test_datum),
                f"{self.test_datum} did not validate in the schema {self.test_schema}",
            )


class RoundTripTestCase(unittest.TestCase):
    def __init__(self, test_schema: str, test_datum: object) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("io_round_trip")
        self.test_schema = avro.schema.parse(test_schema)
        self.test_datum = test_datum
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def io_round_trip(self) -> None:
        """
        A datum should be the same after being encoded and then decoded.
        """
        with warnings.catch_warnings(record=True) as actual_warnings:
            writer, encoder, datum_writer = write_datum(self.test_datum, self.test_schema)
            round_trip_datum = read_datum(writer, self.test_schema)
            expected: object
            round_trip: object
            if isinstance(round_trip_datum, decimal.Decimal):
                expected, round_trip, message = (
                    str(self.test_datum),
                    round_trip_datum.to_eng_string(),
                    "Decimal datum changed value after encode and decode",
                )
            elif isinstance(round_trip_datum, datetime.datetime):
                expected, round_trip, message = (
                    cast(datetime.datetime, self.test_datum).astimezone(tz=avro.timezones.utc),
                    round_trip_datum,
                    "DateTime datum changed value after encode and decode",
                )
            else:
                expected, round_trip, message = (
                    self.test_datum,
                    round_trip_datum,
                    "Datum changed value after encode and decode",
                )
            self.assertEqual(expected, round_trip, message)


class BinaryEncodingTestCase(unittest.TestCase):
    def __init__(self, skip: bool, test_type: str, test_datum: object, test_hex: bytes) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__(f"check_{'skip' if skip else 'binary'}_encoding")
        self.writers_schema = avro.schema.parse(f'"{test_type}"')
        self.test_datum = test_datum
        self.test_hex = test_hex
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_binary_encoding(self) -> None:
        with warnings.catch_warnings(record=True) as actual_warnings:
            writer, encoder, datum_writer = write_datum(self.test_datum, self.writers_schema)
            writer.seek(0)
            hex_val = avro_hexlify(writer)
            self.assertEqual(
                self.test_hex,
                hex_val,
                "Binary encoding did not match expected hex representation.",
            )

    def check_skip_encoding(self) -> None:
        VALUE_TO_READ = 6253
        with warnings.catch_warnings(record=True) as actual_warnings:
            # write the value to skip and a known value
            writer, encoder, datum_writer = write_datum(self.test_datum, self.writers_schema)
            datum_writer.write(VALUE_TO_READ, encoder)

            # skip the value
            reader = io.BytesIO(writer.getvalue())
            decoder = avro.io.BinaryDecoder(reader)
            decoder.skip_long()

            # read data from string buffer
            datum_reader = avro.io.DatumReader(self.writers_schema)
            read_value = datum_reader.read(decoder)

            self.assertEqual(
                read_value,
                VALUE_TO_READ,
                "Unexpected value after skipping a binary encoded value.",
            )


class SchemaPromotionTestCase(unittest.TestCase):
    def __init__(self, write_type: str, read_type: str) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_schema_promotion")
        self.writers_schema = avro.schema.parse(f'"{write_type}"')
        self.readers_schema = avro.schema.parse(f'"{read_type}"')
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_schema_promotion(self) -> None:
        """Test schema promotion"""
        # note that checking writers_schema.type in read_data
        # allows us to handle promotion correctly
        DATUM_TO_WRITE = 219
        with warnings.catch_warnings(record=True) as actual_warnings:
            writer, enc, dw = write_datum(DATUM_TO_WRITE, self.writers_schema)
            datum_read = read_datum(writer, self.writers_schema, self.readers_schema)
            self.assertEqual(
                datum_read,
                DATUM_TO_WRITE,
                f"Datum changed between schema that were supposed to promote: writer: {self.writers_schema} reader: {self.readers_schema}.",
            )


class DefaultValueTestCase(unittest.TestCase):
    def __init__(self, field_type: Collection[str], default: Union[Dict[str, int], List[int], None, float, str]) -> None:
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_default_value")
        self.field_type = field_type
        self.default = default
        # Never hide repeated warnings when running this test case.
        warnings.simplefilter("always")

    def check_default_value(self) -> None:
        datum_read: DefaultValueTestCaseType
        with warnings.catch_warnings(record=True) as actual_warnings:
            datum_to_read = cast(DefaultValueTestCaseType, {"H": self.default})
            readers_schema = avro.schema.parse(
                json.dumps(
                    {
                        "type": "record",
                        "name": "Test",
                        "fields": [
                            {
                                "name": "H",
                                "type": self.field_type,
                                "default": self.default,
                            }
                        ],
                    }
                )
            )
            writer, _, _ = write_datum(LONG_RECORD_DATUM, LONG_RECORD_SCHEMA)
            datum_read_ = cast(DefaultValueTestCaseType, read_datum(writer, LONG_RECORD_SCHEMA, readers_schema))
            datum_read = {"H": cast(bytes, datum_read_["H"]).decode()} if isinstance(datum_read_["H"], bytes) else datum_read_
            self.assertEqual(datum_to_read, datum_read)


class TestMisc(unittest.TestCase):
    def test_decimal_bytes_small_scale(self) -> None:
        """Avro should raise an AvroTypeException when attempting to write a decimal with a larger exponent than the schema's scale."""
        datum = decimal.Decimal("3.1415")
        _, _, exp = datum.as_tuple()
        scale = -1 * exp - 1
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": 5,
                    "scale": scale,
                }
            )
        )
        self.assertRaises(avro.errors.AvroOutOfScaleException, write_datum, datum, schema)

    def test_decimal_fixed_small_scale(self) -> None:
        """Avro should raise an AvroTypeException when attempting to write a decimal with a larger exponent than the schema's scale."""
        datum = decimal.Decimal("3.1415")
        _, _, exp = datum.as_tuple()
        scale = -1 * exp - 1
        schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "fixed",
                    "logicalType": "decimal",
                    "name": "Test",
                    "size": 8,
                    "precision": 5,
                    "scale": scale,
                }
            )
        )
        self.assertRaises(avro.errors.AvroOutOfScaleException, write_datum, datum, schema)

    def test_unknown_symbol(self) -> None:
        datum_to_write = "FOO"
        writers_schema = avro.schema.parse(json.dumps({"type": "enum", "name": "Test", "symbols": ["FOO", "BAR"]}))
        readers_schema = avro.schema.parse(json.dumps({"type": "enum", "name": "Test", "symbols": ["BAR", "BAZ"]}))

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_no_default_value(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [{"name": "H", "type": "int"}],
                }
            )
        )

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        reader = io.BytesIO(writer.getvalue())
        decoder = avro.io.BinaryDecoder(reader)
        datum_reader = avro.io.DatumReader(writers_schema, readers_schema)
        self.assertRaises(avro.errors.SchemaResolutionException, datum_reader.read, decoder)

    def test_projection(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "E", "type": "int"},
                        {"name": "F", "type": "int"},
                    ],
                }
            )
        )
        datum_to_read = {"E": 5, "F": 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        self.assertEqual(datum_to_read, datum_read)

    def test_field_order(self) -> None:
        writers_schema = LONG_RECORD_SCHEMA
        datum_to_write = LONG_RECORD_DATUM

        readers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "F", "type": "int"},
                        {"name": "E", "type": "int"},
                    ],
                }
            )
        )
        datum_to_read = {"E": 5, "F": 6}

        writer, encoder, datum_writer = write_datum(datum_to_write, writers_schema)
        datum_read = read_datum(writer, writers_schema, readers_schema)
        self.assertEqual(datum_to_read, datum_read)

    def test_type_exception_int(self) -> None:
        writers_schema = avro.schema.parse(
            json.dumps(
                {
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "F", "type": "int"},
                        {"name": "E", "type": "int"},
                    ],
                }
            )
        )
        datum_to_write = {"E": 5, "F": "Bad"}
        with self.assertRaises(avro.errors.AvroTypeException) as exc:
            write_datum(datum_to_write, writers_schema)
        assert str(exc.exception) == 'The datum "Bad" provided for "F" is not an example of the schema "int"'

    def test_type_exception_long(self) -> None:
        writers_schema = avro.schema.parse(json.dumps({"type": "record", "name": "Test", "fields": [{"name": "foo", "type": "long"}]}))
        datum_to_write = {"foo": 5.0}

        with self.assertRaises(avro.errors.AvroTypeException) as exc:
            write_datum(datum_to_write, writers_schema)
        assert str(exc.exception) == 'The datum "5.0" provided for "foo" is not an example of the schema "long"'

    def test_type_exception_record(self) -> None:
        writers_schema = avro.schema.parse(json.dumps({"type": "record", "name": "Test", "fields": [{"name": "foo", "type": "long"}]}))
        datum_to_write = ("foo", 5.0)

        with self.assertRaisesRegex(avro.errors.AvroTypeException, r"The datum \".*\" provided for \".*\" is not an example of the schema [\s\S]*"):
            write_datum(datum_to_write, writers_schema)


def load_tests(loader: unittest.TestLoader, default_tests: None, pattern: None) -> unittest.TestSuite:
    """Generate test cases across many test schema."""
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestMisc))
    suite.addTests(IoValidateTestCase(schema_str, datum) for schema_str, datum in SCHEMAS_TO_VALIDATE)
    suite.addTests(RoundTripTestCase(schema_str, datum) for schema_str, datum in SCHEMAS_TO_VALIDATE)
    for skip in False, True:
        for type_ in "int", "long":
            suite.addTests(BinaryEncodingTestCase(skip, type_, datum, hex_) for datum, hex_ in BINARY_ENCODINGS)
    suite.addTests(
        SchemaPromotionTestCase(write_type, read_type) for write_type, read_type in itertools.combinations(("int", "long", "float", "double"), 2)
    )
    suite.addTests(DefaultValueTestCase(field_type, default) for field_type, default in DEFAULT_VALUE_EXAMPLES)
    return suite


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
