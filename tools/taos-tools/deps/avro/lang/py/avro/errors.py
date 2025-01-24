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

import json


def _safe_pretty(schema):
    """Try to pretty-print a schema, but never raise an exception within another exception."""
    try:
        return json.dumps(json.loads(str(schema)), indent=2)
    except Exception:  # Never raise an exception within another exception.
        return schema


class AvroException(Exception):
    """The base class for exceptions in avro."""


class SchemaParseException(AvroException):
    """Raised when a schema failed to parse."""


class InvalidName(SchemaParseException):
    """User attempted to parse a schema with an invalid name."""


class AvroWarning(UserWarning):
    """Base class for warnings."""


class IgnoredLogicalType(AvroWarning):
    """Warnings for unknown or invalid logical types."""


class AvroTypeException(AvroException):
    """Raised when datum is not an example of schema."""

    def __init__(self, *args):
        try:
            expected_schema, name, datum = args[:3]
        except (IndexError, ValueError):
            return super().__init__(*args)
        pretty_expected = json.dumps(json.loads(str(expected_schema)), indent=2)
        return super().__init__(f'The datum "{datum}" provided for "{name}" is not an example of the schema {pretty_expected}')


class InvalidDefaultException(AvroTypeException):
    """Raised when a default value isn't a suitable type for the schema."""


class AvroOutOfScaleException(AvroTypeException):
    """Raised when attempting to write a decimal datum with an exponent too large for the decimal schema."""

    def __init__(self, *args):
        try:
            scale, datum, exponent = args[:3]
        except (IndexError, ValueError):
            return super().__init__(*args)
        return super().__init__(f"The exponent of {datum}, {exponent}, is too large for the schema scale of {scale}")


class SchemaResolutionException(AvroException):
    def __init__(self, fail_msg, writers_schema=None, readers_schema=None, *args):
        writers_message = f"\nWriter's Schema: {_safe_pretty(writers_schema)}" if writers_schema else ""
        readers_message = f"\nReader's Schema: {_safe_pretty(readers_schema)}" if readers_schema else ""
        super().__init__((fail_msg or "") + writers_message + readers_message, *args)


class DataFileException(AvroException):
    """Raised when there's a problem reading or writing file object containers."""


class IONotReadyException(AvroException):
    """Raised when attempting an avro operation on an io object that isn't fully initialized."""


class AvroRemoteException(AvroException):
    """Raised when an error message is sent by an Avro requestor or responder."""


class ConnectionClosedException(AvroException):
    """Raised when attempting IPC on a closed connection."""


class ProtocolParseException(AvroException):
    """Raised when a protocol failed to parse."""


class UnsupportedCodec(NotImplementedError, AvroException):
    """Raised when the compression named cannot be used."""


class UsageError(RuntimeError, AvroException):
    """An exception raised when incorrect arguments were passed."""


class AvroRuntimeException(RuntimeError, AvroException):
    """Raised when compatibility parsing encounters an unknown type"""
