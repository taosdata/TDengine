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
from copy import copy
from enum import Enum
from typing import Container, Iterable, List, Optional, Set, cast

from avro.errors import AvroRuntimeException
from avro.schema import (
    ArraySchema,
    EnumSchema,
    Field,
    FixedSchema,
    MapSchema,
    NamedSchema,
    RecordSchema,
    Schema,
    UnionSchema,
)


class SchemaType(str, Enum):
    ARRAY = "array"
    BOOLEAN = "boolean"
    BYTES = "bytes"
    DOUBLE = "double"
    ENUM = "enum"
    FIXED = "fixed"
    FLOAT = "float"
    INT = "int"
    LONG = "long"
    MAP = "map"
    NULL = "null"
    RECORD = "record"
    STRING = "string"
    UNION = "union"


class SchemaCompatibilityType(Enum):
    compatible = "compatible"
    incompatible = "incompatible"
    recursion_in_progress = "recursion_in_progress"


class SchemaIncompatibilityType(Enum):
    name_mismatch = "name_mismatch"
    fixed_size_mismatch = "fixed_size_mismatch"
    missing_enum_symbols = "missing_enum_symbols"
    reader_field_missing_default_value = "reader_field_missing_default_value"
    type_mismatch = "type_mismatch"
    missing_union_branch = "missing_union_branch"


PRIMITIVE_TYPES = {
    SchemaType.NULL,
    SchemaType.BOOLEAN,
    SchemaType.INT,
    SchemaType.LONG,
    SchemaType.FLOAT,
    SchemaType.DOUBLE,
    SchemaType.BYTES,
    SchemaType.STRING,
}


class SchemaCompatibilityResult:
    def __init__(
        self,
        compatibility: SchemaCompatibilityType = SchemaCompatibilityType.recursion_in_progress,
        incompatibilities: List[SchemaIncompatibilityType] = None,
        messages: Optional[Set[str]] = None,
        locations: Optional[Set[str]] = None,
    ):
        self.locations = locations or {"/"}
        self.messages = messages or set()
        self.compatibility = compatibility
        self.incompatibilities = incompatibilities or []


def merge(this: SchemaCompatibilityResult, that: SchemaCompatibilityResult) -> SchemaCompatibilityResult:
    """
    Merges two {@code SchemaCompatibilityResult} into a new instance, combining the list of Incompatibilities
    and regressing to the SchemaCompatibilityType.incompatible state if any incompatibilities are encountered.
    :param this: SchemaCompatibilityResult
    :param that: SchemaCompatibilityResult
    :return: SchemaCompatibilityResult
    """
    that = cast(SchemaCompatibilityResult, that)
    merged = [*copy(this.incompatibilities), *copy(that.incompatibilities)]
    if this.compatibility is SchemaCompatibilityType.compatible:
        compat = that.compatibility
        messages = that.messages
        locations = that.locations
    else:
        compat = this.compatibility
        messages = this.messages.union(that.messages)
        locations = this.locations.union(that.locations)
    return SchemaCompatibilityResult(
        compatibility=compat,
        incompatibilities=merged,
        messages=messages,
        locations=locations,
    )


CompatibleResult = SchemaCompatibilityResult(SchemaCompatibilityType.compatible)


class ReaderWriter:
    def __init__(self, reader: Schema, writer: Schema) -> None:
        self.reader, self.writer = reader, writer

    def __hash__(self) -> int:
        return id(self.reader) ^ id(self.writer)

    def __eq__(self, other) -> bool:
        if not isinstance(other, ReaderWriter):
            return False
        return self.reader is other.reader and self.writer is other.writer


class ReaderWriterCompatibilityChecker:
    ROOT_REFERENCE_TOKEN = "/"

    def __init__(self):
        self.memoize_map = {}

    def get_compatibility(
        self,
        reader: Schema,
        writer: Schema,
        reference_token: str = ROOT_REFERENCE_TOKEN,
        location: Optional[List[str]] = None,
    ) -> SchemaCompatibilityResult:
        if location is None:
            location = []
        pair = ReaderWriter(reader, writer)
        if pair in self.memoize_map:
            result = cast(SchemaCompatibilityResult, self.memoize_map[pair])
            if result.compatibility is SchemaCompatibilityType.recursion_in_progress:
                result = CompatibleResult
        else:
            self.memoize_map[pair] = SchemaCompatibilityResult()
            result = self.calculate_compatibility(reader, writer, location + [reference_token])
            self.memoize_map[pair] = result
        return result

    # pylSchemaType.INT: disable=too-many-return-statements
    def calculate_compatibility(
        self,
        reader: Schema,
        writer: Schema,
        location: List[str],
    ) -> SchemaCompatibilityResult:
        """
        Calculates the compatibility of a reader/writer schema pair. Will be positive if the reader is capable of reading
        whatever the writer may write
        :param reader: avro.schema.Schema
        :param writer: avro.schema.Schema
        :param location: List[str]
        :return: SchemaCompatibilityResult
        """
        assert reader is not None
        assert writer is not None
        result = CompatibleResult
        if reader.type == writer.type:
            if reader.type in PRIMITIVE_TYPES:
                return result
            if reader.type == SchemaType.ARRAY:
                reader, writer = cast(ArraySchema, reader), cast(ArraySchema, writer)
                return merge(
                    result,
                    self.get_compatibility(reader.items, writer.items, "items", location),
                )
            if reader.type == SchemaType.MAP:
                reader, writer = cast(MapSchema, reader), cast(MapSchema, writer)
                return merge(
                    result,
                    self.get_compatibility(reader.values, writer.values, "values", location),
                )
            if reader.type == SchemaType.FIXED:
                reader, writer = cast(FixedSchema, reader), cast(FixedSchema, writer)
                result = merge(result, check_schema_names(reader, writer, location))
                return merge(result, check_fixed_size(reader, writer, location))
            if reader.type == SchemaType.ENUM:
                reader, writer = cast(EnumSchema, reader), cast(EnumSchema, writer)
                result = merge(result, check_schema_names(reader, writer, location))
                return merge(
                    result,
                    check_reader_enum_contains_writer_enum(reader, writer, location),
                )
            if reader.type == SchemaType.RECORD:
                reader, writer = cast(RecordSchema, reader), cast(RecordSchema, writer)
                result = merge(result, check_schema_names(reader, writer, location))
                return merge(
                    result,
                    self.check_reader_writer_record_fields(reader, writer, location),
                )
            if reader.type == SchemaType.UNION:
                reader, writer = cast(UnionSchema, reader), cast(UnionSchema, writer)
                for i, writer_branch in enumerate(writer.schemas):
                    compat = self.get_compatibility(reader, writer_branch)
                    if compat.compatibility is SchemaCompatibilityType.incompatible:
                        result = merge(
                            result,
                            incompatible(
                                SchemaIncompatibilityType.missing_union_branch,
                                f"reader union lacking writer type: {writer_branch.type.upper()}",
                                location + [str(i)],
                            ),
                        )
                return result
            raise AvroRuntimeException(f"Unknown schema type: {reader.type}")
        if writer.type == SchemaType.UNION:
            writer = cast(UnionSchema, writer)
            for s in writer.schemas:
                result = merge(result, self.get_compatibility(reader, s))
            return result
        if reader.type in {SchemaType.NULL, SchemaType.BOOLEAN, SchemaType.INT}:
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.LONG:
            if writer.type == SchemaType.INT:
                return result
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.FLOAT:
            if writer.type in {SchemaType.INT, SchemaType.LONG}:
                return result
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.DOUBLE:
            if writer.type in {SchemaType.INT, SchemaType.LONG, SchemaType.FLOAT}:
                return result
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.BYTES:
            if writer.type == SchemaType.STRING:
                return result
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.STRING:
            if writer.type == SchemaType.BYTES:
                return result
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type in {
            SchemaType.ARRAY,
            SchemaType.MAP,
            SchemaType.FIXED,
            SchemaType.ENUM,
            SchemaType.RECORD,
        }:
            return merge(result, type_mismatch(reader, writer, location))
        if reader.type == SchemaType.UNION:
            reader = cast(UnionSchema, reader)
            for reader_branch in reader.schemas:
                compat = self.get_compatibility(reader_branch, writer)
                if compat.compatibility is SchemaCompatibilityType.compatible:
                    return result
            # No branch in reader compatible with writer
            message = f"reader union lacking writer type {writer.type}"
            return merge(
                result,
                incompatible(SchemaIncompatibilityType.missing_union_branch, message, location),
            )
        raise AvroRuntimeException(f"Unknown schema type: {reader.type}")

    # pylSchemaType.INT: enable=too-many-return-statements

    def check_reader_writer_record_fields(self, reader: RecordSchema, writer: RecordSchema, location: List[str]) -> SchemaCompatibilityResult:
        result = CompatibleResult
        for i, reader_field in enumerate(reader.fields):
            reader_field = cast(Field, reader_field)
            writer_field = lookup_writer_field(writer_schema=writer, reader_field=reader_field)
            if writer_field is None:
                if not reader_field.has_default:
                    if reader_field.type.type == SchemaType.ENUM and reader_field.type.props.get("default"):
                        result = merge(
                            result,
                            self.get_compatibility(
                                reader_field.type,
                                writer,
                                "type",
                                location + ["fields", str(i)],
                            ),
                        )
                    else:
                        result = merge(
                            result,
                            incompatible(
                                SchemaIncompatibilityType.reader_field_missing_default_value,
                                reader_field.name,
                                location + ["fields", str(i)],
                            ),
                        )
            else:
                result = merge(
                    result,
                    self.get_compatibility(
                        reader_field.type,
                        writer_field.type,
                        "type",
                        location + ["fields", str(i)],
                    ),
                )
        return result


def type_mismatch(reader: Schema, writer: Schema, location: List[str]) -> SchemaCompatibilityResult:
    message = f"reader type: {reader.type} not compatible with writer type: {writer.type}"
    return incompatible(SchemaIncompatibilityType.type_mismatch, message, location)


def check_schema_names(reader: NamedSchema, writer: NamedSchema, location: List[str]) -> SchemaCompatibilityResult:
    result = CompatibleResult
    if not schema_name_equals(reader, writer):
        message = f"expected: {writer.fullname}"
        result = incompatible(SchemaIncompatibilityType.name_mismatch, message, location + ["name"])
    return result


def check_fixed_size(reader: FixedSchema, writer: FixedSchema, location: List[str]) -> SchemaCompatibilityResult:
    result = CompatibleResult
    actual = reader.size
    expected = writer.size
    if actual != expected:
        message = f"expected: {expected}, found: {actual}"
        result = incompatible(
            SchemaIncompatibilityType.fixed_size_mismatch,
            message,
            location + ["size"],
        )
    return result


def check_reader_enum_contains_writer_enum(reader: EnumSchema, writer: EnumSchema, location: List[str]) -> SchemaCompatibilityResult:
    result = CompatibleResult
    writer_symbols, reader_symbols = set(writer.symbols), set(reader.symbols)
    extra_symbols = writer_symbols.difference(reader_symbols)
    if extra_symbols:
        default = reader.props.get("default")
        if default and default in reader_symbols:
            result = CompatibleResult
        else:
            result = incompatible(
                SchemaIncompatibilityType.missing_enum_symbols,
                str(extra_symbols),
                location + ["symbols"],
            )
    return result


def incompatible(incompat_type: SchemaIncompatibilityType, message: str, location: List[str]) -> SchemaCompatibilityResult:
    locations = "/".join(location)
    if len(location) > 1:
        locations = locations[1:]
    ret = SchemaCompatibilityResult(
        compatibility=SchemaCompatibilityType.incompatible,
        incompatibilities=[incompat_type],
        locations={locations},
        messages={message},
    )
    return ret


def schema_name_equals(reader: NamedSchema, writer: NamedSchema) -> bool:
    aliases = reader.props.get("aliases")
    return (reader.name == writer.name) or (isinstance(aliases, Container) and writer.fullname in aliases)


def lookup_writer_field(writer_schema: RecordSchema, reader_field: Field) -> Optional[Field]:
    direct = writer_schema.fields_dict.get(reader_field.name)
    if direct:
        return cast(Field, direct)
    aliases = reader_field.props.get("aliases")
    if not isinstance(aliases, Iterable):
        return None
    for alias in aliases:
        writer_field = writer_schema.fields_dict.get(alias)
        if writer_field is not None:
            return cast(Field, writer_field)
    return None
