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
import unittest

from avro.compatibility import (
    ReaderWriterCompatibilityChecker,
    SchemaCompatibilityType,
    SchemaType,
)
from avro.schema import (
    ArraySchema,
    MapSchema,
    Names,
    PrimitiveSchema,
    Schema,
    UnionSchema,
    parse,
)

BOOLEAN_SCHEMA = PrimitiveSchema(SchemaType.BOOLEAN)
NULL_SCHEMA = PrimitiveSchema(SchemaType.NULL)
INT_SCHEMA = PrimitiveSchema(SchemaType.INT)
LONG_SCHEMA = PrimitiveSchema(SchemaType.LONG)
STRING_SCHEMA = PrimitiveSchema(SchemaType.STRING)
BYTES_SCHEMA = PrimitiveSchema(SchemaType.BYTES)
FLOAT_SCHEMA = PrimitiveSchema(SchemaType.FLOAT)
DOUBLE_SCHEMA = PrimitiveSchema(SchemaType.DOUBLE)
INT_ARRAY_SCHEMA = ArraySchema(SchemaType.INT, names=Names())
LONG_ARRAY_SCHEMA = ArraySchema(SchemaType.LONG, names=Names())
STRING_ARRAY_SCHEMA = ArraySchema(SchemaType.STRING, names=Names())
INT_MAP_SCHEMA = MapSchema(SchemaType.INT, names=Names())
LONG_MAP_SCHEMA = MapSchema(SchemaType.LONG, names=Names())
STRING_MAP_SCHEMA = MapSchema(SchemaType.STRING, names=Names())
ENUM1_AB_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["A", "B"]}))
ENUM1_ABC_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["A", "B", "C"]}))
ENUM1_BC_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum1", "symbols": ["B", "C"]}))
ENUM2_AB_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum2", "symbols": ["A", "B"]}))
ENUM_ABC_ENUM_DEFAULT_A_SCHEMA = parse(json.dumps({"type": "enum", "name": "Enum", "symbols": ["A", "B", "C"], "default": "A"}))
ENUM_AB_ENUM_DEFAULT_A_SCHEMA = parse(json.dumps({"type": SchemaType.ENUM, "name": "Enum", "symbols": ["A", "B"], "default": "A"}))
ENUM_ABC_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum",
                        "symbols": ["A", "B", "C"],
                        "default": "A",
                    },
                }
            ],
        }
    )
)
ENUM_AB_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum",
                        "symbols": ["A", "B"],
                        "default": "A",
                    },
                }
            ],
        }
    )
)
ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum",
                        "symbols": ["A", "B", "C"],
                        "default": "A",
                    },
                    "default": "B",
                }
            ],
        }
    )
)
ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record",
            "fields": [
                {
                    "name": "Field",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum",
                        "symbols": ["A", "B"],
                        "default": "B",
                    },
                    "default": "A",
                }
            ],
        }
    )
)
EMPTY_UNION_SCHEMA = UnionSchema([], names=Names())
NULL_UNION_SCHEMA = UnionSchema([SchemaType.NULL], names=Names())
INT_UNION_SCHEMA = UnionSchema([SchemaType.INT], names=Names())
LONG_UNION_SCHEMA = UnionSchema([SchemaType.LONG], names=Names())
FLOAT_UNION_SCHEMA = UnionSchema([SchemaType.FLOAT], names=Names())
DOUBLE_UNION_SCHEMA = UnionSchema([SchemaType.DOUBLE], names=Names())
STRING_UNION_SCHEMA = UnionSchema([SchemaType.STRING], names=Names())
BYTES_UNION_SCHEMA = UnionSchema([SchemaType.BYTES], names=Names())
INT_STRING_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.STRING], names=Names())
STRING_INT_UNION_SCHEMA = UnionSchema([SchemaType.STRING, SchemaType.INT], names=Names())
INT_FLOAT_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.FLOAT], names=Names())
INT_LONG_UNION_SCHEMA = UnionSchema([SchemaType.INT, SchemaType.LONG], names=Names())
INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA = UnionSchema(
    [SchemaType.INT, SchemaType.LONG, SchemaType.FLOAT, SchemaType.DOUBLE],
    names=Names(),
)
NULL_INT_ARRAY_UNION_SCHEMA = UnionSchema(
    [{"type": SchemaType.NULL}, {"type": SchemaType.ARRAY, "items": SchemaType.INT}],
    names=Names(),
)
NULL_INT_MAP_UNION_SCHEMA = UnionSchema(
    [{"type": SchemaType.NULL}, {"type": SchemaType.MAP, "values": SchemaType.INT}],
    names=Names(),
)
EMPTY_RECORD1 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record1", "fields": []}))
EMPTY_RECORD2 = parse(json.dumps({"type": SchemaType.RECORD, "name": "Record2", "fields": []}))
A_INT_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "a", "type": SchemaType.INT}],
        }
    )
)
A_LONG_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "a", "type": SchemaType.LONG}],
        }
    )
)
A_INT_B_INT_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT},
                {"name": "b", "type": SchemaType.INT},
            ],
        }
    )
)
A_DINT_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "a", "type": SchemaType.INT, "default": 0}],
        }
    )
)
A_INT_B_DINT_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT},
                {"name": "b", "type": SchemaType.INT, "default": 0},
            ],
        }
    )
)
A_DINT_B_DINT_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {"name": "b", "type": SchemaType.INT, "default": 0},
            ],
        }
    )
)
A_DINT_B_DFIXED_4_BYTES_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {
                    "name": "b",
                    "type": {"type": SchemaType.FIXED, "name": "Fixed", "size": 4},
                },
            ],
        }
    )
)
A_DINT_B_DFIXED_8_BYTES_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {
                    "name": "b",
                    "type": {"type": SchemaType.FIXED, "name": "Fixed", "size": 8},
                },
            ],
        }
    )
)
A_DINT_B_DINT_STRING_UNION_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {
                    "name": "b",
                    "type": [SchemaType.INT, SchemaType.STRING],
                    "default": 0,
                },
            ],
        }
    )
)
A_DINT_B_DINT_UNION_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {"name": "b", "type": [SchemaType.INT], "default": 0},
            ],
        }
    )
)
A_DINT_B_DENUM_1_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {
                    "name": "b",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum1",
                        "symbols": ["A", "B"],
                    },
                },
            ],
        }
    )
)
A_DINT_B_DENUM_2_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {"name": "a", "type": SchemaType.INT, "default": 0},
                {
                    "name": "b",
                    "type": {
                        "type": SchemaType.ENUM,
                        "name": "Enum2",
                        "symbols": ["A", "B"],
                    },
                },
            ],
        }
    )
)
FIXED_4_BYTES = parse(json.dumps({"type": SchemaType.FIXED, "name": "Fixed", "size": 4}))
FIXED_8_BYTES = parse(json.dumps({"type": SchemaType.FIXED, "name": "Fixed", "size": 8}))
NS_RECORD1 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {
                    "name": "f1",
                    "type": [
                        SchemaType.NULL,
                        {
                            "type": SchemaType.ARRAY,
                            "items": {
                                "type": SchemaType.RECORD,
                                "name": "InnerRecord1",
                                "namespace": "ns1",
                                "fields": [{"name": "a", "type": SchemaType.INT}],
                            },
                        },
                    ],
                }
            ],
        }
    )
)
NS_RECORD2 = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [
                {
                    "name": "f1",
                    "type": [
                        SchemaType.NULL,
                        {
                            "type": SchemaType.ARRAY,
                            "items": {
                                "type": SchemaType.RECORD,
                                "name": "InnerRecord1",
                                "namespace": "ns2",
                                "fields": [{"name": "a", "type": SchemaType.INT}],
                            },
                        },
                    ],
                }
            ],
        }
    )
)

UNION_INT_RECORD1 = UnionSchema(
    [
        {"type": SchemaType.INT},
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "field1", "type": SchemaType.INT}],
        },
    ]
)
UNION_INT_RECORD2 = UnionSchema(
    [
        {"type": SchemaType.INT},
        {
            "type": "record",
            "name": "Record2",
            "fields": [{"name": "field1", "type": SchemaType.INT}],
        },
    ]
)
UNION_INT_ENUM1_AB = UnionSchema([{"type": SchemaType.INT}, ENUM1_AB_SCHEMA.to_json()])
UNION_INT_FIXED_4_BYTES = UnionSchema([{"type": SchemaType.INT}, FIXED_4_BYTES.to_json()])
UNION_INT_BOOLEAN = UnionSchema([{"type": SchemaType.INT}, {"type": SchemaType.BOOLEAN}])
UNION_INT_ARRAY_INT = UnionSchema([{"type": SchemaType.INT}, INT_ARRAY_SCHEMA.to_json()])
UNION_INT_MAP_INT = UnionSchema([{"type": SchemaType.INT}, INT_MAP_SCHEMA.to_json()])
UNION_INT_NULL = UnionSchema([{"type": SchemaType.INT}, {"type": SchemaType.NULL}])
FIXED_4_ANOTHER_NAME = parse(json.dumps({"type": SchemaType.FIXED, "name": "AnotherName", "size": 4}))
RECORD1_WITH_ENUM_AB = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "field1", "type": ENUM1_AB_SCHEMA.to_json()}],
        }
    )
)
RECORD1_WITH_ENUM_ABC = parse(
    json.dumps(
        {
            "type": SchemaType.RECORD,
            "name": "Record1",
            "fields": [{"name": "field1", "type": ENUM1_ABC_SCHEMA.to_json()}],
        }
    )
)


class TestCompatibility(unittest.TestCase):
    def test_simple_schema_promotion(self):
        field_alias_reader = parse(
            json.dumps(
                {
                    "name": "foo",
                    "type": "record",
                    "fields": [{"type": "int", "name": "bar", "aliases": ["f1"]}],
                }
            )
        )
        record_alias_reader = parse(
            json.dumps(
                {
                    "name": "other",
                    "type": "record",
                    "fields": [{"type": "int", "name": "f1"}],
                    "aliases": ["foo"],
                }
            )
        )

        writer = parse(
            json.dumps(
                {
                    "name": "foo",
                    "type": "record",
                    "fields": [
                        {"type": "int", "name": "f1"},
                        {
                            "type": "string",
                            "name": "f2",
                        },
                    ],
                }
            )
        )
        # alias testing
        res = ReaderWriterCompatibilityChecker().get_compatibility(field_alias_reader, writer)
        self.assertIs(res.compatibility, SchemaCompatibilityType.compatible, res.locations)
        res = ReaderWriterCompatibilityChecker().get_compatibility(record_alias_reader, writer)
        self.assertIs(res.compatibility, SchemaCompatibilityType.compatible, res.locations)

    def test_schema_compatibility(self):
        # testValidateSchemaPairMissingField
        writer = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [
                        {"name": "oldField1", "type": SchemaType.INT},
                        {"name": "oldField2", "type": SchemaType.STRING},
                    ],
                }
            )
        )
        reader = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [{"name": "oldField1", "type": SchemaType.INT}],
                }
            )
        )
        self.assertTrue(self.are_compatible(reader, writer))
        # testValidateSchemaPairMissingSecondField
        reader = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [{"name": "oldField2", "type": SchemaType.STRING}],
                }
            )
        )
        self.assertTrue(self.are_compatible(reader, writer))
        # testValidateSchemaPairAllFields
        reader = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [
                        {"name": "oldField1", "type": SchemaType.INT},
                        {"name": "oldField2", "type": SchemaType.STRING},
                    ],
                }
            )
        )
        self.assertTrue(self.are_compatible(reader, writer))
        # testValidateSchemaNewFieldWithDefault
        reader = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [
                        {"name": "oldField1", "type": SchemaType.INT},
                        {"name": "newField2", "type": SchemaType.INT, "default": 42},
                    ],
                }
            )
        )
        self.assertTrue(self.are_compatible(reader, writer))
        # testValidateSchemaNewField
        reader = parse(
            json.dumps(
                {
                    "type": SchemaType.RECORD,
                    "name": "Record",
                    "fields": [
                        {"name": "oldField1", "type": SchemaType.INT},
                        {"name": "newField2", "type": SchemaType.INT},
                    ],
                }
            )
        )
        self.assertFalse(self.are_compatible(reader, writer))
        # testValidateArrayWriterSchema
        writer = parse(json.dumps({"type": SchemaType.ARRAY, "items": {"type": SchemaType.STRING}}))
        reader = parse(json.dumps({"type": SchemaType.ARRAY, "items": {"type": SchemaType.STRING}}))
        self.assertTrue(self.are_compatible(reader, writer))
        reader = parse(json.dumps({"type": SchemaType.MAP, "values": {"type": SchemaType.STRING}}))
        self.assertFalse(self.are_compatible(reader, writer))
        # testValidatePrimitiveWriterSchema
        writer = parse(json.dumps({"type": SchemaType.STRING}))
        reader = parse(json.dumps({"type": SchemaType.STRING}))
        self.assertTrue(self.are_compatible(reader, writer))
        reader = parse(json.dumps({"type": SchemaType.INT}))
        self.assertFalse(self.are_compatible(reader, writer))
        # testUnionReaderWriterSubsetIncompatibility
        writer = parse(
            json.dumps(
                {
                    "name": "Record",
                    "type": "record",
                    "fields": [
                        {
                            "name": "f1",
                            "type": [
                                SchemaType.INT,
                                SchemaType.STRING,
                                SchemaType.LONG,
                            ],
                        }
                    ],
                }
            )
        )
        reader = parse(
            json.dumps(
                {
                    "name": "Record",
                    "type": SchemaType.RECORD,
                    "fields": [{"name": "f1", "type": [SchemaType.INT, SchemaType.STRING]}],
                }
            )
        )
        reader = reader.fields[0].type
        writer = writer.fields[0].type
        self.assertIsInstance(reader, UnionSchema)
        self.assertIsInstance(writer, UnionSchema)
        self.assertFalse(self.are_compatible(reader, writer))
        # testReaderWriterCompatibility
        compatible_reader_writer_test_cases = [
            (BOOLEAN_SCHEMA, BOOLEAN_SCHEMA),
            (INT_SCHEMA, INT_SCHEMA),
            (LONG_SCHEMA, INT_SCHEMA),
            (LONG_SCHEMA, LONG_SCHEMA),
            (FLOAT_SCHEMA, INT_SCHEMA),
            (FLOAT_SCHEMA, LONG_SCHEMA),
            (DOUBLE_SCHEMA, LONG_SCHEMA),
            (DOUBLE_SCHEMA, INT_SCHEMA),
            (DOUBLE_SCHEMA, FLOAT_SCHEMA),
            (STRING_SCHEMA, STRING_SCHEMA),
            (BYTES_SCHEMA, BYTES_SCHEMA),
            (STRING_SCHEMA, BYTES_SCHEMA),
            (BYTES_SCHEMA, STRING_SCHEMA),
            (INT_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
            (LONG_ARRAY_SCHEMA, INT_ARRAY_SCHEMA),
            (INT_MAP_SCHEMA, INT_MAP_SCHEMA),
            (LONG_MAP_SCHEMA, INT_MAP_SCHEMA),
            (ENUM1_AB_SCHEMA, ENUM1_AB_SCHEMA),
            (ENUM1_ABC_SCHEMA, ENUM1_AB_SCHEMA),
            # Union related pairs
            (EMPTY_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, INT_LONG_UNION_SCHEMA),
            (INT_UNION_SCHEMA, INT_UNION_SCHEMA),
            (INT_STRING_UNION_SCHEMA, STRING_INT_UNION_SCHEMA),
            (INT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (LONG_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (LONG_UNION_SCHEMA, INT_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, INT_UNION_SCHEMA),
            (DOUBLE_UNION_SCHEMA, INT_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, LONG_UNION_SCHEMA),
            (DOUBLE_UNION_SCHEMA, LONG_UNION_SCHEMA),
            (FLOAT_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (DOUBLE_UNION_SCHEMA, FLOAT_UNION_SCHEMA),
            (STRING_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (STRING_UNION_SCHEMA, BYTES_UNION_SCHEMA),
            (BYTES_UNION_SCHEMA, EMPTY_UNION_SCHEMA),
            (BYTES_UNION_SCHEMA, STRING_UNION_SCHEMA),
            (DOUBLE_UNION_SCHEMA, INT_FLOAT_UNION_SCHEMA),
            # Readers capable of reading all branches of a union are compatible
            (FLOAT_SCHEMA, INT_FLOAT_UNION_SCHEMA),
            (LONG_SCHEMA, INT_LONG_UNION_SCHEMA),
            (DOUBLE_SCHEMA, INT_FLOAT_UNION_SCHEMA),
            (DOUBLE_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA),
            # Special case of singleton unions:
            (FLOAT_SCHEMA, FLOAT_UNION_SCHEMA),
            (INT_UNION_SCHEMA, INT_SCHEMA),
            (INT_SCHEMA, INT_UNION_SCHEMA),
            # Fixed types
            (FIXED_4_BYTES, FIXED_4_BYTES),
            # Tests involving records:
            (EMPTY_RECORD1, EMPTY_RECORD1),
            (EMPTY_RECORD1, A_INT_RECORD1),
            (A_INT_RECORD1, A_INT_RECORD1),
            (A_DINT_RECORD1, A_INT_RECORD1),
            (A_DINT_RECORD1, A_DINT_RECORD1),
            (A_INT_RECORD1, A_DINT_RECORD1),
            (A_LONG_RECORD1, A_INT_RECORD1),
            (A_INT_RECORD1, A_INT_B_INT_RECORD1),
            (A_DINT_RECORD1, A_INT_B_INT_RECORD1),
            (A_INT_B_DINT_RECORD1, A_INT_RECORD1),
            (A_DINT_B_DINT_RECORD1, EMPTY_RECORD1),
            (A_DINT_B_DINT_RECORD1, A_INT_RECORD1),
            (A_INT_B_INT_RECORD1, A_DINT_B_DINT_RECORD1),
            (parse(json.dumps({"type": "null"})), parse(json.dumps({"type": "null"}))),
            (NULL_SCHEMA, NULL_SCHEMA),
            (ENUM_AB_ENUM_DEFAULT_A_RECORD, ENUM_ABC_ENUM_DEFAULT_A_RECORD),
            (
                ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD,
                ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD,
            ),
            (NS_RECORD1, NS_RECORD2),
        ]

        for (reader, writer) in compatible_reader_writer_test_cases:
            self.assertTrue(self.are_compatible(reader, writer))

    def test_schema_compatibility_fixed_size_mismatch(self):
        incompatible_fixed_pairs = [
            (FIXED_4_BYTES, FIXED_8_BYTES, "expected: 8, found: 4", "/size"),
            (FIXED_8_BYTES, FIXED_4_BYTES, "expected: 4, found: 8", "/size"),
            (
                A_DINT_B_DFIXED_8_BYTES_RECORD1,
                A_DINT_B_DFIXED_4_BYTES_RECORD1,
                "expected: 4, found: 8",
                "/fields/1/type/size",
            ),
            (
                A_DINT_B_DFIXED_4_BYTES_RECORD1,
                A_DINT_B_DFIXED_8_BYTES_RECORD1,
                "expected: 8, found: 4",
                "/fields/1/type/size",
            ),
        ]
        for (reader, writer, message, location) in incompatible_fixed_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertIn(
                location,
                result.locations,
                f"expected {location}, found {result}",
            )
            self.assertIn(
                message,
                result.messages,
                f"expected {location}, found {result}",
            )

    def test_schema_compatibility_missing_enum_symbols(self):
        incompatible_pairs = [
            # str(set) representation
            (ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA, "{'C'}", "/symbols"),
            (ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA, "{'A'}", "/symbols"),
            (
                RECORD1_WITH_ENUM_AB,
                RECORD1_WITH_ENUM_ABC,
                "{'C'}",
                "/fields/0/type/symbols",
            ),
        ]
        for (reader, writer, message, location) in incompatible_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertIn(message, result.messages)
            self.assertIn(location, result.locations)

    def test_schema_compatibility_missing_union_branch(self):
        incompatible_pairs = [
            (
                INT_UNION_SCHEMA,
                INT_STRING_UNION_SCHEMA,
                {"reader union lacking writer type: STRING"},
                {"/1"},
            ),
            (
                STRING_UNION_SCHEMA,
                INT_STRING_UNION_SCHEMA,
                {"reader union lacking writer type: INT"},
                {"/0"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_RECORD1,
                {"reader union lacking writer type: RECORD"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_RECORD2,
                {"reader union lacking writer type: RECORD"},
                {"/1"},
            ),
            (
                UNION_INT_RECORD1,
                UNION_INT_RECORD2,
                {"reader union lacking writer type: RECORD"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_ENUM1_AB,
                {"reader union lacking writer type: ENUM"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_FIXED_4_BYTES,
                {"reader union lacking writer type: FIXED"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_BOOLEAN,
                {"reader union lacking writer type: BOOLEAN"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                LONG_UNION_SCHEMA,
                {"reader union lacking writer type: LONG"},
                {"/0"},
            ),
            (
                INT_UNION_SCHEMA,
                FLOAT_UNION_SCHEMA,
                {"reader union lacking writer type: FLOAT"},
                {"/0"},
            ),
            (
                INT_UNION_SCHEMA,
                DOUBLE_UNION_SCHEMA,
                {"reader union lacking writer type: DOUBLE"},
                {"/0"},
            ),
            (
                INT_UNION_SCHEMA,
                BYTES_UNION_SCHEMA,
                {"reader union lacking writer type: BYTES"},
                {"/0"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_ARRAY_INT,
                {"reader union lacking writer type: ARRAY"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_MAP_INT,
                {"reader union lacking writer type: MAP"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                UNION_INT_NULL,
                {"reader union lacking writer type: NULL"},
                {"/1"},
            ),
            (
                INT_UNION_SCHEMA,
                INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
                {
                    "reader union lacking writer type: LONG",
                    "reader union lacking writer type: FLOAT",
                    "reader union lacking writer type: DOUBLE",
                },
                {"/1", "/2", "/3"},
            ),
            (
                A_DINT_B_DINT_UNION_RECORD1,
                A_DINT_B_DINT_STRING_UNION_RECORD1,
                {"reader union lacking writer type: STRING"},
                {"/fields/1/type/1"},
            ),
        ]

        for (reader, writer, message, location) in incompatible_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertEqual(result.messages, message)
            self.assertEqual(result.locations, location)

    def test_schema_compatibility_name_mismatch(self):
        incompatible_pairs = [
            (ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name"),
            (EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name"),
            (FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name"),
            (
                A_DINT_B_DENUM_1_RECORD1,
                A_DINT_B_DENUM_2_RECORD1,
                "expected: Enum2",
                "/fields/1/type/name",
            ),
        ]

        for (reader, writer, message, location) in incompatible_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertIn(message, result.messages)
            self.assertIn(location, result.locations)

    def test_schema_compatibility_reader_field_missing_default_value(self):
        incompatible_pairs = [
            (A_INT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
            (A_INT_B_DINT_RECORD1, EMPTY_RECORD1, "a", "/fields/0"),
        ]
        for (reader, writer, message, location) in incompatible_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertEqual(len(result.messages), 1)
            self.assertEqual(len(result.locations), 1)
            self.assertEqual(message, "".join(result.messages))
            self.assertEqual(location, "".join(result.locations))

    def test_schema_compatibility_type_mismatch(self):
        incompatible_pairs = [
            (
                NULL_SCHEMA,
                INT_SCHEMA,
                "reader type: null not compatible with writer type: int",
                "/",
            ),
            (
                NULL_SCHEMA,
                LONG_SCHEMA,
                "reader type: null not compatible with writer type: long",
                "/",
            ),
            (
                BOOLEAN_SCHEMA,
                INT_SCHEMA,
                "reader type: boolean not compatible with writer type: int",
                "/",
            ),
            (
                INT_SCHEMA,
                NULL_SCHEMA,
                "reader type: int not compatible with writer type: null",
                "/",
            ),
            (
                INT_SCHEMA,
                BOOLEAN_SCHEMA,
                "reader type: int not compatible with writer type: boolean",
                "/",
            ),
            (
                INT_SCHEMA,
                LONG_SCHEMA,
                "reader type: int not compatible with writer type: long",
                "/",
            ),
            (
                INT_SCHEMA,
                FLOAT_SCHEMA,
                "reader type: int not compatible with writer type: float",
                "/",
            ),
            (
                INT_SCHEMA,
                DOUBLE_SCHEMA,
                "reader type: int not compatible with writer type: double",
                "/",
            ),
            (
                LONG_SCHEMA,
                FLOAT_SCHEMA,
                "reader type: long not compatible with writer type: float",
                "/",
            ),
            (
                LONG_SCHEMA,
                DOUBLE_SCHEMA,
                "reader type: long not compatible with writer type: double",
                "/",
            ),
            (
                FLOAT_SCHEMA,
                DOUBLE_SCHEMA,
                "reader type: float not compatible with writer type: double",
                "/",
            ),
            (
                DOUBLE_SCHEMA,
                STRING_SCHEMA,
                "reader type: double not compatible with writer type: string",
                "/",
            ),
            (
                FIXED_4_BYTES,
                STRING_SCHEMA,
                "reader type: fixed not compatible with writer type: string",
                "/",
            ),
            (
                STRING_SCHEMA,
                BOOLEAN_SCHEMA,
                "reader type: string not compatible with writer type: boolean",
                "/",
            ),
            (
                STRING_SCHEMA,
                INT_SCHEMA,
                "reader type: string not compatible with writer type: int",
                "/",
            ),
            (
                BYTES_SCHEMA,
                NULL_SCHEMA,
                "reader type: bytes not compatible with writer type: null",
                "/",
            ),
            (
                BYTES_SCHEMA,
                INT_SCHEMA,
                "reader type: bytes not compatible with writer type: int",
                "/",
            ),
            (
                A_INT_RECORD1,
                INT_SCHEMA,
                "reader type: record not compatible with writer type: int",
                "/",
            ),
            (
                INT_ARRAY_SCHEMA,
                LONG_ARRAY_SCHEMA,
                "reader type: int not compatible with writer type: long",
                "/items",
            ),
            (
                INT_MAP_SCHEMA,
                INT_ARRAY_SCHEMA,
                "reader type: map not compatible with writer type: array",
                "/",
            ),
            (
                INT_ARRAY_SCHEMA,
                INT_MAP_SCHEMA,
                "reader type: array not compatible with writer type: map",
                "/",
            ),
            (
                INT_MAP_SCHEMA,
                LONG_MAP_SCHEMA,
                "reader type: int not compatible with writer type: long",
                "/values",
            ),
            (
                INT_SCHEMA,
                ENUM2_AB_SCHEMA,
                "reader type: int not compatible with writer type: enum",
                "/",
            ),
            (
                ENUM2_AB_SCHEMA,
                INT_SCHEMA,
                "reader type: enum not compatible with writer type: int",
                "/",
            ),
            (
                FLOAT_SCHEMA,
                INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
                "reader type: float not compatible with writer type: double",
                "/",
            ),
            (
                LONG_SCHEMA,
                INT_FLOAT_UNION_SCHEMA,
                "reader type: long not compatible with writer type: float",
                "/",
            ),
            (
                INT_SCHEMA,
                INT_FLOAT_UNION_SCHEMA,
                "reader type: int not compatible with writer type: float",
                "/",
            ),
            # (INT_LIST_RECORD, LONG_LIST_RECORD, "reader type: int not compatible with writer type: long", "/fields/0/type"),
            (
                NULL_SCHEMA,
                INT_SCHEMA,
                "reader type: null not compatible with writer type: int",
                "/",
            ),
        ]
        for (reader, writer, message, location) in incompatible_pairs:
            result = ReaderWriterCompatibilityChecker().get_compatibility(reader, writer)
            self.assertIs(result.compatibility, SchemaCompatibilityType.incompatible)
            self.assertIn(message, result.messages)
            self.assertIn(location, result.locations)

    def are_compatible(self, reader: Schema, writer: Schema) -> bool:
        return ReaderWriterCompatibilityChecker().get_compatibility(reader, writer).compatibility is SchemaCompatibilityType.compatible
