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

"""Test the protocol parsing logic."""

import json
import unittest

import avro.errors
import avro.protocol
import avro.schema


class TestProtocol:
    """A proxy for a protocol string that provides useful test metadata."""

    def __init__(self, data, name="", comment=""):
        if not isinstance(data, str):
            data = json.dumps(data)
        self.data = data
        self.name = name or data
        self.comment = comment

    def parse(self):
        return avro.protocol.parse(str(self))

    def __str__(self):
        return str(self.data)


class ValidTestProtocol(TestProtocol):
    """A proxy for a valid protocol string that provides useful test metadata."""

    valid = True


class InvalidTestProtocol(TestProtocol):
    """A proxy for an invalid protocol string that provides useful test metadata."""

    valid = False


HELLO_WORLD = ValidTestProtocol(
    {
        "namespace": "com.acme",
        "protocol": "HelloWorld",
        "types": [
            {
                "name": "Greeting",
                "type": "record",
                "fields": [{"name": "message", "type": "string"}],
            },
            {
                "name": "Curse",
                "type": "error",
                "fields": [{"name": "message", "type": "string"}],
            },
        ],
        "messages": {
            "hello": {
                "request": [{"name": "greeting", "type": "Greeting"}],
                "response": "Greeting",
                "errors": ["Curse"],
            }
        },
    }
)
EXAMPLES = [
    HELLO_WORLD,
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test",
            "protocol": "Simple",
            "types": [
                {"name": "Kind", "type": "enum", "symbols": ["FOO", "BAR", "BAZ"]},
                {"name": "MD5", "type": "fixed", "size": 16},
                {
                    "name": "TestRecord",
                    "type": "record",
                    "fields": [
                        {"name": "name", "type": "string", "order": "ignore"},
                        {"name": "kind", "type": "Kind", "order": "descending"},
                        {"name": "hash", "type": "MD5"},
                    ],
                },
                {
                    "name": "TestError",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "hello": {
                    "request": [{"name": "greeting", "type": "string"}],
                    "response": "string",
                },
                "echo": {
                    "request": [{"name": "record", "type": "TestRecord"}],
                    "response": "TestRecord",
                },
                "add": {
                    "request": [
                        {"name": "arg1", "type": "int"},
                        {"name": "arg2", "type": "int"},
                    ],
                    "response": "int",
                },
                "echoBytes": {
                    "request": [{"name": "data", "type": "bytes"}],
                    "response": "bytes",
                },
                "error": {"request": [], "response": "null", "errors": ["TestError"]},
            },
        }
    ),
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test.namespace",
            "protocol": "TestNamespace",
            "types": [
                {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                {
                    "name": "TestRecord",
                    "type": "record",
                    "fields": [{"name": "hash", "type": "org.apache.avro.test.util.MD5"}],
                },
                {
                    "name": "TestError",
                    "namespace": "org.apache.avro.test.errors",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "echo": {
                    "request": [{"name": "record", "type": "TestRecord"}],
                    "response": "TestRecord",
                },
                "error": {
                    "request": [],
                    "response": "null",
                    "errors": ["org.apache.avro.test.errors.TestError"],
                },
            },
        }
    ),
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test.namespace",
            "protocol": "TestImplicitNamespace",
            "types": [
                {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "fields": [{"name": "foo", "type": "string"}],
                },
                {
                    "name": "TestRecord",
                    "type": "record",
                    "fields": [
                        {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                        {"name": "unqualified", "type": "ReferencedRecord"},
                    ],
                },
                {
                    "name": "TestError",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "echo": {
                    "request": [
                        {
                            "name": "qualified",
                            "type": "org.apache.avro.test.namespace.TestRecord",
                        }
                    ],
                    "response": "TestRecord",
                },
                "error": {
                    "request": [],
                    "response": "null",
                    "errors": ["org.apache.avro.test.namespace.TestError"],
                },
            },
        }
    ),
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test.namespace",
            "protocol": "TestNamespaceTwo",
            "types": [
                {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "namespace": "org.apache.avro.other.namespace",
                    "fields": [{"name": "foo", "type": "string"}],
                },
                {
                    "name": "TestRecord",
                    "type": "record",
                    "fields": [
                        {"name": "hash", "type": "org.apache.avro.test.util.MD5"},
                        {
                            "name": "qualified",
                            "type": "org.apache.avro.other.namespace.ReferencedRecord",
                        },
                    ],
                },
                {
                    "name": "TestError",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "echo": {
                    "request": [
                        {
                            "name": "qualified",
                            "type": "org.apache.avro.test.namespace.TestRecord",
                        }
                    ],
                    "response": "TestRecord",
                },
                "error": {
                    "request": [],
                    "response": "null",
                    "errors": ["org.apache.avro.test.namespace.TestError"],
                },
            },
        }
    ),
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test.namespace",
            "protocol": "TestValidRepeatedName",
            "types": [
                {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "namespace": "org.apache.avro.other.namespace",
                    "fields": [{"name": "foo", "type": "string"}],
                },
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "fields": [{"name": "bar", "type": "double"}],
                },
                {
                    "name": "TestError",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "echo": {
                    "request": [{"name": "qualified", "type": "ReferencedRecord"}],
                    "response": "org.apache.avro.other.namespace.ReferencedRecord",
                },
                "error": {
                    "request": [],
                    "response": "null",
                    "errors": ["org.apache.avro.test.namespace.TestError"],
                },
            },
        }
    ),
    InvalidTestProtocol(
        {
            "namespace": "org.apache.avro.test.namespace",
            "protocol": "TestInvalidRepeatedName",
            "types": [
                {"name": "org.apache.avro.test.util.MD5", "type": "fixed", "size": 16},
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "fields": [{"name": "foo", "type": "string"}],
                },
                {
                    "name": "ReferencedRecord",
                    "type": "record",
                    "fields": [{"name": "bar", "type": "double"}],
                },
                {
                    "name": "TestError",
                    "type": "error",
                    "fields": [{"name": "message", "type": "string"}],
                },
            ],
            "messages": {
                "echo": {
                    "request": [{"name": "qualified", "type": "ReferencedRecord"}],
                    "response": "org.apache.avro.other.namespace.ReferencedRecord",
                },
                "error": {
                    "request": [],
                    "response": "null",
                    "errors": ["org.apache.avro.test.namespace.TestError"],
                },
            },
        }
    ),
    ValidTestProtocol(
        {
            "namespace": "org.apache.avro.test",
            "protocol": "BulkData",
            "types": [],
            "messages": {
                "read": {"request": [], "response": "bytes"},
                "write": {
                    "request": [{"name": "data", "type": "bytes"}],
                    "response": "null",
                },
            },
        }
    ),
    ValidTestProtocol(
        {
            "protocol": "API",
            "namespace": "xyz.api",
            "types": [
                {
                    "type": "enum",
                    "name": "Symbology",
                    "namespace": "xyz.api.product",
                    "symbols": ["OPRA", "CUSIP", "ISIN", "SEDOL"],
                },
                {
                    "type": "record",
                    "name": "Symbol",
                    "namespace": "xyz.api.product",
                    "fields": [
                        {"name": "symbology", "type": "xyz.api.product.Symbology"},
                        {"name": "symbol", "type": "string"},
                    ],
                },
                {
                    "type": "record",
                    "name": "MultiSymbol",
                    "namespace": "xyz.api.product",
                    "fields": [
                        {
                            "name": "symbols",
                            "type": {"type": "map", "values": "xyz.api.product.Symbol"},
                        }
                    ],
                },
            ],
            "messages": {},
        }
    ),
]

VALID_EXAMPLES = [e for e in EXAMPLES if getattr(e, "valid", False)]


class TestMisc(unittest.TestCase):
    def test_inner_namespace_set(self):
        print("")
        print("TEST INNER NAMESPACE")
        print("===================")
        print("")
        proto = HELLO_WORLD.parse()
        self.assertEqual(proto.namespace, "com.acme")
        self.assertEqual(proto.fullname, "com.acme.HelloWorld")
        greeting_type = proto.types_dict["Greeting"]
        self.assertEqual(greeting_type.namespace, "com.acme")

    def test_inner_namespace_not_rendered(self):
        proto = HELLO_WORLD.parse()
        self.assertEqual("com.acme.Greeting", proto.types[0].fullname)
        self.assertEqual("Greeting", proto.types[0].name)
        # but there shouldn't be 'namespace' rendered to json on the inner type
        self.assertFalse("namespace" in proto.to_json()["types"][0])


class ProtocolParseTestCase(unittest.TestCase):
    """Enable generating parse test cases over all the valid and invalid example protocols."""

    def __init__(self, test_proto):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_valid" if test_proto.valid else "parse_invalid")
        self.test_proto = test_proto

    def parse_valid(self):
        """Parsing a valid protocol should not error."""
        try:
            self.test_proto.parse()
        except avro.errors.ProtocolParseException:  # pragma: no coverage
            self.fail(f"Valid protocol failed to parse: {self.test_proto!s}")

    def parse_invalid(self):
        """Parsing an invalid protocol should error."""
        with self.assertRaises(
            (avro.errors.ProtocolParseException, avro.errors.SchemaParseException),
            msg=f"Invalid protocol should not have parsed: {self.test_proto!s}",
        ):
            self.test_proto.parse()


class ErrorProtocolTestCase(unittest.TestCase):
    """Enable generating error protocol test cases across all the valid test protocols."""

    def __init__(self, test_proto):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("check_error_protocol_exists")
        self.test_proto = test_proto

    def check_error_protocol_exists(self):
        """Protocol messages should always have at least a string error protocol."""
        p = self.test_proto.parse()
        if p.messages is not None:
            for k, m in p.messages.items():
                self.assertIsNotNone(m.errors, f"Message {k} did not have the expected implicit string error protocol.")


class RoundTripParseTestCase(unittest.TestCase):
    """Enable generating round-trip parse test cases over all the valid test protocols."""

    def __init__(self, test_proto):
        """Ignore the normal signature for unittest.TestCase because we are generating
        many test cases from this one class. This is safe as long as the autoloader
        ignores this class. The autoloader will ignore this class as long as it has
        no methods starting with `test_`.
        """
        super().__init__("parse_round_trip")
        self.test_proto = test_proto

    def parse_round_trip(self):
        """The string of a Protocol should be parseable to the same Protocol."""
        parsed = self.test_proto.parse()
        round_trip = avro.protocol.parse(str(parsed))
        self.assertEqual(parsed, round_trip)


def load_tests(loader, default_tests, pattern):
    """Generate test cases across many test protocol."""
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestMisc))
    suite.addTests(ProtocolParseTestCase(ex) for ex in EXAMPLES)
    suite.addTests(RoundTripParseTestCase(ex) for ex in VALID_EXAMPLES)
    suite.addTests(ErrorProtocolTestCase(ex) for ex in VALID_EXAMPLES)
    return suite


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
