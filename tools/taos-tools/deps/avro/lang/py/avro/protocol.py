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
Protocol implementation.

https://avro.apache.org/docs/current/spec.html#Protocol+Declaration
"""

import hashlib
import json
from typing import Mapping, Optional, Sequence, Union, cast

import avro.errors
import avro.name
import avro.schema
from avro.utils import TypedDict

# TODO(hammer): confirmed 'fixed' with Doug
VALID_TYPE_SCHEMA_TYPES = ("enum", "record", "error", "fixed")


class MessageObject(TypedDict, total=False):
    request: Sequence[Mapping[str, object]]
    response: Union[str, object]
    errors: Optional[Sequence[str]]


class ProtocolObject(TypedDict, total=False):
    protocol: str
    namespace: str
    types: Sequence[str]
    messages: Mapping[str, MessageObject]


class Protocol:
    """
    Avro protocols describe RPC interfaces. Like schemas, they are defined with JSON text.

    A protocol is a JSON object with the following attributes:

    - protocol, a string, the name of the protocol (required);
    - namespace, an optional string that qualifies the name;
    - doc, an optional string describing this protocol;
    - types, an optional list of definitions of named types (records, enums, fixed and errors). An error definition is just like a record definition except it uses "error" instead of "record". Note that forward references to named types are not permitted.
    - messages, an optional JSON object whose keys are message names and whose values are objects whose attributes are described below. No two messages may have the same name.

    The name and namespace qualification rules defined for schema objects apply to protocols as well.
    """

    __slots__ = [
        "_md5",
        "_messages",
        "_name",
        "_namespace",
        "_types",
    ]

    _md5: bytes
    _messages: Optional[Mapping[str, "Message"]]
    _name: str
    _namespace: Optional[str]
    _types: Optional[Sequence[avro.schema.NamedSchema]]

    def __init__(
        self,
        name: str,
        namespace: Optional[str] = None,
        types: Optional[Sequence[str]] = None,
        messages: Optional[Mapping[str, "MessageObject"]] = None,
    ) -> None:
        if not name:
            raise avro.errors.ProtocolParseException("Protocols must have a non-empty name.")
        if not isinstance(name, str):
            raise avro.errors.ProtocolParseException("The name property must be a string.")
        if not (namespace is None or isinstance(namespace, str)):
            raise avro.errors.ProtocolParseException("The namespace property must be a string.")
        if not (types is None or isinstance(types, list)):
            raise avro.errors.ProtocolParseException("The types property must be a list.")
        if not (messages is None or callable(getattr(messages, "get", None))):
            raise avro.errors.ProtocolParseException("The messages property must be a JSON object.")
        type_names = avro.name.Names()

        self._name = name
        self._namespace = type_names.default_namespace = namespace
        self._types = _parse_types(types, type_names) if types else None
        self._messages = _parse_messages(messages, type_names) if messages else None
        self._md5 = hashlib.md5(str(self).encode()).digest()

    @property
    def name(self) -> str:
        return self._name

    @property
    def namespace(self) -> Optional[str]:
        return self._namespace

    @property
    def fullname(self) -> Optional[str]:
        return avro.name.Name(self.name, self.namespace, None).fullname

    @property
    def types(self) -> Optional[Sequence[avro.schema.NamedSchema]]:
        return self._types

    @property
    def types_dict(self) -> Optional[Mapping[str, avro.schema.NamedSchema]]:
        return None if self.types is None else {type_.name: type_ for type_ in self.types}

    @property
    def messages(self) -> Optional[Mapping[str, "Message"]]:
        return self._messages

    @property
    def md5(self) -> bytes:
        return self._md5

    def to_json(self) -> Mapping[str, Union[str, Sequence[object], Mapping[str, "MessageObject"]]]:
        names = avro.name.Names(default_namespace=self.namespace)
        return {
            "protocol": self.name,
            **({"namespace": self.namespace} if self.namespace else {}),
            **({"types": [t.to_json(names) for t in self.types]} if self.types else {}),
            **({"messages": {name: body.to_json(names) for name, body in self.messages.items()}} if self.messages else {}),
        }

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def __eq__(self, that: object) -> bool:
        this_ = json.loads(str(self))
        try:
            that_ = json.loads(str(that))
        except json.decoder.JSONDecodeError:
            return False
        return cast(bool, this_ == that_)


class Message:
    """
    A message has attributes:

    - a doc, an optional description of the message,
    - a request, a list of named, typed parameter schemas (this has the same form as the fields of a record declaration);
    - a response schema;
    - an optional union of declared error schemas. The effective union has "string" prepended to the declared union, to permit transmission of undeclared "system" errors. For example, if the declared error union is ["AccessError"], then the effective union is ["string", "AccessError"]. When no errors are declared, the effective error union is ["string"]. Errors are serialized using the effective union; however, a protocol's JSON declaration contains only the declared union.
    - an optional one-way boolean parameter.

    A request parameter list is processed equivalently to an anonymous record. Since record field lists may vary between reader and writer, request parameters may also differ between the caller and responder, and such differences are resolved in the same manner as record field differences.

    The one-way parameter may only be true when the response type is "null" and no errors are listed.
    """

    __slots__ = [
        "_errors",
        "_name",
        "_request",
        "_response",
    ]

    def __init__(
        self,
        name: str,
        request: Sequence[Mapping[str, object]],
        response: Union[str, object],
        errors: Optional[Sequence[str]] = None,
        names: Optional[avro.name.Names] = None,
    ) -> None:
        self._name = name
        names = names or avro.name.Names()
        self._request = _parse_request(request, names)
        self._response = _parse_response(response, names)
        self._errors = _parse_errors(errors or [], names)

    @property
    def name(self) -> str:
        return self._name

    @property
    def request(self) -> avro.schema.RecordSchema:
        return self._request

    @property
    def response(self) -> avro.schema.Schema:
        return self._response

    @property
    def errors(self) -> avro.schema.ErrorUnionSchema:
        return self._errors

    def __str__(self) -> str:
        return json.dumps(self.to_json())

    def to_json(self, names: Optional[avro.name.Names] = None) -> "MessageObject":
        names = names or avro.name.Names()

        try:
            to_dump = MessageObject()
        except NameError:
            to_dump = {}
        to_dump["request"] = self.request.to_json(names)
        to_dump["response"] = self.response.to_json(names)
        if self.errors:
            to_dump["errors"] = self.errors.to_json(names)

        return to_dump

    def __eq__(self, that: object) -> bool:
        return all(hasattr(that, prop) and getattr(self, prop) == getattr(that, prop) for prop in self.__class__.__slots__)


def _parse_request(request: Sequence[Mapping[str, object]], names: avro.name.Names) -> avro.schema.RecordSchema:
    if not isinstance(request, Sequence):
        raise avro.errors.ProtocolParseException(f"Request property not a list: {request}")
    return avro.schema.RecordSchema(None, None, request, names, "request")


def _parse_response(response: Union[str, object], names: avro.name.Names) -> avro.schema.Schema:
    return (isinstance(response, str) and names.get_name(response)) or avro.schema.make_avsc_object(response, names)


def _parse_errors(errors: Sequence[str], names: avro.name.Names) -> avro.schema.ErrorUnionSchema:
    """Even if errors is empty, we still want an ErrorUnionSchema with "string" in it."""
    if not isinstance(errors, Sequence):
        raise avro.errors.ProtocolParseException(f"Errors property not a list: {errors}")
    errors_for_parsing = {"type": "error_union", "declared_errors": errors}
    return cast(avro.schema.ErrorUnionSchema, avro.schema.make_avsc_object(errors_for_parsing, names))


def make_avpr_object(json_data: "ProtocolObject") -> Protocol:
    """Build Avro Protocol from data parsed out of JSON string."""
    if not hasattr(json_data, "get"):
        raise avro.errors.ProtocolParseException(f"Not a JSON object: {json_data}")
    name = json_data["protocol"]
    namespace = json_data.get("namespace")
    types = json_data.get("types")
    messages = json_data.get("messages")
    return Protocol(name, namespace, types, messages)


def parse(json_string: str) -> Protocol:
    """Constructs the Protocol from the JSON text."""
    try:
        protocol_object = json.loads(json_string)
    except ValueError:
        raise avro.errors.ProtocolParseException(f"Error parsing JSON: {json_string}")
    return make_avpr_object(protocol_object)


def _parse_types(types: Sequence[str], type_names: avro.name.Names) -> Sequence[avro.schema.NamedSchema]:
    schemas = []
    for type_ in types:
        schema = avro.schema.make_avsc_object(type_, type_names)
        if isinstance(schema, avro.schema.NamedSchema):
            schemas.append(schema)
            continue
        raise avro.errors.ProtocolParseException(f"Type {type_} not an enum, fixed, record, or error.")
    return schemas


def _parse_messages(message_objects: Mapping[str, "MessageObject"], names: avro.name.Names) -> Mapping[str, Message]:
    messages = {}
    for name, body in message_objects.items():
        if not hasattr(body, "get"):
            raise avro.errors.ProtocolParseException(f'Message name "{name}" has non-object body {body}.')
        request = body["request"]
        response = body["response"]
        errors = body.get("errors")
        messages[name] = Message(name, request, response, errors, names)
    return messages
