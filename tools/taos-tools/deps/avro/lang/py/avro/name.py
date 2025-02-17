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

"""Contains the Name classes."""
from typing import TYPE_CHECKING, Dict, Optional

from avro.constants import VALID_TYPES

if TYPE_CHECKING:
    from avro.schema import NamedSchema

import re
import warnings

import avro.errors

# The name portion of a fullname, record field names, and enum symbols must:
# start with [A-Za-z_]
# subsequently contain only [A-Za-z0-9_]
_BASE_NAME_PATTERN = re.compile(r"(?:^|\.)[A-Za-z_][A-Za-z0-9_]*$")


def validate_basename(basename: str) -> None:
    """Raise InvalidName if the given basename is not a valid name."""
    if not _BASE_NAME_PATTERN.search(basename):
        raise avro.errors.InvalidName(f"{basename!s} is not a valid Avro name because it does not match the pattern {_BASE_NAME_PATTERN.pattern!s}")


def _validate_fullname(fullname: str) -> None:
    for name in fullname.split("."):
        validate_basename(name)


class Name:
    """Class to describe Avro name."""

    _full: Optional[str] = None

    def __init__(self, name_attr: Optional[str] = None, space_attr: Optional[str] = None, default_space: Optional[str] = None) -> None:
        """The fullname is determined in one of the following ways:

        - A name and namespace are both specified. For example, one might use "name": "X",
            "namespace": "org.foo" to indicate the fullname org.foo.X.
        - A fullname is specified. If the name specified contains a dot,
            then it is assumed to be a fullname, and any namespace also specified is ignored.
            For example, use "name": "org.foo.X" to indicate the fullname org.foo.X.
        - A name only is specified, i.e., a name that contains no dots.
            In this case the namespace is taken from the most tightly enclosing schema or protocol.
            For example, if "name": "X" is specified, and this occurs within a field of
            the record definition of org.foo.Y, then the fullname is org.foo.X.
            If there is no enclosing namespace then the null namespace is used.

        References to previously defined names are as in the latter two cases above:
        if they contain a dot they are a fullname,
        if they do not contain a dot, the namespace is the namespace of the enclosing definition.

        @arg name_attr: name value read in schema or None.
        @arg space_attr: namespace value read in schema or None. The empty string may be used as a namespace
            to indicate the null namespace.
        @arg default_space: the current default space or None.
        """
        if name_attr is None:
            return
        if name_attr == "":
            raise avro.errors.SchemaParseException("Name must not be the empty string.")
        # The empty string may be used as a namespace to indicate the null namespace.
        self._full = (
            name_attr
            if "." in name_attr or space_attr == "" or not (space_attr or default_space)
            else f"{space_attr or default_space!s}.{name_attr!s}"
        )
        _validate_fullname(self._full)

    def __eq__(self, other: object) -> bool:
        """Equality of names is defined on the fullname and is case-sensitive."""
        return hasattr(other, "fullname") and self.fullname == getattr(other, "fullname")

    @property
    def fullname(self) -> Optional[str]:
        return self._full

    @property
    def space(self) -> Optional[str]:
        """Back out a namespace from full name."""
        full = self._full or ""
        return full.rsplit(".", 1)[0] if "." in full else None

    def get_space(self) -> Optional[str]:
        warnings.warn("Name.get_space() is deprecated in favor of Name.space")
        return self.space


class Names:
    """Track name set and default namespace during parsing."""

    names: Dict[str, "NamedSchema"]

    def __init__(self, default_namespace: Optional[str] = None) -> None:
        self.names = {}
        self.default_namespace = default_namespace

    def has_name(self, name_attr: str, space_attr: Optional[str] = None) -> bool:
        test = Name(name_attr, space_attr, self.default_namespace).fullname
        return test in self.names

    def get_name(self, name_attr: str, space_attr: Optional[str] = None) -> Optional["NamedSchema"]:
        test = Name(name_attr, space_attr, self.default_namespace).fullname
        return None if test is None else self.names.get(test)

    def prune_namespace(self, properties: Dict[str, object]) -> Dict[str, object]:
        """given a properties, return properties with namespace removed if
        it matches the own default namespace"""
        if self.default_namespace is None:
            # I have no default -- no change
            return properties

        if "namespace" not in properties:
            # he has no namespace - no change
            return properties

        if properties["namespace"] != self.default_namespace:
            # we're different - leave his stuff alone
            return properties

        # we each have a namespace and it's redundant. delete his.
        prunable = properties.copy()
        del prunable["namespace"]
        return prunable

    def add_name(self, name_attr: str, space_attr: Optional[str], new_schema: "NamedSchema") -> Name:
        """
        Add a new schema object to the name set.

        @arg name_attr: name value read in schema
        @arg space_attr: namespace value read in schema.

        @return: the Name that was just added.
        """
        to_add = Name(name_attr, space_attr, self.default_namespace)

        if to_add.fullname in VALID_TYPES:
            raise avro.errors.SchemaParseException(f"{to_add.fullname} is a reserved type name.")
        if to_add.fullname in self.names:
            raise avro.errors.SchemaParseException(f'The name "{to_add.fullname}" is already in use.')
        if to_add.fullname is None:
            raise avro.errors.SchemaParseException(f'The name built from "{space_attr}.{name_attr}" is None')

        self.names[to_add.fullname] = new_schema
        return to_add
