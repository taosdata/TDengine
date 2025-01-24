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

"""Contains Constants for Python Avro"""


DATE = "date"
DECIMAL = "decimal"
TIMESTAMP_MICROS = "timestamp-micros"
TIMESTAMP_MILLIS = "timestamp-millis"
TIME_MICROS = "time-micros"
TIME_MILLIS = "time-millis"
UUID = "uuid"

SUPPORTED_LOGICAL_TYPE = [
    DATE,
    DECIMAL,
    TIMESTAMP_MICROS,
    TIMESTAMP_MILLIS,
    TIME_MICROS,
    TIME_MILLIS,
    UUID,
]

PRIMITIVE_TYPES = (
    "null",
    "boolean",
    "string",
    "bytes",
    "int",
    "long",
    "float",
    "double",
)

NAMED_TYPES = (
    "fixed",
    "enum",
    "record",
    "error",
)

VALID_TYPES = PRIMITIVE_TYPES + NAMED_TYPES + ("array", "map", "union", "request", "error_union")
