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
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import base64
import io
import json
import os
from pathlib import Path
from typing import IO, TextIO

import avro.codecs
import avro.datafile
import avro.io
import avro.schema

NULL_CODEC = avro.datafile.NULL_CODEC
CODECS_TO_VALIDATE = avro.codecs.KNOWN_CODECS.keys()

DATUM = {
    "intField": 12,
    "longField": 15234324,
    "stringField": "hey",
    "boolField": True,
    "floatField": 1234.0,
    "doubleField": -1234.0,
    "bytesField": b"12312adf",
    "nullField": None,
    "arrayField": [5.0, 0.0, 12.0],
    "mapField": {"a": {"label": "a"}, "bee": {"label": "cee"}},
    "unionField": 12.0,
    "enumField": "C",
    "fixedField": b"1019181716151413",
    "recordField": {"label": "blah", "children": [{"label": "inner", "children": []}]},
}


def gen_data(codec: str, datum_writer: avro.io.DatumWriter, interop_schema: avro.schema.Schema) -> bytes:
    with io.BytesIO() as file_, avro.datafile.DataFileWriter(file_, datum_writer, interop_schema, codec=codec) as dfw:
        dfw.append(DATUM)
        dfw.flush()
        return file_.getvalue()


def generate(schema_file: TextIO, output_path: IO) -> None:
    interop_schema = avro.schema.parse(schema_file.read())
    datum_writer = avro.io.DatumWriter()
    output = ((codec, gen_data(codec, datum_writer, interop_schema)) for codec in CODECS_TO_VALIDATE)
    if output_path.isatty():
        json.dump({codec: base64.b64encode(data).decode() for codec, data in output}, output_path)
        return
    for codec, data in output:
        if codec == NULL_CODEC:
            output_path.write(data)
            continue
        base, ext = os.path.splitext(output_path.name)
        Path(f"{base}_{codec}{ext}").write_bytes(data)


def _parse_args() -> argparse.Namespace:
    """Parse the command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("schema_path", type=argparse.FileType("r"))
    parser.add_argument(
        "output_path",
        type=argparse.FileType("wb"),
        help=(
            "Write the different codec variants to these files. "
            "Will append codec extensions to multiple files. "
            "If '-', will output base64 encoded binary"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    generate(args.schema_path, args.output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
