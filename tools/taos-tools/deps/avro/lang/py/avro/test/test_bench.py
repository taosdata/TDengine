#!/usr/bin/env python

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
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import platform
import random
import string
import tempfile
import timeit
import unittest
import unittest.mock
from pathlib import Path
from typing import Mapping, Sequence, cast

import avro.datafile
import avro.io
import avro.schema
from avro.utils import randbytes

TYPES = ("A", "CNAME")
SCHEMA = cast(
    avro.schema.RecordSchema,
    avro.schema.parse(
        json.dumps(
            {
                "type": "record",
                "name": "Query",
                "fields": [
                    {"name": "query", "type": "string"},
                    {"name": "response", "type": "string"},
                    {"name": "type", "type": "string", "default": "A"},
                ],
            }
        )
    ),
)
READER = avro.io.DatumReader(SCHEMA)
WRITER = avro.io.DatumWriter(SCHEMA)
NUMBER_OF_TESTS = 10000
MAX_WRITE_SECONDS = 10 if platform.python_implementation() == "PyPy" else 3
MAX_READ_SECONDS = 10 if platform.python_implementation() == "PyPy" else 3


class TestBench(unittest.TestCase):
    def test_minimum_speed(self) -> None:
        with tempfile.NamedTemporaryFile(suffix="avr") as temp_:
            pass
        temp = Path(temp_.name)
        self.assertLess(
            time_writes(temp, NUMBER_OF_TESTS),
            MAX_WRITE_SECONDS,
            f"Took longer than {MAX_WRITE_SECONDS} second(s) to write the test file with {NUMBER_OF_TESTS} values.",
        )
        self.assertLess(
            time_read(temp),
            MAX_READ_SECONDS,
            f"Took longer than {MAX_READ_SECONDS} second(s) to read the test file with {NUMBER_OF_TESTS} values.",
        )


def rand_name() -> str:
    return "".join(random.sample(string.ascii_lowercase, 15))


def rand_ip() -> str:
    return ".".join(map(str, randbytes(4)))


def picks(n) -> Sequence[Mapping[str, str]]:
    return [{"query": rand_name(), "response": rand_ip(), "type": random.choice(TYPES)} for _ in range(n)]


def time_writes(path: Path, number: int) -> float:
    with avro.datafile.DataFileWriter(path.open("wb"), WRITER, SCHEMA) as dw:
        globals_ = {"dw": dw, "picks": picks(number)}
        return timeit.timeit("dw.append(next(p))", number=number, setup="p=iter(picks)", globals=globals_)


def time_read(path: Path) -> float:
    """
    Time how long it takes to read the file written in the `write` function.
    We only do this once, because the size of the file is defined by the number sent to `write`.
    """
    with avro.datafile.DataFileReader(path.open("rb"), READER) as dr:
        return timeit.timeit("tuple(dr)", number=1, globals={"dr": dr})


def parse_args() -> argparse.Namespace:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Benchmark writing some random avro.")
    parser.add_argument(
        "--number",
        "-n",
        type=int,
        default=getattr(timeit, "default_number", 1000000),
        help="how many times to run",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover
    args = parse_args()
    with tempfile.NamedTemporaryFile(suffix=".avr") as temp_:
        pass
    temp = Path(temp_.name)

    print(f"Using file {temp.name}")
    print(f"Writing: {time_writes(temp, args.number)}")
    print(f"Reading: {time_read(temp)}")


if __name__ == "__main__":  # pragma: no cover
    main()
