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

import csv
import io
import json
import operator
import os
import os.path
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import avro.datafile
import avro.io
import avro.schema

NUM_RECORDS = 7


SCHEMA = json.dumps(
    {
        "namespace": "test.avro",
        "name": "LooneyTunes",
        "type": "record",
        "fields": [{"name": "first", "type": "string"}, {"name": "last", "type": "string"}, {"name": "type", "type": "string"}],
    }
)

LOONIES = (
    ("daffy", "duck", "duck"),
    ("bugs", "bunny", "bunny"),
    ("tweety", "", "bird"),
    ("road", "runner", "bird"),
    ("wile", "e", "coyote"),
    ("pepe", "le pew", "skunk"),
    ("foghorn", "leghorn", "rooster"),
)


def looney_records():
    return ({"first": f, "last": l, "type": t} for f, l, t in LOONIES)


_JSON_PRETTY = """{
    "first": "daffy",
    "last": "duck",
    "type": "duck"
}"""


def gen_avro(filename):
    schema = avro.schema.parse(SCHEMA)
    with avro.datafile.DataFileWriter(Path(filename).open("wb"), avro.io.DatumWriter(), schema) as writer:
        for record in looney_records():
            writer.append(record)


def _tempfile():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pass
    return f.name


class TestCat(unittest.TestCase):
    def setUp(self):
        self.avro_file = _tempfile()
        gen_avro(self.avro_file)

    def tearDown(self):
        if os.path.isfile(self.avro_file):
            os.unlink(self.avro_file)

    def _run(self, *args, **kw):
        out = subprocess.check_output([sys.executable, "-m", "avro", "cat", self.avro_file] + list(args)).decode()
        return out if kw.get("raw") else out.splitlines()

    def test_print(self):
        return len(self._run()) == NUM_RECORDS

    def test_filter(self):
        return len(self._run("--filter", "r['type']=='bird'")) == 2

    def test_skip(self):
        skip = 3
        return len(self._run("--skip", str(skip))) == NUM_RECORDS - skip

    def test_csv(self):
        reader = csv.reader(io.StringIO(self._run("-f", "csv", raw=True)))
        self.assertEqual(len(list(reader)), NUM_RECORDS)

    def test_csv_header(self):
        r = {"type": "duck", "last": "duck", "first": "daffy"}
        out = self._run("-f", "csv", "--header", raw=True)
        io_ = io.StringIO(out)
        reader = csv.DictReader(io_)
        self.assertEqual(next(reader), r)

    def test_print_schema(self):
        out = self._run("--print-schema", raw=True)
        self.assertEqual(json.loads(out)["namespace"], "test.avro")

    def test_help(self):
        # Just see we have these
        self._run("-h")
        self._run("--help")

    def test_json_pretty(self):
        out = self._run("--format", "json-pretty", "-n", "1", raw=1)
        self.assertEqual(out.strip(), _JSON_PRETTY.strip())

    def test_version(self):
        subprocess.check_output([sys.executable, "-m", "avro", "cat", "--version"])

    def test_files(self):
        out = self._run(self.avro_file)
        self.assertEqual(len(out), 2 * NUM_RECORDS)

    def test_fields(self):
        # One field selection (no comma)
        out = self._run("--fields", "last")
        self.assertEqual(json.loads(out[0]), {"last": "duck"})

        # Field selection (with comma and space)
        out = self._run("--fields", "first, last")
        self.assertEqual(json.loads(out[0]), {"first": "daffy", "last": "duck"})

        # Empty fields should get all
        out = self._run("--fields", "")
        self.assertEqual(json.loads(out[0]), {"first": "daffy", "last": "duck", "type": "duck"})

        # Non existing fields are ignored
        out = self._run("--fields", "first,last,age")
        self.assertEqual(json.loads(out[0]), {"first": "daffy", "last": "duck"})


class TestWrite(unittest.TestCase):
    def setUp(self):
        self.json_file = _tempfile() + ".json"
        with Path(self.json_file).open("w") as fo:
            for record in looney_records():
                json.dump(record, fo)
                fo.write("\n")

        self.csv_file = _tempfile() + ".csv"
        get = operator.itemgetter("first", "last", "type")
        with Path(self.csv_file).open("w") as fo:
            write = csv.writer(fo).writerow
            for record in looney_records():
                write(get(record))

        self.schema_file = _tempfile()
        Path(self.schema_file).write_text(SCHEMA)

    def tearDown(self):
        for filename in (self.csv_file, self.json_file, self.schema_file):
            try:
                os.unlink(filename)
            except OSError:  # pragma: no coverage
                continue

    def _run(self, *args, **kw):
        args = [sys.executable, "-m", "avro", "write", "--schema", self.schema_file] + list(args)
        subprocess.check_call(args, **kw)

    def load_avro(self, filename):
        out = subprocess.check_output([sys.executable, "-m", "avro", "cat", filename]).decode()
        return [json.loads(o) for o in out.splitlines()]

    def test_version(self):
        subprocess.check_call([sys.executable, "-m", "avro", "write", "--version"])

    def format_check(self, format, filename):
        tmp = _tempfile()
        try:
            with Path(tmp).open("w") as fo:  # standard io are always text, never binary
                self._run(filename, "-f", format, stdout=fo)
            records = self.load_avro(tmp)
        finally:
            try:
                os.unlink(tmp)
            except IOError:  # TODO: Move this to test teardown.
                pass
        self.assertEqual(len(records), NUM_RECORDS)
        self.assertEqual(records[0]["first"], "daffy")

    def test_write_json(self):
        self.format_check("json", self.json_file)

    def test_write_csv(self):
        self.format_check("csv", self.csv_file)

    def test_outfile(self):
        tmp = _tempfile()
        os.unlink(tmp)
        self._run(self.json_file, "-o", tmp)

        self.assertEqual(len(self.load_avro(tmp)), NUM_RECORDS)
        os.unlink(tmp)

    def test_multi_file(self):
        tmp = _tempfile()
        with Path(tmp).open("w") as o:  # standard io are always text, never binary
            self._run(self.json_file, self.json_file, stdout=o)
        self.assertEqual(len(self.load_avro(tmp)), 2 * NUM_RECORDS)
        os.unlink(tmp)

    def test_stdin(self):
        tmp = _tempfile()
        with open(self.json_file, "r") as info:  # standard io are always text, never binary
            self._run("--input-type", "json", "-o", tmp, stdin=info)
        self.assertEqual(len(self.load_avro(tmp)), NUM_RECORDS)
        os.unlink(tmp)
