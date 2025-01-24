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

import os
import unittest
from pathlib import Path
from typing import Optional, cast

import avro
import avro.datafile
import avro.io

_INTEROP_DATA_DIR = Path(avro.__file__).parent / "test" / "interop" / "data"


@unittest.skipUnless(os.path.exists(_INTEROP_DATA_DIR), f"{_INTEROP_DATA_DIR} does not exist")
class TestDataFileInterop(unittest.TestCase):
    def test_interop(self) -> None:
        """Test Interop"""
        datum: Optional[object] = None
        for filename in _INTEROP_DATA_DIR.iterdir():
            self.assertGreater(os.stat(filename).st_size, 0)
            base_ext = filename.stem.split("_", 1)
            if len(base_ext) < 2 or base_ext[1] not in avro.codecs.KNOWN_CODECS:
                print(f"SKIPPING {filename} due to an unsupported codec\n")
                continue
            i = None
            with self.subTest(filename=filename), avro.datafile.DataFileReader(filename.open("rb"), avro.io.DatumReader()) as dfr:
                for i, datum in enumerate(cast(avro.datafile.DataFileReader, dfr), 1):
                    self.assertIsNotNone(datum)
                self.assertIsNotNone(i)


if __name__ == "__main__":
    unittest.main()
