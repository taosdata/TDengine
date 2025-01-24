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

import io
import os
import subprocess
import sys
import time
import unittest

import avro.io
import avro.test.mock_tether_parent
import avro.test.word_count_task
import avro.tether.tether_task
import avro.tether.util


class TestTetherTask(unittest.TestCase):
    """
    TODO: We should validate the the server response by looking at stdout
    """

    def test_tether_task(self) -> None:
        """
        Test that the tether_task is working. We run the mock_tether_parent in a separate
        subprocess
        """
        task = avro.test.word_count_task.WordCountTask()
        proc = None
        pyfile = avro.test.mock_tether_parent.__file__
        server_port = avro.tether.util.find_port()
        input_port = avro.tether.util.find_port()
        try:
            # launch the server in a separate process
            proc = subprocess.Popen([sys.executable, pyfile, "start_server", str(server_port)])

            print(f"Mock server started process pid={proc.pid}")

            # Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
            # so we give the subprocess time to start up
            time.sleep(1)
            task.open(input_port, clientPort=server_port)

            # TODO: We should validate that open worked by grabbing the STDOUT of the subproces
            # and ensuring that it outputted the correct message.

            # ***************************************************************
            # Test the mapper
            if avro.tether.tether_task.TaskType is None:
                self.fail()
            task.configure(
                avro.tether.tether_task.TaskType.MAP,
                str(task.inschema),
                str(task.midschema),
            )

            # Serialize some data so we can send it to the input function
            datum = "This is a line of text"
            writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(writer)
            datum_writer = avro.io.DatumWriter(task.inschema)
            datum_writer.write(datum, encoder)

            writer.seek(0)
            data = writer.read()

            # Call input to simulate calling map
            task.input(data, 1)

            # Test the reducer
            task.configure(
                avro.tether.tether_task.TaskType.REDUCE,
                str(task.midschema),
                str(task.outschema),
            )

            # Serialize some data so we can send it to the input function
            writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(writer)
            datum_writer = avro.io.DatumWriter(task.midschema)
            datum_writer.write({"key": "word", "value": 2}, encoder)

            writer.seek(0)
            data = writer.read()

            # Call input to simulate calling reduce
            task.input(data, 1)

            task.complete()

            # try a status
            task.status("Status message")
        finally:
            # close the process
            if not (proc is None):
                proc.kill()


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
