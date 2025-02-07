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
import logging
import os
import subprocess
import sys
import time
import unittest

import avro.io
import avro.test.mock_tether_parent
import avro.test.word_count_task
import avro.tether.tether_task
import avro.tether.tether_task_runner
import avro.tether.util


class TestTetherTaskRunner(unittest.TestCase):
    """unit test for a tethered task runner."""

    def test1(self):
        # set the logging level to debug so that debug messages are printed
        logging.basicConfig(level=logging.DEBUG)

        proc = None
        try:
            # launch the server in a separate process
            parent_port = avro.tether.util.find_port()

            pyfile = avro.test.mock_tether_parent.__file__
            proc = subprocess.Popen([sys.executable, pyfile, "start_server", f"{parent_port}"])
            input_port = avro.tether.util.find_port()

            print(f"Mock server started process pid={proc.pid}")
            # Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
            # so we give the subprocess time to start up
            time.sleep(1)

            runner = avro.tether.tether_task_runner.TaskRunner(avro.test.word_count_task.WordCountTask())

            runner.start(outputport=parent_port, join=False)
            # Test sending various messages to the server and ensuring they are processed correctly
            requestor = avro.tether.tether_task.HTTPRequestor(
                "localhost",
                runner.server.server_address[1],
                avro.tether.tether_task.inputProtocol,
            )

            # TODO: We should validate that open worked by grabbing the STDOUT of the subproces
            # and ensuring that it outputted the correct message.

            # Test the mapper
            requestor.request(
                "configure",
                {
                    "taskType": avro.tether.tether_task.TaskType.MAP,
                    "inSchema": str(runner.task.inschema),
                    "outSchema": str(runner.task.midschema),
                },
            )

            # Serialize some data so we can send it to the input function
            datum = "This is a line of text"
            writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(writer)
            datum_writer = avro.io.DatumWriter(runner.task.inschema)
            datum_writer.write(datum, encoder)

            writer.seek(0)
            data = writer.read()

            # Call input to simulate calling map
            requestor.request("input", {"data": data, "count": 1})

            # Test the reducer
            requestor.request(
                "configure",
                {
                    "taskType": avro.tether.tether_task.TaskType.REDUCE,
                    "inSchema": str(runner.task.midschema),
                    "outSchema": str(runner.task.outschema),
                },
            )

            # Serialize some data so we can send it to the input function
            datum = {"key": "word", "value": 2}
            writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(writer)
            datum_writer = avro.io.DatumWriter(runner.task.midschema)
            datum_writer.write(datum, encoder)

            writer.seek(0)
            data = writer.read()

            # Call input to simulate calling reduce
            requestor.request("input", {"data": data, "count": 1})

            requestor.request("complete", {})

            runner.task.ready_for_shutdown.wait()
            runner.server.shutdown()
            # time.sleep(2)
            # runner.server.shutdown()

            sthread = runner.sthread

            # Possible race condition?
            time.sleep(1)

            # make sure the other thread terminated
            self.assertFalse(sthread.is_alive())

            # shutdown the logging
            logging.shutdown()

        finally:
            # close the process
            if not (proc is None):
                proc.kill()

    def test2(self):
        """
        In this test we want to make sure that when we run "tether_task_runner.py"
        as our main script everything works as expected. We do this by using subprocess to run it
        in a separate thread.
        """
        proc = None

        runnerproc = None
        try:
            # launch the server in a separate process
            parent_port = avro.tether.util.find_port()

            pyfile = avro.test.mock_tether_parent.__file__
            proc = subprocess.Popen([sys.executable, pyfile, "start_server", f"{parent_port}"])

            # Possible race condition? when we start tether_task_runner it will call
            # open tries to connect to the subprocess before the subprocess is fully started
            # so we give the subprocess time to start up
            time.sleep(1)

            # start the tether_task_runner in a separate process
            runnerproc = subprocess.Popen(
                [
                    sys.executable,
                    avro.tether.tether_task_runner.__file__,
                    "avro.test.word_count_task.WordCountTask",
                ],
                env={"AVRO_TETHER_OUTPUT_PORT": f"{parent_port}", "PYTHONPATH": ":".join(sys.path)},
            )

            # possible race condition wait for the process to start
            time.sleep(1)

            print(f"Mock server started process pid={proc.pid}")
            # Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
            # so we give the subprocess time to start up
            time.sleep(1)

        finally:
            # close the process
            if not (runnerproc is None):
                runnerproc.kill()

            if not (proc is None):
                proc.kill()


if __name__ == ("__main__"):  # pragma: no coverage
    unittest.main()
