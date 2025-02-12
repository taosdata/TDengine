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
# See the License for the specific language governing permissions and
# limitations under the License.

import http.server
import logging
import sys
import threading
import time
import traceback
import weakref

import avro.errors
import avro.ipc
import avro.tether.tether_task
import avro.tether.util

__all__ = ["TaskRunner"]


class TaskRunnerResponder(avro.ipc.Responder):
    """
    The responder for the tethered process
    """

    def __init__(self, runner):
        """
        Param
        ----------------------------------------------------------
        runner - Instance of TaskRunner
        """
        avro.ipc.Responder.__init__(self, avro.tether.tether_task.inputProtocol)

        self.log = logging.getLogger("TaskRunnerResponder")

        # should we use weak references to avoid circular references?
        # We use weak references b\c self.runner owns this instance of TaskRunnerResponder
        if isinstance(runner, weakref.ProxyType):
            self.runner = runner
        else:
            self.runner = weakref.proxy(runner)

        self.task = weakref.proxy(runner.task)

    def invoke(self, message, request):
        try:
            if message.name == "configure":
                self.log.info("TetherTaskRunner: Received configure")
                self.task.configure(request["taskType"], request["inSchema"], request["outSchema"])
            elif message.name == "partitions":
                self.log.info("TetherTaskRunner: Received partitions")
                try:
                    self.task.partitions = request["partitions"]
                except Exception as e:
                    self.log.error("Exception occured while processing the partitions message: Message:\n%s", traceback.format_exc())
                    raise
            elif message.name == "input":
                self.log.info("TetherTaskRunner: Received input")
                self.task.input(request["data"], request["count"])
            elif message.name == "abort":
                self.log.info("TetherTaskRunner: Received abort")
                self.runner.close()
            elif message.name == "complete":
                self.log.info("TetherTaskRunner: Received complete")
                self.task.complete()
                self.task.close()
                self.runner.close()
            else:
                self.log.warning("TetherTaskRunner: Received unknown message %s", message.name)

        except Exception as e:
            self.log.error("Error occured while processing message: %s", message.name)
            e = traceback.format_exc()
            self.task.fail(e)

        return None


def HTTPHandlerGen(runner):
    """
    This is a class factory for the HTTPHandler. We need
    a factory because we need a reference to the runner

    Parameters
    -----------------------------------------------------------------
    runner - instance of the task runner
    """

    if not (isinstance(runner, weakref.ProxyType)):
        runnerref = weakref.proxy(runner)
    else:
        runnerref = runner

    class TaskRunnerHTTPHandler(http.server.BaseHTTPRequestHandler):
        """Create a handler for the parent."""

        runner = runnerref

        def __init__(self, *args, **param):
            """ """
            http.server.BaseHTTPRequestHandler.__init__(self, *args, **param)

        def do_POST(self):
            self.responder = TaskRunnerResponder(self.runner)
            call_request_reader = avro.ipc.FramedReader(self.rfile)
            call_request = call_request_reader.read_framed_message()
            resp_body = self.responder.respond(call_request)
            self.send_response(200)
            self.send_header("Content-Type", "avro/binary")
            self.end_headers()
            resp_writer = avro.ipc.FramedWriter(self.wfile)
            resp_writer.write_framed_message(resp_body)

    return TaskRunnerHTTPHandler


class TaskRunner:
    """This class ties together the server handling the requests from
    the parent process and the instance of TetherTask which actually
    implements the logic for the mapper and reducer phases
    """

    _server = None
    sthread = None
    timeout = 12  # number of seconds to wait for server to come up.

    def __init__(self, task):
        """
        Construct the runner

        Parameters
        ---------------------------------------------------------------
        task - An instance of tether task
        """
        self.log = logging.getLogger("TaskRunner:")
        if not isinstance(task, avro.tether.tether_task.TetherTask):
            raise avro.errors.AvroException("task must be an instance of tether task")
        self.task = task

    @property
    def server(self):
        for t in range(self.timeout):
            if self._server:
                return self._server
            time.sleep(1)
        raise RuntimeError("Server never started")

    def start(self, outputport=None, join=True):
        """
        Start the server

        Parameters
        -------------------------------------------------------------------
        outputport - (optional) The port on which the parent process is listening
                     for requests from the task.
                   - This will typically be supplied by an environment variable
                     we allow it to be supplied as an argument mainly for debugging
        join       - (optional) If set to fault then we don't issue a join to block
                     until the thread excecuting the server terminates.
                    This is mainly for debugging. By setting it to false,
                    we can resume execution in this thread so that we can do additional
                    testing
        """
        port = avro.tether.util.find_port()
        address = ("localhost", port)

        def thread_run(task_runner=None):
            task_runner._server = http.server.HTTPServer(address, HTTPHandlerGen(task_runner))
            task_runner._server.allow_reuse_address = True
            task_runner._server.serve_forever()

        # create a separate thread for the http server
        sthread = threading.Thread(target=thread_run, kwargs={"task_runner": self})
        sthread.start()

        self.sthread = sthread
        # This needs to run in a separate thread because serve_forever() blocks.
        self.task.open(port, clientPort=outputport)

        # wait for the other thread to finish
        if join:
            self.task.ready_for_shutdown.wait()
            self._server.shutdown()

            # should we do some kind of check to make sure it exits
            self.log.info("Shutdown the logger")
            # shutdown the logging
            logging.shutdown()

    def close(self):
        """
        Handler for the close message
        """
        self.task.close()


if __name__ == "__main__":
    # TODO::Make the logging level a parameter we can set
    # logging.basicConfig(level=logging.INFO,filename='/tmp/log',filemode='w')
    logging.basicConfig(level=logging.INFO)

    try:
        fullcls = sys.argv[1]
    except IndexError:
        raise avro.errors.UsageError("Usage: tether_task_runner task_package.task_module.TaskClass")

    mod, cname = fullcls.rsplit(".", 1)

    logging.info(f"tether_task_runner.__main__: Task: {fullcls}")

    modobj = __import__(mod, fromlist=cname)

    taskcls = getattr(modobj, cname)
    task = taskcls()

    try:
        runner = TaskRunner(task=task)
    except avro.errors.AvroException as e:
        raise avro.errors.UsageError(e)

    runner.start()
