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
There are currently no IPC tests within python, in part because there are no
servers yet available.
"""

import unittest

import avro.ipc


class TestIPC(unittest.TestCase):
    def test_placeholder(self):
        pass

    def test_server_with_path(self):
        client_with_custom_path = avro.ipc.HTTPTransceiver("apache.org", 80, "/service/article")
        self.assertEqual("/service/article", client_with_custom_path.req_resource)

        client_with_default_path = avro.ipc.HTTPTransceiver("apache.org", 80)
        self.assertEqual("/", client_with_default_path.req_resource)


if __name__ == "__main__":  # pragma: no coverage
    unittest.main()
