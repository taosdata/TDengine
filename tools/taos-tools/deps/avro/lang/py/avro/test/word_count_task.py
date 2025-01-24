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

import logging

import avro.tether.tether_task

__all__ = ["WordCountTask"]


# TODO::Make the logging level a parameter we can set
# logging.basicConfig(level=logging.INFO)
class WordCountTask(avro.tether.tether_task.TetherTask):
    """
    Implements the mapper and reducer for the word count example
    """

    def __init__(self):
        """ """

        inschema = """{"type":"string"}"""
        midschema = """{"type":"record", "name":"Pair","namespace":"org.apache.avro.mapred","fields":[
              {"name":"key","type":"string"},
              {"name":"value","type":"long","order":"ignore"}]
              }"""
        outschema = midschema
        avro.tether.tether_task.TetherTask.__init__(self, inschema, midschema, outschema)

        # keep track of the partial sums of the counts
        self.psum = 0

    def map(self, record, collector):
        """Implement the mapper for the word count example

        Parameters
        ----------------------------------------------------------------------------
        record - The input record
        collector - The collector to collect the output
        """

        words = record.split()

        for w in words:
            logging.info("WordCountTask.Map: word=%s", w)
            collector.collect({"key": w, "value": 1})

    def reduce(self, record, collector):
        """Called with input values to generate reducer output. Inputs are sorted by the mapper
        key.

        The reduce function is invoked once for each value belonging to a given key outputted
        by the mapper.

        Parameters
        ----------------------------------------------------------------------------
        record - The mapper output
        collector - The collector to collect the output
        """

        self.psum += record["value"]

    def reduceFlush(self, record, collector):
        """
        Called with the last intermediate value in each equivalence run.
        In other words, reduceFlush is invoked once for each key produced in the reduce
        phase. It is called after reduce has been invoked on each value for the given key.

        Parameters
        ------------------------------------------------------------------
        record - the last record on which reduce was invoked.
        """

        # collect the current record
        logging.info("WordCountTask.reduceFlush key=%s value=%s", record["key"], self.psum)

        collector.collect({"key": record["key"], "value": self.psum})

        # reset the sum
        self.psum = 0
