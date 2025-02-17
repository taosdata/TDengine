/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.mapred.tether;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.mapred.AvroJob;

class TetherMapRunner extends MapRunner<TetherData, NullWritable, TetherData, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(TetherMapRunner.class);

  private JobConf job;
  private TetheredProcess process;

  public void configure(JobConf job) {
    this.job = job;
  }

  @SuppressWarnings("unchecked")
  public void run(RecordReader<TetherData, NullWritable> recordReader,
      OutputCollector<TetherData, NullWritable> collector, Reporter reporter) throws IOException {
    try {
      // start tethered process
      process = new TetheredProcess(job, collector, reporter);

      // configure it
      LOG.info("send configure to subprocess for map task");
      process.inputClient.configure(TaskType.MAP, job.get(AvroJob.INPUT_SCHEMA),
          AvroJob.getMapOutputSchema(job).toString());

      LOG.info("send partitions to subprocess for map task");
      process.inputClient.partitions(job.getNumReduceTasks());

      // run map
      Counter inputRecordCounter = reporter.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS");
      TetherData data = new TetherData();
      while (recordReader.next(data, NullWritable.get())) {
        process.inputClient.input(data.buffer(), data.count());
        inputRecordCounter.increment(data.count() - 1);
        if (process.outputService.isFinished())
          break;
      }
      LOG.info("send complete to subprocess for map task");
      process.inputClient.complete();

      // wait for completion
      if (process.outputService.waitForFinish())
        throw new IOException("Task failed: " + process.outputService.error());

    } catch (Throwable t) { // send abort
      LOG.warn("Task failed", t);
      process.inputClient.abort();
      throw new IOException("Task failed: " + t, t);

    } finally { // clean up
      if (process != null)
        process.close();
    }
  }

}
