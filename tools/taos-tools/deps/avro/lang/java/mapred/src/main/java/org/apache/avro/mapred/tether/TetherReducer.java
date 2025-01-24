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
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.avro.mapred.AvroJob;

class TetherReducer implements Reducer<TetherData, NullWritable, TetherData, NullWritable> {

  private JobConf job;
  private TetheredProcess process;
  private boolean error;

  @Override
  public void configure(JobConf job) {
    this.job = job;
  }

  @Override
  public void reduce(TetherData datum, Iterator<NullWritable> ignore,
      OutputCollector<TetherData, NullWritable> collector, Reporter reporter) throws IOException {
    try {
      if (process == null) {
        process = new TetheredProcess(job, collector, reporter);
        process.inputClient.configure(TaskType.REDUCE, AvroJob.getMapOutputSchema(job).toString(),
            AvroJob.getOutputSchema(job).toString());
      }
      process.inputClient.input(datum.buffer(), datum.count());
    } catch (IOException e) {
      error = true;
      throw e;
    } catch (Exception e) {
      error = true;
      throw new IOException(e);
    }
  }

  /**
   * Handle the end of the input by closing down the application.
   */
  @Override
  public void close() throws IOException {
    if (process == null)
      return;
    try {
      if (error)
        process.inputClient.abort();
      else
        process.inputClient.complete();
      process.outputService.waitForFinish();
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      process.close();
    }
  }
}
