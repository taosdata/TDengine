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

import java.nio.ByteBuffer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TetherOutputService implements OutputProtocol {
  private Reporter reporter;
  private OutputCollector<TetherData, NullWritable> collector;
  private int inputPort;
  private boolean complete;
  private String error;

  private static final Logger LOG = LoggerFactory.getLogger(TetherOutputService.class);

  // timeout when waiting for messages in seconds.
  // what is a good value?
  public static final long TIMEOUT = 10 * 1000;

  public TetherOutputService(OutputCollector<TetherData, NullWritable> collector, Reporter reporter) {
    this.reporter = reporter;
    this.collector = collector;
  }

  @Override
  public synchronized void configure(int inputPort) {
    LOG.info("got input port from child: inputport=" + inputPort);
    this.inputPort = inputPort;
    notify();
  }

  public synchronized int inputPort() throws Exception {
    if (inputPort == 0) {
      LOG.info("waiting for input port from child");
      wait(TIMEOUT);
    }

    if (inputPort == 0) {
      LOG.error(
          "Parent process timed out waiting for subprocess to send input port. Check the job log files for more info.");
      throw new Exception("Parent process timed out waiting for subprocess to send input port");
    }
    return inputPort;
  }

  @Override
  public void output(ByteBuffer datum) {
    try {
      collector.collect(new TetherData(datum), NullWritable.get());
    } catch (Throwable e) {
      LOG.warn("Error: " + e, e);
      synchronized (this) {
        error = e.toString();
      }
    }
  }

  @Override
  public void outputPartitioned(int partition, ByteBuffer datum) {
    TetherPartitioner.setNextPartition(partition);
    output(datum);
  }

  @Override
  public void status(String message) {
    reporter.setStatus(message.toString());
  }

  @Override
  public void count(String group, String name, long amount) {
    reporter.getCounter(group.toString(), name.toString()).increment(amount);
  }

  @Override
  public synchronized void fail(String message) {
    LOG.warn("Failing: " + message);
    error = message;
    notify();
  }

  @Override
  public synchronized void complete() {
    LOG.info("got task complete");
    complete = true;
    notify();
  }

  public synchronized boolean isFinished() {
    return complete || (error != null);
  }

  public String error() {
    return error;
  }

  public synchronized boolean waitForFinish() throws InterruptedException {
    while (!isFinished())
      wait();
    return error != null;
  }

}
