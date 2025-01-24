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

package org.apache.trevni.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes Trevni records to an Trevni container file output stream.
 *
 * @param <T> The Java type of the Trevni data to write.
 */
public class AvroTrevniKeyRecordWriter<T> extends AvroTrevniRecordWriterBase<AvroKey<T>, NullWritable, T> {

  /**
   * Constructor.
   * 
   * @param context The TaskAttempContext to supply the writer with information
   *                form the job configuration
   */
  public AvroTrevniKeyRecordWriter(TaskAttemptContext context) throws IOException {
    super(context);
  }

  /** {@inheritDoc} */
  @Override
  public void write(AvroKey<T> key, NullWritable value) throws IOException, InterruptedException {
    writer.write(key.datum());
    if (writer.sizeEstimate() >= blockSize) // block full
      flush();
  }

  /** {@inheritDoc} */
  @Override
  protected Schema initSchema(TaskAttemptContext context) {
    boolean isMapOnly = context.getNumReduceTasks() == 0;
    return isMapOnly ? AvroJob.getMapOutputKeySchema(context.getConfiguration())
        : AvroJob.getOutputKeySchema(context.getConfiguration());
  }
}
