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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MapReduce InputFormat that can handle Avro container files.
 *
 * <p>
 * Keys are AvroKey wrapper objects that contain the Avro data. Since Avro
 * container files store only records (not key/value pairs), the value from this
 * InputFormat is a NullWritable.
 * </p>
 */
public class AvroKeyInputFormat<T> extends FileInputFormat<AvroKey<T>, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyInputFormat.class);

  /** {@inheritDoc} */
  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Schema readerSchema = AvroJob.getInputKeySchema(context.getConfiguration());
    if (null == readerSchema) {
      LOG.warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
      LOG.info("Using a reader schema equal to the writer schema.");
    }
    return new AvroKeyRecordReader<>(readerSchema);
  }
}
