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
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MapReduce InputFormat that reads from Avro container files of key/value
 * generic records.
 *
 * <p>
 * Avro container files that container generic records with the two fields 'key'
 * and 'value' are expected. The contents of the 'key' field will be used as the
 * job input key, and the contents of the 'value' field will be used as the job
 * output value.
 * </p>
 *
 * @param <K> The type of the Avro key to read.
 * @param <V> The type of the Avro value to read.
 */
public class AvroKeyValueInputFormat<K, V> extends FileInputFormat<AvroKey<K>, AvroValue<V>> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyValueInputFormat.class);

  /** {@inheritDoc} */
  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Schema keyReaderSchema = AvroJob.getInputKeySchema(context.getConfiguration());
    if (null == keyReaderSchema) {
      LOG.warn("Key reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
      LOG.info("Using a key reader schema equal to the writer schema.");
    }
    Schema valueReaderSchema = AvroJob.getInputValueSchema(context.getConfiguration());
    if (null == valueReaderSchema) {
      LOG.warn("Value reader schema was not set. Use AvroJob.setInputValueSchema() if desired.");
      LOG.info("Using a value reader schema equal to the writer schema.");
    }
    return new AvroKeyValueRecordReader<>(keyReaderSchema, valueReaderSchema);
  }
}
