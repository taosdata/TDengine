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

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for Trevni files.
 *
 * This implement was modeled off
 * {@link org.apache.avro.mapreduce.AvroKeyValueInputFormat} to allow for easy
 * transition
 *
 * <p>
 * A MapReduce InputFormat that reads from Trevni container files of key/value
 * generic records.
 *
 * <p>
 * Trevni container files that container generic records with the two fields
 * 'key' and 'value' are expected. The contents of the 'key' field will be used
 * as the job input key, and the contents of the 'value' field will be used as
 * the job output value.
 * </p>
 *
 * @param <K> The type of the Trevni key to read.
 * @param <V> The type of the Trevni value to read.
 *
 *            <p>
 *            A subset schema to be read may be specified with
 *            {@link org.apache.avro.mapreduce.AvroJob#setInputKeySchema} and
 *            {@link org.apache.avro.mapreduce.AvroJob#setInputValueSchema}.
 */
public class AvroTrevniKeyValueInputFormat<K, V> extends FileInputFormat<AvroKey<K>, AvroValue<V>> {

  /** {@inheritDoc} */
  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    return new AvroTrevniKeyValueRecordReader<>();
  }

}
