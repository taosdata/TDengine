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
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An {@link org.apache.hadoop.mapreduce.OutputFormat} that writes Avro data to
 * Trevni files.
 *
 * This implement was modeled off
 * {@link org.apache.avro.mapreduce.AvroKeyValueOutputFormat} to allow for easy
 * transition
 *
 * * FileOutputFormat for writing Trevni container files of key/value pairs.
 *
 * <p>
 * Since Trevni container files can only contain records (not key/value pairs),
 * this output format puts the key and value into an Avro generic record with
 * two fields, named 'key' and 'value'.
 * </p>
 *
 * <p>
 * The keys and values given to this output format may be Avro objects wrapped
 * in <code>AvroKey</code> or <code>AvroValue</code> objects. The basic Writable
 * types are also supported (e.g., IntWritable, Text); they will be converted to
 * their corresponding Avro types.
 * </p>
 *
 * @param <K> The type of key. If an Avro type, it must be wrapped in an
 *            <code>AvroKey</code>.
 * @param <V> The type of value. If an Avro type, it must be wrapped in an
 *            <code>AvroValue</code>.
 *
 *            <p>
 *            Writes a directory of files per task, each comprising a single
 *            filesystem block. To reduce the number of files, increase the
 *            default filesystem block size for the job. Each task also requires
 *            enough memory to buffer a filesystem block.
 */
public class AvroTrevniKeyValueOutputFormat<K, V> extends FileOutputFormat<AvroKey<K>, AvroValue<V>> {

  /** {@inheritDoc} */
  @Override
  public RecordWriter<AvroKey<K>, AvroValue<V>> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {

    return new AvroTrevniKeyValueRecordWriter<>(context);
  }
}
