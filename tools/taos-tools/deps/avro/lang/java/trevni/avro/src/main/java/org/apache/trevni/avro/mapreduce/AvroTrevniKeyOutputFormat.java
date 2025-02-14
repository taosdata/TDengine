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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * An {@link org.apache.hadoop.mapreduce.OutputFormat} that writes Avro data to
 * Trevni files.
 *
 * This implement was modeled off
 * {@link org.apache.avro.mapreduce.AvroKeyOutputFormat} to allow for easy
 * transition
 *
 * FileOutputFormat for writing Trevni container files.
 *
 * <p>
 * Since Trevni container files only contain records (not key/value pairs), this
 * output format ignores the value.
 * </p>
 *
 * @param <T> The (java) type of the Trevni data to write.
 *
 *            <p>
 *            Writes a directory of files per task, each comprising a single
 *            filesystem block. To reduce the number of files, increase the
 *            default filesystem block size for the job. Each task also requires
 *            enough memory to buffer a filesystem block.
 */
public class AvroTrevniKeyOutputFormat<T> extends FileOutputFormat<AvroKey<T>, NullWritable> {

  @Override
  public RecordWriter<AvroKey<T>, NullWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {

    return new AvroTrevniKeyRecordWriter<>(context);
  }
}
