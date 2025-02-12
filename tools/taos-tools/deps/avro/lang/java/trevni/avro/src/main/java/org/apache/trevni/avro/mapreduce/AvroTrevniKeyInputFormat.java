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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for Trevni files.
 *
 * This implement was modeled off
 * {@link org.apache.avro.mapreduce.AvroKeyInputFormat} to allow for easy
 * transition
 *
 * A MapReduce InputFormat that can handle Trevni container files.
 *
 * <p>
 * Keys are AvroKey wrapper objects that contain the Trevni data. Since Trevni
 * container files store only records (not key/value pairs), the value from this
 * InputFormat is a NullWritable.
 * </p>
 *
 * <p>
 * A subset schema to be read may be specified with
 * {@link org.apache.avro.mapreduce.AvroJob#setInputKeySchema}.
 */
public class AvroTrevniKeyInputFormat<T> extends FileInputFormat<AvroKey<T>, NullWritable> {

  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    return new AvroTrevniKeyRecordReader<>();
  }

}
