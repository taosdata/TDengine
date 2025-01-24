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

package org.apache.avro.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

/** An {@link org.apache.hadoop.mapred.InputFormat} for sequence files. */
public class SequenceFileInputFormat<K, V> extends FileInputFormat<AvroWrapper<Pair<K, V>>, NullWritable> {

  @Override
  public RecordReader<AvroWrapper<Pair<K, V>>, NullWritable> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());
    return new SequenceFileRecordReader<>(job, (FileSplit) split);
  }

}
