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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroOutputFormat;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} for tethered Avro input.
 *
 * By default, when pointed at a directory, this will silently skip over any
 * files in it that do not have .avro extension. To instead include all files,
 * set the avro.mapred.ignore.inputs.without.extension property to false.
 */
class TetherInputFormat extends FileInputFormat<TetherData, NullWritable> {

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    if (job.getBoolean(AvroInputFormat.IGNORE_FILES_WITHOUT_EXTENSION_KEY,
        AvroInputFormat.IGNORE_INPUTS_WITHOUT_EXTENSION_DEFAULT)) {
      List<FileStatus> result = new ArrayList<>();
      for (FileStatus file : super.listStatus(job))
        if (file.getPath().getName().endsWith(AvroOutputFormat.EXT))
          result.add(file);
      return result.toArray(new FileStatus[0]);
    } else {
      return super.listStatus(job);
    }
  }

  @Override
  public RecordReader<TetherData, NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    reporter.setStatus(split.toString());
    return new TetherRecordReader(job, (FileSplit) split);
  }

}
