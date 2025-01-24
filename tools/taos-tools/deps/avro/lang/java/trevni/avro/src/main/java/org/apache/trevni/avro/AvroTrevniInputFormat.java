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

package org.apache.trevni.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.avro.reflect.ReflectData;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} for Trevni files.
 *
 * <p>
 * A subset schema to be read may be specified with
 * {@link AvroJob#setInputSchema(JobConf,Schema)}.
 */
public class AvroTrevniInputFormat<T> extends FileInputFormat<AvroWrapper<T>, NullWritable> {

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    List<FileStatus> result = new ArrayList<>();
    job.setBoolean("mapred.input.dir.recursive", true);
    for (FileStatus file : super.listStatus(job))
      if (file.getPath().getName().endsWith(AvroTrevniOutputFormat.EXT))
        result.add(file);
    return result.toArray(new FileStatus[0]);
  }

  @Override
  public RecordReader<AvroWrapper<T>, NullWritable> getRecordReader(InputSplit split, final JobConf job,
      Reporter reporter) throws IOException {
    final FileSplit file = (FileSplit) split;
    reporter.setStatus(file.toString());

    final AvroColumnReader.Params params = new AvroColumnReader.Params(new HadoopInput(file.getPath(), job));
    params.setModel(ReflectData.get());
    if (job.get(AvroJob.INPUT_SCHEMA) != null)
      params.setSchema(AvroJob.getInputSchema(job));

    return new RecordReader<AvroWrapper<T>, NullWritable>() {
      private AvroColumnReader<T> reader = new AvroColumnReader<>(params);
      private float rows = reader.getRowCount();
      private long row;

      @Override
      public AvroWrapper<T> createKey() {
        return new AvroWrapper<>(null);
      }

      @Override
      public NullWritable createValue() {
        return NullWritable.get();
      }

      @Override
      public boolean next(AvroWrapper<T> wrapper, NullWritable ignore) throws IOException {
        if (!reader.hasNext())
          return false;
        wrapper.datum(reader.next());
        row++;
        return true;
      }

      @Override
      public float getProgress() throws IOException {
        return row / rows;
      }

      @Override
      public long getPos() throws IOException {
        return row;
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };

  }

}
