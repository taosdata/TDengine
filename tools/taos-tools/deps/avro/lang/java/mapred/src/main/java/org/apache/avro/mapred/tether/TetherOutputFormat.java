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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;

/** An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files. */
class TetherOutputFormat extends FileOutputFormat<TetherData, NullWritable> {

  /** Enable output compression using the deflate codec and specify its level. */
  public static void setDeflateLevel(JobConf job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.setInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, level);
  }

  @SuppressWarnings("unchecked")
  @Override
  public RecordWriter<TetherData, NullWritable> getRecordWriter(FileSystem ignore, JobConf job, String name,
      Progressable prog) throws IOException {

    Schema schema = AvroJob.getOutputSchema(job);

    final DataFileWriter writer = new DataFileWriter(new GenericDatumWriter());

    if (FileOutputFormat.getCompressOutput(job)) {
      int level = job.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, CodecFactory.DEFAULT_DEFLATE_LEVEL);
      writer.setCodec(CodecFactory.deflateCodec(level));
    }

    Path path = FileOutputFormat.getTaskOutputPath(job, name + AvroOutputFormat.EXT);
    writer.create(schema, path.getFileSystem(job).create(path));

    return new RecordWriter<TetherData, NullWritable>() {
      @Override
      public void write(TetherData datum, NullWritable ignore) throws IOException {
        writer.appendEncoded(datum.buffer());
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        writer.close();
      }
    };
  }

}
