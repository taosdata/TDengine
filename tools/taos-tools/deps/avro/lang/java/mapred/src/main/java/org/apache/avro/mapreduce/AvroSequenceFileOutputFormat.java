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
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A sequence file output format that knows how to write AvroKeys and AvroValues
 * in addition to Writables.
 *
 * @param <K> The job output key type (may be a Writable, AvroKey).
 * @param <V> The job output value type (may be a Writable, AvroValue).
 */
public class AvroSequenceFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
  /** {@inheritDoc} */
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    // Configure compression if requested.
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    if (getCompressOutput(context)) {
      // Find the kind of compression to do.
      compressionType = getOutputCompressionType(conf);

      // Find the right codec.
      Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    // Get the path of the output file.
    Path outputFile = getDefaultWorkFile(context, "");
    FileSystem fs = outputFile.getFileSystem(conf);

    // Configure the writer.
    AvroSequenceFile.Writer.Options options = new AvroSequenceFile.Writer.Options().withFileSystem(fs)
        .withConfiguration(conf).withOutputPath(outputFile).withKeyClass(context.getOutputKeyClass())
        .withValueClass(context.getOutputValueClass()).withProgressable(context).withCompressionType(compressionType)
        .withCompressionCodec(codec);
    Schema keySchema = AvroJob.getOutputKeySchema(conf);
    if (null != keySchema) {
      options.withKeySchema(keySchema);
    }
    Schema valueSchema = AvroJob.getOutputValueSchema(conf);
    if (null != valueSchema) {
      options.withValueSchema(valueSchema);
    }
    final SequenceFile.Writer out = AvroSequenceFile.createWriter(options);

    return new RecordWriter<K, V>() {
      @Override
      public void write(K key, V value) throws IOException {
        out.append(key, value);
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException {
        out.close();
      }
    };
  }

  /**
   * Sets the type of compression for the output sequence file.
   *
   * @param job             The job configuration.
   * @param compressionType The compression type for the target sequence file.
   */
  public static void setOutputCompressionType(Job job, CompressionType compressionType) {
    setCompressOutput(job, true);
    job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE, compressionType.name());
  }

  /**
   * Gets type of compression for the output sequence file.
   *
   * @param conf The job configuration.
   * @return The compression type.
   */
  public static CompressionType getOutputCompressionType(Configuration conf) {
    String typeName = conf.get(FileOutputFormat.COMPRESS_TYPE);
    if (typeName != null) {
      return CompressionType.valueOf(typeName);
    }
    return SequenceFile.getDefaultCompressionType(conf);
  }
}
