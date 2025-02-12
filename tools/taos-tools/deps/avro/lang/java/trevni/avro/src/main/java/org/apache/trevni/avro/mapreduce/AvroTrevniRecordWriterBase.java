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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

/**
 * Abstract base class for <code>RecordWriter</code>s that writes Trevni
 * container files.
 *
 * @param <K> The type of key the record writer should generate.
 * @param <V> The type of value the record wrtier should generate.
 * @param <T> The type of the entries within the Trevni container file being
 *            written.
 */
public abstract class AvroTrevniRecordWriterBase<K, V, T> extends RecordWriter<K, V> {

  /** trevni file extension */
  public final static String EXT = ".trv";

  /** prefix of job configs that we care about */
  public static final String META_PREFIX = "trevni.meta.";

  /**
   * Counter that increments as new trevni files are create because the current
   * file has exceeded the block size
   */
  protected int part = 0;

  /** Trevni file writer */
  protected AvroColumnWriter<T> writer;

  /** This will be a unique directory linked to the task */
  final Path dirPath;

  /** HDFS object */
  final FileSystem fs;

  /** Current configured blocksize */
  final long blockSize;

  /** Provided avro schema from the context */
  protected Schema schema;

  /** meta data to be stored in the output file. */
  protected ColumnFileMetaData meta;

  /**
   * Constructor.
   * 
   * @param context The TaskAttempContext to supply the writer with information
   *                form the job configuration
   */
  public AvroTrevniRecordWriterBase(TaskAttemptContext context) throws IOException {

    schema = initSchema(context);
    meta = filterMetadata(context.getConfiguration());
    writer = new AvroColumnWriter<>(schema, meta, ReflectData.get());

    Path outputPath = FileOutputFormat.getOutputPath(context);

    String dir = FileOutputFormat.getUniqueFile(context, "part", "");
    dirPath = new Path(outputPath.toString() + "/" + dir);
    fs = dirPath.getFileSystem(context.getConfiguration());
    fs.mkdirs(dirPath);

    blockSize = fs.getDefaultBlockSize(dirPath);
  }

  /**
   * Use the task context to construct a schema for writing
   * 
   * @throws IOException
   */
  abstract protected Schema initSchema(TaskAttemptContext context);

  /**
   * A Trevni flush will close the current file and prep a new writer
   * 
   * @throws IOException
   */
  public void flush() throws IOException {
    try (OutputStream out = fs.create(new Path(dirPath, "part-" + (part++) + EXT))) {
      writer.writeTo(out);
    }
    writer = new AvroColumnWriter<>(schema, meta, ReflectData.get());
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
    flush();
  }

  static ColumnFileMetaData filterMetadata(final Configuration configuration) {
    final ColumnFileMetaData meta = new ColumnFileMetaData();

    for (Entry<String, String> confEntry : configuration) {
      if (confEntry.getKey().startsWith(META_PREFIX))
        meta.put(confEntry.getKey().substring(META_PREFIX.length()),
            confEntry.getValue().getBytes(StandardCharsets.UTF_8));
    }

    return meta;
  }
}
