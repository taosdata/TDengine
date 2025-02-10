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
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.file.HadoopCodecFactory;

import static org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL;
import static org.apache.avro.file.DataFileConstants.DEFLATE_CODEC;
import static org.apache.avro.file.DataFileConstants.XZ_CODEC;
import static org.apache.avro.file.DataFileConstants.ZSTANDARD_CODEC;
import static org.apache.avro.file.CodecFactory.DEFAULT_DEFLATE_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_XZ_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_ZSTANDARD_LEVEL;
import static org.apache.avro.file.CodecFactory.DEFAULT_ZSTANDARD_BUFFERPOOL;

/**
 * An {@link org.apache.hadoop.mapred.OutputFormat} for Avro data files.
 * <p/>
 * You can specify various options using Job Configuration properties. Look at
 * the fields in {@link AvroJob} as well as this class to get an overview of the
 * supported options.
 */
public class AvroOutputFormat<T> extends FileOutputFormat<AvroWrapper<T>, NullWritable> {

  /** The file name extension for avro data files. */
  public final static String EXT = ".avro";

  /** The configuration key for Avro deflate level. */
  public static final String DEFLATE_LEVEL_KEY = "avro.mapred.deflate.level";

  /** The configuration key for Avro XZ level. */
  public static final String XZ_LEVEL_KEY = "avro.mapred.xz.level";

  /** The configuration key for Avro ZSTD level. */
  public static final String ZSTD_LEVEL_KEY = "avro.mapred.zstd.level";

  /** The configuration key for Avro ZSTD buffer pool. */
  public static final String ZSTD_BUFFERPOOL_KEY = "avro.mapred.zstd.bufferpool";

  /** The configuration key for Avro sync interval. */
  public static final String SYNC_INTERVAL_KEY = "avro.mapred.sync.interval";

  /** Enable output compression using the deflate codec and specify its level. */
  public static void setDeflateLevel(JobConf job, int level) {
    FileOutputFormat.setCompressOutput(job, true);
    job.setInt(DEFLATE_LEVEL_KEY, level);
  }

  /**
   * Set the sync interval to be used by the underlying {@link DataFileWriter}.
   */
  public static void setSyncInterval(JobConf job, int syncIntervalInBytes) {
    job.setInt(SYNC_INTERVAL_KEY, syncIntervalInBytes);
  }

  static <T> void configureDataFileWriter(DataFileWriter<T> writer, JobConf job) throws UnsupportedEncodingException {

    CodecFactory factory = getCodecFactory(job);

    if (factory != null) {
      writer.setCodec(factory);
    }

    writer.setSyncInterval(job.getInt(SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL));

    // copy metadata from job
    for (Map.Entry<String, String> e : job) {
      if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.TEXT_PREFIX.length()), e.getValue());
      if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
        writer.setMeta(e.getKey().substring(AvroJob.BINARY_PREFIX.length()),
            URLDecoder.decode(e.getValue(), StandardCharsets.ISO_8859_1.name()).getBytes(StandardCharsets.ISO_8859_1));
    }
  }

  /**
   * This will select the correct compression codec from the JobConf. The order of
   * selection is as follows:
   * <ul>
   * <li>If mapred.output.compress is true then look for codec otherwise no
   * compression</li>
   * <li>Use avro.output.codec if populated</li>
   * <li>Next use mapred.output.compression.codec if populated</li>
   * <li>If not default to Deflate Codec</li>
   * </ul>
   */
  static CodecFactory getCodecFactory(JobConf job) {
    CodecFactory factory = null;

    if (FileOutputFormat.getCompressOutput(job)) {
      int deflateLevel = job.getInt(DEFLATE_LEVEL_KEY, DEFAULT_DEFLATE_LEVEL);
      int xzLevel = job.getInt(XZ_LEVEL_KEY, DEFAULT_XZ_LEVEL);
      int zstdLevel = job.getInt(ZSTD_LEVEL_KEY, DEFAULT_ZSTANDARD_LEVEL);
      boolean zstdBufferPool = job.getBoolean(ZSTD_BUFFERPOOL_KEY, DEFAULT_ZSTANDARD_BUFFERPOOL);
      String codecName = job.get(AvroJob.OUTPUT_CODEC);

      if (codecName == null) {
        String codecClassName = job.get("mapred.output.compression.codec", null);
        String avroCodecName = HadoopCodecFactory.getAvroCodecName(codecClassName);
        if (codecClassName != null && avroCodecName != null) {
          factory = HadoopCodecFactory.fromHadoopString(codecClassName);
          job.set(AvroJob.OUTPUT_CODEC, avroCodecName);
          return factory;
        } else {
          return CodecFactory.deflateCodec(deflateLevel);
        }
      } else {
        if (codecName.equals(DEFLATE_CODEC)) {
          factory = CodecFactory.deflateCodec(deflateLevel);
        } else if (codecName.equals(XZ_CODEC)) {
          factory = CodecFactory.xzCodec(xzLevel);
        } else if (codecName.equals(ZSTANDARD_CODEC)) {
          factory = CodecFactory.zstandardCodec(zstdLevel, false, zstdBufferPool);
        } else {
          factory = CodecFactory.fromString(codecName);
        }
      }
    }

    return factory;
  }

  @Override
  public RecordWriter<AvroWrapper<T>, NullWritable> getRecordWriter(FileSystem ignore, JobConf job, String name,
      Progressable prog) throws IOException {

    boolean isMapOnly = job.getNumReduceTasks() == 0;
    Schema schema = isMapOnly ? AvroJob.getMapOutputSchema(job) : AvroJob.getOutputSchema(job);
    GenericData dataModel = AvroJob.createDataModel(job);

    final DataFileWriter<T> writer = new DataFileWriter<T>(dataModel.createDatumWriter(null));

    configureDataFileWriter(writer, job);

    Path path = FileOutputFormat.getTaskOutputPath(job, name + EXT);
    writer.create(schema, path.getFileSystem(job).create(path));

    return new RecordWriter<AvroWrapper<T>, NullWritable>() {
      @Override
      public void write(AvroWrapper<T> wrapper, NullWritable ignore) throws IOException {
        writer.append(wrapper.datum());
      }

      @Override
      public void close(Reporter reporter) throws IOException {
        writer.close();
      }
    };
  }

}
