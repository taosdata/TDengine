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

import static org.apache.avro.mapred.AvroOutputFormat.EXT;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * The equivalent of {@link org.apache.hadoop.mapred.TextOutputFormat} for
 * writing to Avro Data Files with a <code>"bytes"</code> schema.
 */
public class AvroTextOutputFormat<K, V> extends FileOutputFormat<K, V> {

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignore, JobConf job, String name, Progressable prog)
      throws IOException {

    Schema schema = Schema.create(Schema.Type.BYTES);

    final byte[] keyValueSeparator = job.get("mapreduce.output.textoutputformat.separator", "\t")
        .getBytes(StandardCharsets.UTF_8);

    final DataFileWriter<ByteBuffer> writer = new DataFileWriter<>(new ReflectDatumWriter<>());

    AvroOutputFormat.configureDataFileWriter(writer, job);

    Path path = FileOutputFormat.getTaskOutputPath(job, name + EXT);
    writer.create(schema, path.getFileSystem(job).create(path));

    return new AvroTextRecordWriter(writer, keyValueSeparator);
  }

  class AvroTextRecordWriter implements RecordWriter<K, V> {
    private final DataFileWriter<ByteBuffer> writer;
    private final byte[] keyValueSeparator;

    public AvroTextRecordWriter(DataFileWriter<ByteBuffer> writer, byte[] keyValueSeparator) {
      this.writer = writer;
      this.keyValueSeparator = keyValueSeparator;
    }

    @Override
    public void write(K key, V value) throws IOException {
      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        // NO-OP
      } else if (!nullKey && nullValue) {
        writer.append(toByteBuffer(key));
      } else if (nullKey && !nullValue) {
        writer.append(toByteBuffer(value));
      } else {
        writer.append(toByteBuffer(key, keyValueSeparator, value));
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      writer.close();
    }

    private ByteBuffer toByteBuffer(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        return ByteBuffer.wrap(to.getBytes(), 0, to.getLength());
      } else {
        return ByteBuffer.wrap(o.toString().getBytes(StandardCharsets.UTF_8));
      }
    }

    private ByteBuffer toByteBuffer(Object key, byte[] sep, Object value) throws IOException {
      byte[] keyBytes, valBytes;
      int keyLength, valLength;
      if (key instanceof Text) {
        Text tkey = (Text) key;
        keyBytes = tkey.getBytes();
        keyLength = tkey.getLength();
      } else {
        keyBytes = key.toString().getBytes(StandardCharsets.UTF_8);
        keyLength = keyBytes.length;
      }
      if (value instanceof Text) {
        Text tval = (Text) value;
        valBytes = tval.getBytes();
        valLength = tval.getLength();
      } else {
        valBytes = value.toString().getBytes(StandardCharsets.UTF_8);
        valLength = valBytes.length;
      }
      ByteBuffer buf = ByteBuffer.allocate(keyLength + sep.length + valLength);
      buf.put(keyBytes, 0, keyLength);
      buf.put(sep);
      buf.put(valBytes, 0, valLength);
      ((Buffer) buf).rewind();
      return buf;
    }

  }

}
