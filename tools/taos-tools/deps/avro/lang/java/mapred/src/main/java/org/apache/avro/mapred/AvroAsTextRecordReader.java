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
import java.nio.ByteBuffer;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

class AvroAsTextRecordReader<T> implements RecordReader<Text, Text> {

  private FileReader<T> reader;
  private T datum;
  private long start;
  private long end;

  public AvroAsTextRecordReader(JobConf job, FileSplit split) throws IOException {
    this(DataFileReader.openReader(new FsInput(split.getPath(), job), new GenericDatumReader<>()), split);
  }

  protected AvroAsTextRecordReader(FileReader<T> reader, FileSplit split) throws IOException {
    this.reader = reader;
    reader.sync(split.getStart()); // sync to start
    this.start = reader.tell();
    this.end = split.getStart() + split.getLength();
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public Text createValue() {
    return new Text();
  }

  @Override
  public boolean next(Text key, Text ignore) throws IOException {
    if (!reader.hasNext() || reader.pastSync(end))
      return false;
    datum = reader.next(datum);
    if (datum instanceof ByteBuffer) {
      ByteBuffer b = (ByteBuffer) datum;
      if (b.hasArray()) {
        int offset = b.arrayOffset();
        int start = b.position();
        int length = b.remaining();
        key.set(b.array(), offset + start, offset + start + length);
      } else {
        byte[] bytes = new byte[b.remaining()];
        b.duplicate().get(bytes);
        key.set(bytes);
      }
    } else {
      key.set(GenericData.get().toString(datum));
    }
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    if (end == start) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getPos() - start) / (float) (end - start));
    }
  }

  @Override
  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
