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

import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.HadoopInput;

/**
 * Abstract base class for <code>RecordReader</code>s that read Trevni container
 * files.
 *
 * @param <K> The type of key the record reader should generate.
 * @param <V> The type of value the record reader should generate.
 * @param <T> The type of the entries within the Trevni container file being
 *            read.
 */
public abstract class AvroTrevniRecordReaderBase<K, V, T> extends RecordReader<K, V> {

  /** The Trevni file reader */
  private AvroColumnReader<T> reader;

  /** Number of rows in the Trevni file */
  private float rows;

  /** The current row number being read in */
  private long row;

  /** A reusable object to hold records of the Avro container file. */
  private T mCurrentRecord;

  /** {@inheritDoc} */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
    final FileSplit file = (FileSplit) inputSplit;
    context.setStatus(file.toString());

    final AvroColumnReader.Params params = new AvroColumnReader.Params(
        new HadoopInput(file.getPath(), context.getConfiguration()));
    params.setModel(ReflectData.get());

    if (AvroJob.getInputKeySchema(context.getConfiguration()) != null) {
      params.setSchema(AvroJob.getInputKeySchema(context.getConfiguration()));
    }

    reader = new AvroColumnReader<>(params);
    rows = reader.getRowCount();
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!reader.hasNext())
      return false;
    mCurrentRecord = reader.next();
    row++;
    return true;
  }

  /**
   * Gets the current record read from the Trevni container file.
   *
   * <p>
   * Calling <code>nextKeyValue()</code> moves this to the next record.
   * </p>
   *
   * @return The current Trevni record (may be null if no record has been read).
   */
  protected T getCurrentRecord() {
    return mCurrentRecord;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    reader.close();
  }

  /** {@inheritDoc} */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return row / rows;
  }
}
