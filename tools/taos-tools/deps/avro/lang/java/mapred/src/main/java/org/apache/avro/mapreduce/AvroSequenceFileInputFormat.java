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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.avro.hadoop.io.AvroSequenceFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * An input format for reading from AvroSequenceFiles (sequence files that
 * support Avro data).
 *
 * @param <K> The input key type.
 * @param <V> The input value type.
 */
public class AvroSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
  /** {@inheritDoc} */
  @Override
  public RecordReader<K, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
    return new AvroSequenceFileRecordReader();
  }

  /**
   * Reads records from a SequenceFile that supports Avro data.
   *
   * <p>
   * This class is based on Hadoop's SequenceFileRecordReader, modified to
   * construct an AvroSequenceFile.Reader instead of a SequenceFile.Reader.
   * </p>
   */
  protected class AvroSequenceFileRecordReader extends RecordReader<K, V> {
    private SequenceFile.Reader mReader;
    private long mStart;
    private long mEnd;
    private boolean mHasMoreData;
    private K mCurrentKey;
    private V mCurrentValue;

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) split;
      Configuration conf = context.getConfiguration();
      Path path = fileSplit.getPath();
      FileSystem fs = path.getFileSystem(conf);

      // Configure the SequenceFile reader.
      AvroSequenceFile.Reader.Options options = new AvroSequenceFile.Reader.Options().withFileSystem(fs)
          .withInputPath(path).withConfiguration(conf);
      Schema keySchema = AvroJob.getInputKeySchema(conf);
      if (null != keySchema) {
        options.withKeySchema(keySchema);
      }
      Schema valueSchema = AvroJob.getInputValueSchema(conf);
      if (null != valueSchema) {
        options.withValueSchema(valueSchema);
      }

      mReader = new AvroSequenceFile.Reader(options);
      mEnd = fileSplit.getStart() + fileSplit.getLength();

      if (fileSplit.getStart() > mReader.getPosition()) {
        // Sync to the beginning of the input split.
        mReader.sync(fileSplit.getStart());
      }

      mStart = mReader.getPosition();
      mHasMoreData = mStart < mEnd;
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!mHasMoreData) {
        return false;
      }
      long pos = mReader.getPosition();
      mCurrentKey = (K) mReader.next(mCurrentKey);
      if (null == mCurrentKey || (pos >= mEnd && mReader.syncSeen())) {
        mHasMoreData = false;
        mCurrentKey = null;
        mCurrentValue = null;
      } else {
        mCurrentValue = (V) mReader.getCurrentValue(mCurrentValue);
      }
      return mHasMoreData;
    }

    /** {@inheritDoc} */
    @Override
    public K getCurrentKey() {
      return mCurrentKey;
    }

    /** {@inheritDoc} */
    @Override
    public V getCurrentValue() {
      return mCurrentValue;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      if (mEnd == mStart) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (mReader.getPosition() - mStart) / (float) (mEnd - mStart));
      }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() throws IOException {
      mReader.close();
    }
  }
}
