/**
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

package org.apache.avro.mapreduce;

import java.io.IOException;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * A combine avro keyvalue file input format that can combine small avro files
 * into mappers.
 *
 * @param <K> The type of the Avro key to read.
 * @param <V> The type of the Avro value to read.
 */
public class CombineAvroKeyValueFileInputFormat<K, V> extends CombineFileInputFormat<AvroKey<K>, AvroValue<V>> {

  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException {
    return new CombineFileRecordReader((CombineFileSplit) inputSplit, taskAttemptContext,
        CombineAvroKeyValueFileInputFormat.AvroKeyValueFileRecordReaderWrapper.class);
  }

  /**
   * A record reader that may be passed to <code>CombineFileRecordReader</code> so
   * that it can be used in a <code>CombineFileInputFormat</code>-equivalent for
   * <code>AvroKeyValueInputFormat</code>.
   *
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see AvroKeyValueInputFormat
   */
  private static class AvroKeyValueFileRecordReaderWrapper<K, V>
      extends CombineFileRecordReaderWrapper<AvroKey<K>, AvroValue<V>> {
    // this constructor signature is required by CombineFileRecordReader
    public AvroKeyValueFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx)
        throws IOException, InterruptedException {
      super(new AvroKeyValueInputFormat<>(), split, context, idx);
    }
  }
}
