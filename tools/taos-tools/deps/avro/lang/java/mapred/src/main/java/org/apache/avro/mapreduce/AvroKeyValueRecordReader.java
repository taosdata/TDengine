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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

/**
 * Reads Avro generic records from an Avro container file, where the records
 * contain two fields: 'key' and 'value'.
 *
 * <p>
 * The contents of the 'key' field will be parsed into an AvroKey object. The
 * contents of the 'value' field will be parsed into an AvroValue object.
 * </p>
 *
 * @param <K> The type of the Avro key to read.
 * @param <V> The type of the Avro value to read.
 */
public class AvroKeyValueRecordReader<K, V> extends AvroRecordReaderBase<AvroKey<K>, AvroValue<V>, GenericRecord> {
  /** The current key the reader is on. */
  private final AvroKey<K> mCurrentKey;

  /** The current value the reader is on. */
  private final AvroValue<V> mCurrentValue;

  /**
   * Constructor.
   *
   * @param keyReaderSchema   The reader schema for the key within the generic
   *                          record.
   * @param valueReaderSchema The reader schema for the value within the generic
   *                          record.
   */
  public AvroKeyValueRecordReader(Schema keyReaderSchema, Schema valueReaderSchema) {
    super(AvroKeyValue.getSchema(keyReaderSchema, valueReaderSchema));
    mCurrentKey = new AvroKey<>(null);
    mCurrentValue = new AvroValue<>(null);
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = super.nextKeyValue();
    if (hasNext) {
      AvroKeyValue<K, V> avroKeyValue = new AvroKeyValue<>(getCurrentRecord());
      mCurrentKey.datum(avroKeyValue.getKey());
      mCurrentValue.datum(avroKeyValue.getValue());
    } else {
      mCurrentKey.datum(null);
      mCurrentValue.datum(null);
    }
    return hasNext;
  }

  /** {@inheritDoc} */
  @Override
  public AvroKey<K> getCurrentKey() throws IOException, InterruptedException {
    return mCurrentKey;
  }

  /** {@inheritDoc} */
  @Override
  public AvroValue<V> getCurrentValue() throws IOException, InterruptedException {
    return mCurrentValue;
  }
}
