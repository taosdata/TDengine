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

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;

/**
 * Reads Trevni generic records from an Trevni container file, where the records
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
public class AvroTrevniKeyValueRecordReader<K, V>
    extends AvroTrevniRecordReaderBase<AvroKey<K>, AvroValue<V>, GenericRecord> {

  /** The current key the reader is on. */
  private final AvroKey<K> mCurrentKey = new AvroKey<>();
  /** The current value the reader is on. */
  private final AvroValue<V> mCurrentValue = new AvroValue<>();

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

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = super.nextKeyValue();
    AvroKeyValue<K, V> avroKeyValue = new AvroKeyValue<>(getCurrentRecord());
    mCurrentKey.datum(avroKeyValue.getKey());
    mCurrentValue.datum(avroKeyValue.getValue());
    return hasNext;
  }
}
