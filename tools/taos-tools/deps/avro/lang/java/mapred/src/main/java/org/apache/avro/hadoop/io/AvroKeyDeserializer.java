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

package org.apache.avro.hadoop.io;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroWrapper;

/**
 * Deserializes AvroKey objects within Hadoop.
 *
 * @param <D> The java type of the avro data to deserialize.
 *
 * @see AvroDeserializer
 */
public class AvroKeyDeserializer<D> extends AvroDeserializer<AvroWrapper<D>, D> {
  /**
   * Constructor.
   *
   * @param writerSchema The Avro writer schema for the data to deserialize.
   * @param readerSchema The Avro reader schema for the data to deserialize.
   */
  public AvroKeyDeserializer(Schema writerSchema, Schema readerSchema, ClassLoader classLoader) {
    super(writerSchema, readerSchema, classLoader);
  }

  /**
   * Constructor.
   *
   * @param writerSchema The Avro writer schema for the data to deserialize.
   * @param readerSchema The Avro reader schema for the data to deserialize.
   * @param datumReader  The Avro datum reader to use for deserialization.
   */
  public AvroKeyDeserializer(Schema writerSchema, Schema readerSchema, DatumReader<D> datumReader) {
    super(writerSchema, readerSchema, datumReader);
  }

  /**
   * Creates a new empty <code>AvroKey</code> instance.
   *
   * @return a new empty AvroKey.
   */
  @Override
  protected AvroWrapper<D> createAvroWrapper() {
    return new AvroKey<>(null);
  }
}
