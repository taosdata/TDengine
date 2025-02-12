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
package org.apache.avro.thrift;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.generic.GenericDatumWriter;

import java.nio.ByteBuffer;
import java.io.IOException;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for generated thrift
 * classes.
 */
public class ThriftDatumWriter<T> extends GenericDatumWriter<T> {
  public ThriftDatumWriter() {
    super(ThriftData.get());
  }

  public ThriftDatumWriter(Class<T> c) {
    super(ThriftData.get().getSchema(c), ThriftData.get());
  }

  public ThriftDatumWriter(Schema schema) {
    super(schema, ThriftData.get());
  }

  protected ThriftDatumWriter(Schema root, ThriftData thriftData) {
    super(root, thriftData);
  }

  protected ThriftDatumWriter(ThriftData thriftData) {
    super(thriftData);
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    // Thrift assymetry: setter takes ByteBuffer but getter returns byte[]
    out.writeBytes(ByteBuffer.wrap((byte[]) datum));
  }

}
