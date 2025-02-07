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
package org.apache.avro.protobuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for generated Protobuf
 * classes.
 */
public class ProtobufDatumReader<T> extends GenericDatumReader<T> {
  public ProtobufDatumReader() {
    this(null, null, ProtobufData.get());
  }

  public ProtobufDatumReader(Class<T> c) {
    this(ProtobufData.get().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ProtobufDatumReader(Schema schema) {
    this(schema, schema, ProtobufData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ProtobufDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ProtobufData.get());
  }

  protected ProtobufDatumReader(Schema writer, Schema reader, ProtobufData data) {
    super(writer, reader, data);
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    Message.Builder b = (Message.Builder) super.readRecord(old, expected, in);
    return b.build(); // build instance
  }

  @Override
  protected Object createEnum(String symbol, Schema schema) {
    try {
      Class c = SpecificData.get().getClass(schema);
      if (c == null)
        return super.createEnum(symbol, schema); // punt to generic
      return ((ProtocolMessageEnum) Enum.valueOf(c, symbol)).getValueDescriptor();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Object readBytes(Object old, Decoder in) throws IOException {
    return ByteString.copyFrom(((ByteBuffer) super.readBytes(old, in)).array());
  }

}
