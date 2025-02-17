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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for generated protobuf
 * classes.
 */
public class ProtobufDatumWriter<T> extends GenericDatumWriter<T> {
  public ProtobufDatumWriter() {
    super(ProtobufData.get());
  }

  public ProtobufDatumWriter(Class<T> c) {
    super(ProtobufData.get().getSchema(c), ProtobufData.get());
  }

  public ProtobufDatumWriter(Schema schema) {
    super(schema, ProtobufData.get());
  }

  protected ProtobufDatumWriter(Schema root, ProtobufData protobufData) {
    super(root, protobufData);
  }

  protected ProtobufDatumWriter(ProtobufData protobufData) {
    super(protobufData);
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof EnumValueDescriptor))
      super.writeEnum(schema, datum, out); // punt to generic
    else
      out.writeEnum(schema.getEnumOrdinal(((EnumValueDescriptor) datum).getName()));
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    ByteString bytes = (ByteString) datum;
    out.writeBytes(bytes.toByteArray(), 0, bytes.size());
  }

}
