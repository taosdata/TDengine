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

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.ClassUtils;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for generated Thrift
 * classes.
 */
public class ThriftDatumReader<T> extends GenericDatumReader<T> {
  public ThriftDatumReader() {
    this(null, null, ThriftData.get());
  }

  public ThriftDatumReader(Class<T> c) {
    this(ThriftData.get().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ThriftDatumReader(Schema schema) {
    this(schema, schema, ThriftData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ThriftDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ThriftData.get());
  }

  protected ThriftDatumReader(Schema writer, Schema reader, ThriftData data) {
    super(writer, reader, data);
  }

  @Override
  protected Object createEnum(String symbol, Schema schema) {
    try {
      Class c = ClassUtils.forName(SpecificData.getClassName(schema));
      if (c == null)
        return super.createEnum(symbol, schema); // punt to generic
      return Enum.valueOf(c, symbol);
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  protected Object readInt(Object old, Schema s, Decoder in) throws IOException {
    String type = s.getProp(ThriftData.THRIFT_PROP);
    int value = in.readInt();
    if (type != null) {
      if ("byte".equals(type))
        return (byte) value;
      if ("short".equals(type))
        return (short) value;
    }
    return value;
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    if ("set".equals(schema.getProp(ThriftData.THRIFT_PROP))) {
      if (old instanceof Set) {
        ((Set) old).clear();
        return old;
      }
      return new HashSet();
    } else {
      return super.newArray(old, size, schema);
    }
  }

}
