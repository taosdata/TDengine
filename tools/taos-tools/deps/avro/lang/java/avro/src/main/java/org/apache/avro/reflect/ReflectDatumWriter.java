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
package org.apache.avro.reflect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.MapEntry;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for existing classes via
 * Java reflection.
 */
public class ReflectDatumWriter<T> extends SpecificDatumWriter<T> {
  public ReflectDatumWriter() {
    this(ReflectData.get());
  }

  public ReflectDatumWriter(Class<T> c) {
    this(c, ReflectData.get());
  }

  public ReflectDatumWriter(Class<T> c, ReflectData data) {
    this(data.getSchema(c), data);
  }

  public ReflectDatumWriter(Schema root) {
    this(root, ReflectData.get());
  }

  public ReflectDatumWriter(Schema root, ReflectData reflectData) {
    super(root, reflectData);
  }

  protected ReflectDatumWriter(ReflectData reflectData) {
    super(reflectData);
  }

  /**
   * Called to write a array. May be overridden for alternate array
   * representations.
   */
  @Override
  protected void writeArray(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof Collection) {
      super.writeArray(schema, datum, out);
      return;
    }
    Class<?> elementClass = datum.getClass().getComponentType();
    if (null == elementClass) {
      // not a Collection or an Array
      throw new AvroRuntimeException("Array data must be a Collection or Array");
    }
    Schema element = schema.getElementType();
    if (elementClass.isPrimitive()) {
      Schema.Type type = element.getType();
      out.writeArrayStart();
      switch (type) {
      case BOOLEAN:
        if (elementClass.isPrimitive())
          ArrayAccessor.writeArray((boolean[]) datum, out);
        break;
      case DOUBLE:
        ArrayAccessor.writeArray((double[]) datum, out);
        break;
      case FLOAT:
        ArrayAccessor.writeArray((float[]) datum, out);
        break;
      case INT:
        if (elementClass.equals(int.class)) {
          ArrayAccessor.writeArray((int[]) datum, out);
        } else if (elementClass.equals(char.class)) {
          ArrayAccessor.writeArray((char[]) datum, out);
        } else if (elementClass.equals(short.class)) {
          ArrayAccessor.writeArray((short[]) datum, out);
        } else {
          arrayError(elementClass, type);
        }
        break;
      case LONG:
        ArrayAccessor.writeArray((long[]) datum, out);
        break;
      default:
        arrayError(elementClass, type);
      }
      out.writeArrayEnd();
    } else {
      out.writeArrayStart();
      writeObjectArray(element, (Object[]) datum, out);
      out.writeArrayEnd();
    }
  }

  private void writeObjectArray(Schema element, Object[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (Object datum : data) {
      this.write(element, datum, out);
    }
  }

  private void arrayError(Class<?> cl, Schema.Type type) {
    throw new AvroRuntimeException("Error writing array with inner type " + cl + " and avro type: " + type);
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    if (datum instanceof byte[])
      out.writeBytes((byte[]) datum);
    else
      super.writeBytes(datum, out);
  }

  @Override
  protected void write(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof Byte)
      datum = ((Byte) datum).intValue();
    else if (datum instanceof Short)
      datum = ((Short) datum).intValue();
    else if (datum instanceof Character)
      datum = (int) (char) (Character) datum;
    else if (datum instanceof Map && ReflectData.isNonStringMapSchema(schema)) {
      // Maps with non-string keys are written as arrays.
      // Schema for such maps is already changed. Here we
      // just switch the map to a similar form too.
      Set entries = ((Map) datum).entrySet();
      List<Map.Entry> entryList = new ArrayList<>(entries.size());
      for (Object obj : ((Map) datum).entrySet()) {
        Map.Entry e = (Map.Entry) obj;
        entryList.add(new MapEntry(e.getKey(), e.getValue()));
      }
      datum = entryList;
    }
    try {
      super.write(schema, datum, out);
    } catch (NullPointerException e) { // improve error message
      throw npe(e, " in " + schema.getFullName());
    }
  }

  @Override
  protected void writeField(Object record, Field f, Encoder out, Object state) throws IOException {
    if (state != null) {
      FieldAccessor accessor = ((FieldAccessor[]) state)[f.pos()];
      if (accessor != null) {
        if (accessor.supportsIO() && (!Schema.Type.UNION.equals(f.schema().getType()) || accessor.isCustomEncoded())) {
          accessor.write(record, out);
          return;
        }
        if (accessor.isStringable()) {
          try {
            Object object = accessor.get(record);
            write(f.schema(), (object == null) ? null : object.toString(), out);
          } catch (IllegalAccessException e) {
            throw new AvroRuntimeException("Failed to write Stringable", e);
          }
          return;
        }
      }
    }
    super.writeField(record, f, out, state);
  }
}
