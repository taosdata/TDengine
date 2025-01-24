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
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for existing classes via
 * Java reflection.
 */
public class ReflectDatumReader<T> extends SpecificDatumReader<T> {
  public ReflectDatumReader() {
    this(null, null, ReflectData.get());
  }

  /** Construct for reading instances of a class. */
  public ReflectDatumReader(Class<T> c) {
    this(new ReflectData(c.getClassLoader()));
    setSchema(getSpecificData().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public ReflectDatumReader(Schema root) {
    this(root, root, ReflectData.get());
  }

  /** Construct given writer's and reader's schema. */
  public ReflectDatumReader(Schema writer, Schema reader) {
    this(writer, reader, ReflectData.get());
  }

  /** Construct given writer's and reader's schema and the data model. */
  public ReflectDatumReader(Schema writer, Schema reader, ReflectData data) {
    super(writer, reader, data);
  }

  /** Construct given a {@link ReflectData}. */
  public ReflectDatumReader(ReflectData data) {
    super(data);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    Class<?> collectionClass = ReflectData.getClassProp(schema, SpecificData.CLASS_PROP);
    Class<?> elementClass = ReflectData.getClassProp(schema, SpecificData.ELEMENT_PROP);

    if (elementClass == null) {
      // see if the element class will be converted and use that class
      // logical types cannot conflict with java-element-class
      Conversion<?> elementConversion = getData().getConversionFor(schema.getElementType().getLogicalType());
      if (elementConversion != null) {
        elementClass = elementConversion.getConvertedType();
      }
    }

    if (collectionClass == null && elementClass == null)
      return super.newArray(old, size, schema); // use specific/generic

    if (collectionClass != null && !collectionClass.isArray()) {
      if (old instanceof Collection) {
        ((Collection<?>) old).clear();
        return old;
      }
      if (collectionClass.isAssignableFrom(ArrayList.class))
        return new ArrayList<>();
      return SpecificData.newInstance(collectionClass, schema);
    }

    if (elementClass == null) {
      elementClass = collectionClass.getComponentType();
    }
    if (elementClass == null) {
      ReflectData data = (ReflectData) getData();
      elementClass = data.getClass(schema.getElementType());
    }
    return Array.newInstance(elementClass, size);
  }

  @Override
  protected Object peekArray(Object array) {
    return null;
  }

  @Override
  protected void addToArray(Object array, long pos, Object e) {
    throw new AvroRuntimeException("reflectDatumReader does not use addToArray");
  }

  @Override
  /**
   * Called to read an array instance. May be overridden for alternate array
   * representations.
   */
  protected Object readArray(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    Schema expectedType = expected.getElementType();
    long l = in.readArrayStart();
    if (l <= 0) {
      return newArray(old, 0, expected);
    }
    Object array = newArray(old, (int) l, expected);
    if (array instanceof Collection) {
      @SuppressWarnings("unchecked")
      Collection<Object> c = (Collection<Object>) array;
      return readCollection(c, expectedType, l, in);
    } else if (array instanceof Map) {
      // Only for non-string keys, we can use NS_MAP_* fields
      // So we check the samee explicitly here
      if (ReflectData.isNonStringMapSchema(expected)) {
        Collection<Object> c = new ArrayList<>();
        readCollection(c, expectedType, l, in);
        Map m = (Map) array;
        for (Object ele : c) {
          IndexedRecord rec = ((IndexedRecord) ele);
          Object key = rec.get(ReflectData.NS_MAP_KEY_INDEX);
          Object value = rec.get(ReflectData.NS_MAP_VALUE_INDEX);
          m.put(key, value);
        }
        return array;
      } else {
        String msg = "Expected a schema of map with non-string keys but got " + expected;
        throw new AvroRuntimeException(msg);
      }
    } else {
      return readJavaArray(array, expectedType, l, in);
    }
  }

  private Object readJavaArray(Object array, Schema expectedType, long l, ResolvingDecoder in) throws IOException {
    Class<?> elementType = array.getClass().getComponentType();
    if (elementType.isPrimitive()) {
      return readPrimitiveArray(array, elementType, l, in);
    } else {
      return readObjectArray((Object[]) array, expectedType, l, in);
    }
  }

  private Object readPrimitiveArray(Object array, Class<?> c, long l, ResolvingDecoder in) throws IOException {
    return ArrayAccessor.readArray(array, c, l, in);
  }

  private Object readObjectArray(Object[] array, Schema expectedType, long l, ResolvingDecoder in) throws IOException {
    LogicalType logicalType = expectedType.getLogicalType();
    Conversion<?> conversion = getData().getConversionFor(logicalType);
    int index = 0;
    if (logicalType != null && conversion != null) {
      do {
        int limit = index + (int) l;
        while (index < limit) {
          Object element = readWithConversion(null, expectedType, logicalType, conversion, in);
          array[index] = element;
          index++;
        }
      } while ((l = in.arrayNext()) > 0);
    } else {
      do {
        int limit = index + (int) l;
        while (index < limit) {
          Object element = readWithoutConversion(null, expectedType, in);
          array[index] = element;
          index++;
        }
      } while ((l = in.arrayNext()) > 0);
    }
    return array;
  }

  private Object readCollection(Collection<Object> c, Schema expectedType, long l, ResolvingDecoder in)
      throws IOException {
    LogicalType logicalType = expectedType.getLogicalType();
    Conversion<?> conversion = getData().getConversionFor(logicalType);
    if (logicalType != null && conversion != null) {
      do {
        for (int i = 0; i < l; i++) {
          Object element = readWithConversion(null, expectedType, logicalType, conversion, in);
          c.add(element);
        }
      } while ((l = in.arrayNext()) > 0);
    } else {
      do {
        for (int i = 0; i < l; i++) {
          Object element = readWithoutConversion(null, expectedType, in);
          c.add(element);
        }
      } while ((l = in.arrayNext()) > 0);
    }
    return c;
  }

  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    return super.readString(null, in).toString();
  }

  @Override
  protected Object createString(String value) {
    return value;
  }

  @Override
  protected Object readBytes(Object old, Schema s, Decoder in) throws IOException {
    ByteBuffer bytes = in.readBytes(null);
    Class<?> c = ReflectData.getClassProp(s, SpecificData.CLASS_PROP);
    if (c != null && c.isArray()) {
      byte[] result = new byte[bytes.remaining()];
      bytes.get(result);
      return result;
    } else {
      return bytes;
    }
  }

  @Override
  protected Object readInt(Object old, Schema expected, Decoder in) throws IOException {
    Object value = in.readInt();
    String intClass = expected.getProp(SpecificData.CLASS_PROP);
    if (Byte.class.getName().equals(intClass))
      value = ((Integer) value).byteValue();
    else if (Short.class.getName().equals(intClass))
      value = ((Integer) value).shortValue();
    else if (Character.class.getName().equals(intClass))
      value = (char) (int) (Integer) value;
    return value;
  }

  @Override
  protected void readField(Object record, Field field, Object oldDatum, ResolvingDecoder in, Object state)
      throws IOException {
    if (state != null) {
      FieldAccessor accessor = ((FieldAccessor[]) state)[field.pos()];
      if (accessor != null) {
        if (accessor.supportsIO()
            && (!Schema.Type.UNION.equals(field.schema().getType()) || accessor.isCustomEncoded())) {
          accessor.read(record, in);
          return;
        }
        if (accessor.isStringable()) {
          try {
            String asString = (String) read(null, field.schema(), in);
            accessor.set(record,
                asString == null ? null : newInstanceFromString(accessor.getField().getType(), asString));
            return;
          } catch (Exception e) {
            throw new AvroRuntimeException("Failed to read Stringable", e);
          }
        }
        LogicalType logicalType = field.schema().getLogicalType();
        if (logicalType != null) {
          Conversion<?> conversion = getData().getConversionByClass(accessor.getField().getType(), logicalType);
          if (conversion != null) {
            try {
              accessor.set(record, convert(readWithoutConversion(oldDatum, field.schema(), in), field.schema(),
                  logicalType, conversion));
            } catch (IllegalAccessException e) {
              throw new AvroRuntimeException("Failed to set " + field);
            }
            return;
          }
        }
        try {
          accessor.set(record, readWithoutConversion(oldDatum, field.schema(), in));
          return;
        } catch (IllegalAccessException e) {
          throw new AvroRuntimeException("Failed to set " + field);
        }
      }
    }
    super.readField(record, field, oldDatum, in, state);
  }
}
