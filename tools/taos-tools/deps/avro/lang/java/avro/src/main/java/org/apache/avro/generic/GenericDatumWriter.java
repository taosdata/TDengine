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
package org.apache.avro.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

/** {@link DatumWriter} for generic Java objects. */
public class GenericDatumWriter<D> implements DatumWriter<D> {
  private final GenericData data;
  private Schema root;

  public GenericDatumWriter() {
    this(GenericData.get());
  }

  protected GenericDatumWriter(GenericData data) {
    this.data = data;
  }

  public GenericDatumWriter(Schema root) {
    this();
    setSchema(root);
  }

  public GenericDatumWriter(Schema root, GenericData data) {
    this(data);
    setSchema(root);
  }

  /** Return the {@link GenericData} implementation. */
  public GenericData getData() {
    return data;
  }

  public void setSchema(Schema root) {
    this.root = root;
  }

  public void write(D datum, Encoder out) throws IOException {
    Objects.requireNonNull(out, "Encoder cannot be null");
    write(root, datum, out);
  }

  /** Called to write data. */
  protected void write(Schema schema, Object datum, Encoder out) throws IOException {
    LogicalType logicalType = schema.getLogicalType();
    if (datum != null && logicalType != null) {
      Conversion<?> conversion = getData().getConversionByClass(datum.getClass(), logicalType);
      writeWithoutConversion(schema, convert(schema, logicalType, conversion, datum), out);
    } else {
      writeWithoutConversion(schema, datum, out);
    }
  }

  /**
   * Convert a high level representation of a logical type (such as a BigDecimal)
   * to the its underlying representation object (such as a ByteBuffer).
   *
   * @throws IllegalArgumentException if a null schema or logicalType is passed in
   *                                  while datum and conversion are not null.
   *                                  Please be noticed that the exception type
   *                                  has changed. With version 1.8.0 and earlier,
   *                                  in above circumstance, the exception thrown
   *                                  out depends on the implementation of
   *                                  conversion (most likely a
   *                                  NullPointerException). Now, an
   *                                  IllegalArgumentException will be thrown out
   *                                  instead.
   */
  protected <T> Object convert(Schema schema, LogicalType logicalType, Conversion<T> conversion, Object datum) {
    try {
      if (conversion == null) {
        return datum;
      } else {
        return Conversions.convertToRawType(datum, schema, logicalType, conversion);
      }
    } catch (AvroRuntimeException e) {
      Throwable cause = e.getCause();
      if (cause != null && cause.getClass() == ClassCastException.class) {
        // This is to keep backwards compatibility. The convert function here used to
        // throw CCE. After being moved to Conversions, it throws AvroRuntimeException
        // instead. To keep as much same behaviour as before, this function checks if
        // the cause is a CCE. If yes, rethrow it in case any child class checks it.
        // This
        // behaviour can be changed later in future versions to make it consistent with
        // reading path, which throws AvroRuntimeException
        throw (ClassCastException) cause;
      } else {
        throw e;
      }
    }
  }

  /** Called to write data. */
  protected void writeWithoutConversion(Schema schema, Object datum, Encoder out) throws IOException {
    try {
      switch (schema.getType()) {
      case RECORD:
        writeRecord(schema, datum, out);
        break;
      case ENUM:
        writeEnum(schema, datum, out);
        break;
      case ARRAY:
        writeArray(schema, datum, out);
        break;
      case MAP:
        writeMap(schema, datum, out);
        break;
      case UNION:
        int index = resolveUnion(schema, datum);
        out.writeIndex(index);
        write(schema.getTypes().get(index), datum, out);
        break;
      case FIXED:
        writeFixed(schema, datum, out);
        break;
      case STRING:
        writeString(schema, datum, out);
        break;
      case BYTES:
        writeBytes(datum, out);
        break;
      case INT:
        out.writeInt(((Number) datum).intValue());
        break;
      case LONG:
        out.writeLong(((Number) datum).longValue());
        break;
      case FLOAT:
        out.writeFloat(((Number) datum).floatValue());
        break;
      case DOUBLE:
        out.writeDouble(((Number) datum).doubleValue());
        break;
      case BOOLEAN:
        out.writeBoolean((Boolean) datum);
        break;
      case NULL:
        out.writeNull();
        break;
      default:
        error(schema, datum);
      }
    } catch (NullPointerException e) {
      throw npe(e, " of " + schema.getFullName());
    }
  }

  /** Helper method for adding a message to an NPE . */
  protected NullPointerException npe(NullPointerException e, String s) {
    NullPointerException result = new NullPointerException(e.getMessage() + s);
    result.initCause(e.getCause() == null ? e : e.getCause());
    return result;
  }

  /** Helper method for adding a message to an Class Cast Exception . */
  protected ClassCastException addClassCastMsg(ClassCastException e, String s) {
    ClassCastException result = new ClassCastException(e.getMessage() + s);
    result.initCause(e.getCause() == null ? e : e.getCause());
    return result;
  }

  /** Helper method for adding a message to an Avro Type Exception . */
  protected AvroTypeException addAvroTypeMsg(AvroTypeException e, String s) {
    AvroTypeException result = new AvroTypeException(e.getMessage() + s);
    result.initCause(e.getCause() == null ? e : e.getCause());
    return result;
  }

  /**
   * Called to write a record. May be overridden for alternate record
   * representations.
   */
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
    Object state = data.getRecordState(datum, schema);
    for (Field f : schema.getFields()) {
      writeField(datum, f, out, state);
    }
  }

  /**
   * Called to write a single field of a record. May be overridden for more
   * efficient or alternate implementations.
   */
  protected void writeField(Object datum, Field f, Encoder out, Object state) throws IOException {
    Object value = data.getField(datum, f.name(), f.pos(), state);
    try {
      write(f.schema(), value, out);
    } catch (final UnresolvedUnionException uue) { // recreate it with the right field info
      final UnresolvedUnionException unresolvedUnionException = new UnresolvedUnionException(f.schema(), f, value);
      unresolvedUnionException.addSuppressed(uue);
      throw unresolvedUnionException;
    } catch (NullPointerException e) {
      throw npe(e, " in field " + f.name());
    } catch (ClassCastException cce) {
      throw addClassCastMsg(cce, " in field " + f.name());
    } catch (AvroTypeException ate) {
      throw addAvroTypeMsg(ate, " in field " + f.name());
    }
  }

  /**
   * Called to write an enum value. May be overridden for alternate enum
   * representations.
   */
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!data.isEnum(datum))
      throw new AvroTypeException("Not an enum: " + datum + " for schema: " + schema);
    out.writeEnum(schema.getEnumOrdinal(datum.toString()));
  }

  /**
   * Called to write a array. May be overridden for alternate array
   * representations.
   */
  protected void writeArray(Schema schema, Object datum, Encoder out) throws IOException {
    Schema element = schema.getElementType();
    long size = getArraySize(datum);
    long actualSize = 0;
    out.writeArrayStart();
    out.setItemCount(size);
    for (Iterator<? extends Object> it = getArrayElements(datum); it.hasNext();) {
      out.startItem();
      write(element, it.next(), out);
      actualSize++;
    }
    out.writeArrayEnd();
    if (actualSize != size) {
      throw new ConcurrentModificationException(
          "Size of array written was " + size + ", but number of elements written was " + actualSize + ". ");
    }
  }

  /**
   * Called to find the index for a datum within a union. By default calls
   * {@link GenericData#resolveUnion(Schema,Object)}.
   */
  protected int resolveUnion(Schema union, Object datum) {
    return data.resolveUnion(union, datum);
  }

  /**
   * Called by the default implementation of {@link #writeArray} to get the size
   * of an array. The default implementation is for {@link Collection}.
   */
  @SuppressWarnings("unchecked")
  protected long getArraySize(Object array) {
    return ((Collection) array).size();
  }

  /**
   * Called by the default implementation of {@link #writeArray} to enumerate
   * array elements. The default implementation is for {@link Collection}.
   */
  @SuppressWarnings("unchecked")
  protected Iterator<? extends Object> getArrayElements(Object array) {
    return ((Collection) array).iterator();
  }

  /**
   * Called to write a map. May be overridden for alternate map representations.
   */
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    Schema value = schema.getValueType();
    int size = getMapSize(datum);
    int actualSize = 0;
    out.writeMapStart();
    out.setItemCount(size);
    for (Map.Entry<Object, Object> entry : getMapEntries(datum)) {
      out.startItem();
      writeString(entry.getKey().toString(), out);
      write(value, entry.getValue(), out);
      actualSize++;
    }
    out.writeMapEnd();
    if (actualSize != size) {
      throw new ConcurrentModificationException(
          "Size of map written was " + size + ", but number of entries written was " + actualSize + ". ");
    }
  }

  /**
   * Called by the default implementation of {@link #writeMap} to get the size of
   * a map. The default implementation is for {@link Map}.
   */
  @SuppressWarnings("unchecked")
  protected int getMapSize(Object map) {
    return ((Map) map).size();
  }

  /**
   * Called by the default implementation of {@link #writeMap} to enumerate map
   * elements. The default implementation is for {@link Map}.
   */
  @SuppressWarnings("unchecked")
  protected Iterable<Map.Entry<Object, Object>> getMapEntries(Object map) {
    return ((Map) map).entrySet();
  }

  /**
   * Called to write a string. May be overridden for alternate string
   * representations.
   */
  protected void writeString(Schema schema, Object datum, Encoder out) throws IOException {
    writeString(datum, out);
  }

  /**
   * Called to write a string. May be overridden for alternate string
   * representations.
   */
  protected void writeString(Object datum, Encoder out) throws IOException {
    out.writeString((CharSequence) datum);
  }

  /**
   * Called to write a bytes. May be overridden for alternate bytes
   * representations.
   */
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    out.writeBytes((ByteBuffer) datum);
  }

  /**
   * Called to write a fixed value. May be overridden for alternate fixed
   * representations.
   */
  protected void writeFixed(Schema schema, Object datum, Encoder out) throws IOException {
    out.writeFixed(((GenericFixed) datum).bytes(), 0, schema.getFixedSize());
  }

  private void error(Schema schema, Object datum) {
    throw new AvroTypeException("Not a " + schema + ": " + datum);
  }

}
