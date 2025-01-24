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
import java.lang.reflect.Field;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
class FieldAccessUnsafe extends FieldAccess {

  private static final Unsafe UNSAFE;

  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
      // It seems not all Unsafe implementations implement the following method.
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected FieldAccessor getAccessor(Field field) {
    AvroEncode enc = field.getAnnotation(AvroEncode.class);
    if (enc != null)
      try {
        return new UnsafeCustomEncodedField(field, enc.using().getDeclaredConstructor().newInstance());
      } catch (Exception e) {
        throw new AvroRuntimeException("Could not instantiate custom Encoding");
      }
    Class<?> c = field.getType();
    if (c == int.class)
      return new UnsafeIntField(field);
    else if (c == long.class)
      return new UnsafeLongField(field);
    else if (c == byte.class)
      return new UnsafeByteField(field);
    else if (c == float.class)
      return new UnsafeFloatField(field);
    else if (c == double.class)
      return new UnsafeDoubleField(field);
    else if (c == char.class)
      return new UnsafeCharField(field);
    else if (c == boolean.class)
      return new UnsafeBooleanField(field);
    else if (c == short.class)
      return new UnsafeShortField(field);
    else
      return new UnsafeObjectField(field);
  }

  abstract static class UnsafeCachedField extends FieldAccessor {
    protected final long offset;
    protected Field field;
    protected final boolean isStringable;

    UnsafeCachedField(Field f) {
      this.offset = UNSAFE.objectFieldOffset(f);
      this.field = f;
      this.isStringable = f.isAnnotationPresent(Stringable.class);
    }

    @Override
    protected Field getField() {
      return field;
    }

    @Override
    protected boolean supportsIO() {
      return true;
    }

    @Override
    protected boolean isStringable() {
      return isStringable;
    }
  }

  final static class UnsafeIntField extends UnsafeCachedField {
    UnsafeIntField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putInt(object, offset, (Integer) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getInt(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putInt(object, offset, in.readInt());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeInt(UNSAFE.getInt(object, offset));
    }
  }

  final static class UnsafeFloatField extends UnsafeCachedField {
    protected UnsafeFloatField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putFloat(object, offset, (Float) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getFloat(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putFloat(object, offset, in.readFloat());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeFloat(UNSAFE.getFloat(object, offset));
    }
  }

  final static class UnsafeShortField extends UnsafeCachedField {
    protected UnsafeShortField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putShort(object, offset, (Short) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getShort(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putShort(object, offset, (short) in.readInt());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeInt(UNSAFE.getShort(object, offset));
    }
  }

  final static class UnsafeByteField extends UnsafeCachedField {
    protected UnsafeByteField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putByte(object, offset, (Byte) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getByte(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putByte(object, offset, (byte) in.readInt());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeInt(UNSAFE.getByte(object, offset));
    }
  }

  final static class UnsafeBooleanField extends UnsafeCachedField {
    protected UnsafeBooleanField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putBoolean(object, offset, (Boolean) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getBoolean(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putBoolean(object, offset, in.readBoolean());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeBoolean(UNSAFE.getBoolean(object, offset));
    }
  }

  final static class UnsafeCharField extends UnsafeCachedField {
    protected UnsafeCharField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putChar(object, offset, (Character) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getChar(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putChar(object, offset, (char) in.readInt());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeInt(UNSAFE.getChar(object, offset));
    }
  }

  final static class UnsafeLongField extends UnsafeCachedField {
    protected UnsafeLongField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putLong(object, offset, (Long) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getLong(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putLong(object, offset, in.readLong());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeLong(UNSAFE.getLong(object, offset));
    }
  }

  final static class UnsafeDoubleField extends UnsafeCachedField {
    protected UnsafeDoubleField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putDouble(object, offset, (Double) value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getDouble(object, offset);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putDouble(object, offset, in.readDouble());
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      out.writeDouble(UNSAFE.getDouble(object, offset));
    }
  }

  final static class UnsafeObjectField extends UnsafeCachedField {
    protected UnsafeObjectField(Field f) {
      super(f);
    }

    @Override
    protected void set(Object object, Object value) {
      UNSAFE.putObject(object, offset, value);
    }

    @Override
    protected Object get(Object object) {
      return UNSAFE.getObject(object, offset);
    }

    @Override
    protected boolean supportsIO() {
      return false;
    }

  }

  final static class UnsafeCustomEncodedField extends UnsafeCachedField {

    private CustomEncoding<?> encoding;

    UnsafeCustomEncodedField(Field f, CustomEncoding<?> encoding) {
      super(f);
      this.encoding = encoding;
    }

    @Override
    protected Object get(Object object) throws IllegalAccessException {
      return UNSAFE.getObject(object, offset);
    }

    @Override
    protected void set(Object object, Object value) throws IllegalAccessException, IOException {
      UNSAFE.putObject(object, offset, value);
    }

    @Override
    protected void read(Object object, Decoder in) throws IOException {
      UNSAFE.putObject(object, offset, encoding.read(in));
    }

    @Override
    protected void write(Object object, Encoder out) throws IOException {
      encoding.write(UNSAFE.getObject(object, offset), out);
    }

    @Override
    protected boolean isCustomEncoded() {
      return true;
    }
  }
}
