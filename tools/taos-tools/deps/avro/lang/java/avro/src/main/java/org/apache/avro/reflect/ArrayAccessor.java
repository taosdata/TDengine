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
import java.util.Arrays;

import org.apache.avro.io.Encoder;
import org.apache.avro.io.ResolvingDecoder;

/**
 * Helper class to provide native array access whenever possible. It is much
 * faster than using reflection-based operations on arrays.
 */
class ArrayAccessor {

  static void writeArray(boolean[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (boolean datum : data) {
      out.startItem();
      out.writeBoolean(datum);
    }
  }

  // short, and char arrays are upcast to avro int

  static void writeArray(short[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (short datum : data) {
      out.startItem();
      out.writeInt(datum);
    }
  }

  static void writeArray(char[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (char datum : data) {
      out.startItem();
      out.writeInt(datum);
    }
  }

  static void writeArray(int[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (int datum : data) {
      out.startItem();
      out.writeInt(datum);
    }
  }

  static void writeArray(long[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (long datum : data) {
      out.startItem();
      out.writeLong(datum);
    }
  }

  static void writeArray(float[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (float datum : data) {
      out.startItem();
      out.writeFloat(datum);
    }
  }

  static void writeArray(double[] data, Encoder out) throws IOException {
    int size = data.length;
    out.setItemCount(size);
    for (double datum : data) {
      out.startItem();
      out.writeDouble(datum);
    }
  }

  static Object readArray(Object array, Class<?> elementType, long l, ResolvingDecoder in) throws IOException {
    if (elementType == int.class)
      return readArray((int[]) array, l, in);
    if (elementType == long.class)
      return readArray((long[]) array, l, in);
    if (elementType == float.class)
      return readArray((float[]) array, l, in);
    if (elementType == double.class)
      return readArray((double[]) array, l, in);
    if (elementType == boolean.class)
      return readArray((boolean[]) array, l, in);
    if (elementType == char.class)
      return readArray((char[]) array, l, in);
    if (elementType == short.class)
      return readArray((short[]) array, l, in);
    return null;
  }

  static boolean[] readArray(boolean[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = in.readBoolean();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static int[] readArray(int[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = in.readInt();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static short[] readArray(short[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = (short) in.readInt();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static char[] readArray(char[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = (char) in.readInt();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static long[] readArray(long[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = in.readLong();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static float[] readArray(float[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = in.readFloat();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

  static double[] readArray(double[] array, long l, ResolvingDecoder in) throws IOException {
    int index = 0;
    do {
      int limit = index + (int) l;
      if (array.length < limit) {
        array = Arrays.copyOf(array, limit);
      }
      while (index < limit) {
        array[index] = in.readDouble();
        index++;
      }
    } while ((l = in.arrayNext()) > 0);
    return array;
  }

}
