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

import java.util.Arrays;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestReflectAllowNulls {

  private static class Primitives {
    boolean aBoolean;
    byte aByte;
    short aShort;
    int anInt;
    long aLong;
    float aFloat;
    double aDouble;
  }

  private static class Wrappers {
    Boolean aBoolean;
    Byte aByte;
    Short aShort;
    Integer anInt;
    Long aLong;
    Float aFloat;
    Double aDouble;
    Primitives anObject;
  }

  private static class AllowNullWithNullable {
    @Nullable
    Double aDouble;

    @AvroSchema("[\"double\", \"long\"]")
    Object doubleOrLong;

    @Nullable
    @AvroSchema("[\"double\", \"long\"]")
    Object doubleOrLongOrNull1;

    @AvroSchema("[\"double\", \"long\", \"null\"]")
    Object doubleOrLongOrNull2;

    @Nullable
    @AvroSchema("[\"double\", \"long\", \"null\"]")
    Object doubleOrLongOrNull3;
  }

  @Test
  public void testPrimitives() {
    // AllowNull only makes fields nullable, so testing must use a base record
    Schema primitives = ReflectData.AllowNull.get().getSchema(Primitives.class);
    Assert.assertEquals(requiredSchema(boolean.class), primitives.getField("aBoolean").schema());
    Assert.assertEquals(requiredSchema(byte.class), primitives.getField("aByte").schema());
    Assert.assertEquals(requiredSchema(short.class), primitives.getField("aShort").schema());
    Assert.assertEquals(requiredSchema(int.class), primitives.getField("anInt").schema());
    Assert.assertEquals(requiredSchema(long.class), primitives.getField("aLong").schema());
    Assert.assertEquals(requiredSchema(float.class), primitives.getField("aFloat").schema());
    Assert.assertEquals(requiredSchema(double.class), primitives.getField("aDouble").schema());
  }

  @Test
  public void testWrappers() {
    // AllowNull only makes fields nullable, so testing must use a base record
    Schema wrappers = ReflectData.AllowNull.get().getSchema(Wrappers.class);
    Assert.assertEquals(nullableSchema(boolean.class), wrappers.getField("aBoolean").schema());
    Assert.assertEquals(nullableSchema(byte.class), wrappers.getField("aByte").schema());
    Assert.assertEquals(nullableSchema(short.class), wrappers.getField("aShort").schema());
    Assert.assertEquals(nullableSchema(int.class), wrappers.getField("anInt").schema());
    Assert.assertEquals(nullableSchema(long.class), wrappers.getField("aLong").schema());
    Assert.assertEquals(nullableSchema(float.class), wrappers.getField("aFloat").schema());
    Assert.assertEquals(nullableSchema(double.class), wrappers.getField("aDouble").schema());
    Assert.assertEquals(nullableSchema(Primitives.class), wrappers.getField("anObject").schema());
  }

  @Test
  public void testAllowNullWithNullableAnnotation() {
    Schema withNullable = ReflectData.AllowNull.get().getSchema(AllowNullWithNullable.class);

    Assert.assertEquals("Should produce a nullable double", nullableSchema(double.class),
        withNullable.getField("aDouble").schema());

    Schema nullableDoubleOrLong = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.LONG)));

    Assert.assertEquals("Should add null to a non-null union", nullableDoubleOrLong,
        withNullable.getField("doubleOrLong").schema());

    Assert.assertEquals("Should add null to a non-null union", nullableDoubleOrLong,
        withNullable.getField("doubleOrLongOrNull1").schema());

    Schema doubleOrLongOrNull = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL)));

    Assert.assertEquals("Should add null to a non-null union", doubleOrLongOrNull,
        withNullable.getField("doubleOrLongOrNull2").schema());

    Assert.assertEquals("Should add null to a non-null union", doubleOrLongOrNull,
        withNullable.getField("doubleOrLongOrNull3").schema());
  }

  private Schema requiredSchema(Class<?> type) {
    return ReflectData.get().getSchema(type);
  }

  private Schema nullableSchema(Class<?> type) {
    return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), ReflectData.get().getSchema(type)));
  }
}
