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

import com.google.protobuf.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtoConversions.TimestampMicrosConversion;
import org.apache.avro.protobuf.ProtoConversions.TimestampMillisConversion;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestProtoConversions {

  private static Schema TIMESTAMP_MILLIS_SCHEMA;
  private static Schema TIMESTAMP_MICROS_SCHEMA;

  private static Calendar Jan_2_1900_3_4_5_678 = Calendar.getInstance();
  private static Calendar May_28_2015_21_46_53_221 = Calendar.getInstance();

  static {
    May_28_2015_21_46_53_221.setTimeZone(TimeZone.getTimeZone("UTC"));
    May_28_2015_21_46_53_221.set(2015, Calendar.MAY, 28, 21, 46, 53);
    May_28_2015_21_46_53_221.set(Calendar.MILLISECOND, 221);

    Jan_2_1900_3_4_5_678.setTimeZone(TimeZone.getTimeZone("UTC"));
    Jan_2_1900_3_4_5_678.set(1900, Calendar.JANUARY, 2, 3, 4, 5);
    Jan_2_1900_3_4_5_678.set(Calendar.MILLISECOND, 678);
  }

  @BeforeClass
  public static void createSchemas() {
    TestProtoConversions.TIMESTAMP_MILLIS_SCHEMA = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    TestProtoConversions.TIMESTAMP_MICROS_SCHEMA = LogicalTypes.timestampMicros()
        .addToSchema(Schema.create(Schema.Type.LONG));
  }

  @Test
  public void testTimestampMillisConversion() throws Exception {
    TimestampMillisConversion conversion = new TimestampMillisConversion();
    Timestamp May_28_2015_21_46_53_221_ts = Timestamp.newBuilder().setSeconds(1432849613L).setNanos(221000000).build();
    Timestamp Jan_2_1900_3_4_5_678_ts = Timestamp.newBuilder().setSeconds(-2208891355L).setNanos(678000000).build();

    long instant = May_28_2015_21_46_53_221.getTimeInMillis();
    Timestamp tsFromInstant = conversion.fromLong(instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    long roundTrip = conversion.toLong(tsFromInstant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());

    Assert.assertEquals("Round-trip conversion should work", instant, roundTrip);
    Assert.assertEquals("Known timestamp should be correct", May_28_2015_21_46_53_221_ts,
        conversion.fromLong(instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
    Assert.assertEquals("Known timestamp should be correct", instant,
        (long) conversion.toLong(May_28_2015_21_46_53_221_ts, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));

    instant = Jan_2_1900_3_4_5_678.getTimeInMillis();
    tsFromInstant = conversion.fromLong(instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
    roundTrip = conversion.toLong(tsFromInstant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());

    Assert.assertEquals("Round-trip conversion should work", instant, roundTrip);
    Assert.assertEquals("Known timestamp should be correct", Jan_2_1900_3_4_5_678_ts,
        conversion.fromLong(instant, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
    Assert.assertEquals("Known timestamp should be correct", instant,
        (long) conversion.toLong(Jan_2_1900_3_4_5_678_ts, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis()));
  }

  @Test
  public void testTimestampMicrosConversion() {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();
    Timestamp May_28_2015_21_46_53_221_843_ts = Timestamp.newBuilder().setSeconds(1432849613L).setNanos(221843000)
        .build();
    Timestamp Jan_2_1900_3_4_5_678_901_ts = Timestamp.newBuilder().setSeconds(-2208891355L).setNanos(678901000).build();

    long instant = May_28_2015_21_46_53_221.getTimeInMillis() * 1000 + 843;
    Timestamp tsFromInstant = conversion.fromLong(instant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
    long roundTrip = conversion.toLong(tsFromInstant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());

    Assert.assertEquals("Round-trip conversion should work", instant, roundTrip);
    Assert.assertEquals("Known timestamp should be correct", May_28_2015_21_46_53_221_843_ts,
        conversion.fromLong(instant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));
    Assert.assertEquals("Known timestamp should be correct", instant, (long) conversion
        .toLong(May_28_2015_21_46_53_221_843_ts, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));

    instant = Jan_2_1900_3_4_5_678.getTimeInMillis() * 1000 + 901;
    tsFromInstant = conversion.fromLong(instant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
    roundTrip = conversion.toLong(tsFromInstant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());

    Assert.assertEquals("Round-trip conversion should work", instant, roundTrip);
    Assert.assertEquals("Known timestamp should be correct", Jan_2_1900_3_4_5_678_901_ts,
        conversion.fromLong(instant, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));
    Assert.assertEquals("Known timestamp should be correct", instant,
        (long) conversion.toLong(Jan_2_1900_3_4_5_678_901_ts, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampMillisConversionSecondsLowerLimit() throws Exception {
    TimestampMillisConversion conversion = new TimestampMillisConversion();
    long exceeded = (ProtoConversions.SECONDS_LOWERLIMIT - 1) * 1000;
    conversion.fromLong(exceeded, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampMillisConversionSecondsUpperLimit() throws Exception {
    TimestampMillisConversion conversion = new TimestampMillisConversion();
    long exceeded = (ProtoConversions.SECONDS_UPPERLIMIT + 1) * 1000;
    conversion.fromLong(exceeded, TIMESTAMP_MILLIS_SCHEMA, LogicalTypes.timestampMillis());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampMicrosConversionSecondsLowerLimit() throws Exception {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();
    long exceeded = (ProtoConversions.SECONDS_LOWERLIMIT - 1) * 1000000;
    conversion.fromLong(exceeded, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTimestampMicrosConversionSecondsUpperLimit() throws Exception {
    TimestampMicrosConversion conversion = new TimestampMicrosConversion();
    long exceeded = (ProtoConversions.SECONDS_UPPERLIMIT + 1) * 1000000;
    conversion.fromLong(exceeded, TIMESTAMP_MICROS_SCHEMA, LogicalTypes.timestampMicros());
  }

  /*
   * model.addLogicalTypeConversion(new ProtoConversions.TimeMicrosConversion());
   * model.addLogicalTypeConversion(new
   * ProtoConversions.TimestampMicrosConversion());
   */
  @Test
  public void testDynamicSchemaWithDateTimeConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("com.google.protobuf.Timestamp", new TimestampMillisConversion());
    Assert.assertEquals("Reflected schema should be logicalType timestampMillis", TIMESTAMP_MILLIS_SCHEMA, schema);
  }

  @Test
  public void testDynamicSchemaWithDateTimeMicrosConversion() throws ClassNotFoundException {
    Schema schema = getReflectedSchemaByName("com.google.protobuf.Timestamp", new TimestampMicrosConversion());
    Assert.assertEquals("Reflected schema should be logicalType timestampMicros", TIMESTAMP_MICROS_SCHEMA, schema);
  }

  private Schema getReflectedSchemaByName(String className, Conversion<?> conversion) throws ClassNotFoundException {
    // one argument: a fully qualified class name
    Class<?> cls = Class.forName(className);

    // get the reflected schema for the given class
    ReflectData model = new ReflectData();
    model.addLogicalTypeConversion(conversion);
    return model.getSchema(cls);
  }

}
