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

package org.apache.avro.data;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class TimeConversions {
  public static class DateConversion extends Conversion<LocalDate> {

    @Override
    public Class<LocalDate> getConvertedType() {
      return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "date";
    }

    @Override
    public LocalDate fromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
      return LocalDate.ofEpochDay(daysFromEpoch);
    }

    @Override
    public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
      long epochDays = date.toEpochDay();

      return (int) epochDays;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    }
  }

  public static class TimeMillisConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-millis";
    }

    @Override
    public String adjustAndSetValue(String varName, String valParamName) {
      return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MILLIS);";
    }

    @Override
    public LocalTime fromInt(Integer millisFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight));
    }

    @Override
    public Integer toInt(LocalTime time, Schema schema, LogicalType type) {
      return (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    }
  }

  public static class TimeMicrosConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-micros";
    }

    @Override
    public String adjustAndSetValue(String varName, String valParamName) {
      return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MICROS);";
    }

    @Override
    public LocalTime fromLong(Long microsFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight));
    }

    @Override
    public Long toLong(LocalTime time, Schema schema, LogicalType type) {
      return TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay());
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMillisConversion extends Conversion<Instant> {
    @Override
    public Class<Instant> getConvertedType() {
      return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public String adjustAndSetValue(String varName, String valParamName) {
      return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MILLIS);";
    }

    @Override
    public Instant fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      return Instant.ofEpochMilli(millisFromEpoch);
    }

    @Override
    public Long toLong(Instant timestamp, Schema schema, LogicalType type) {
      return timestamp.toEpochMilli();
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMicrosConversion extends Conversion<Instant> {
    @Override
    public Class<Instant> getConvertedType() {
      return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public String adjustAndSetValue(String varName, String valParamName) {
      return varName + " = " + valParamName + ".truncatedTo(java.time.temporal.ChronoUnit.MICROS);";
    }

    @Override
    public Instant fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      long epochSeconds = microsFromEpoch / (1_000_000L);
      long nanoAdjustment = (microsFromEpoch % (1_000_000L)) * 1_000L;

      return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
    }

    @Override
    public Long toLong(Instant instant, Schema schema, LogicalType type) {
      long seconds = instant.getEpochSecond();
      int nanos = instant.getNano();

      if (seconds < 0 && nanos > 0) {
        long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
        long adjustment = (nanos / 1_000L) - 1_000_000;

        return Math.addExact(micros, adjustment);
      } else {
        long micros = Math.multiplyExact(seconds, 1_000_000L);

        return Math.addExact(micros, nanos / 1_000L);
      }
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class LocalTimestampMillisConversion extends Conversion<LocalDateTime> {
    private final TimestampMillisConversion timestampMillisConversion = new TimestampMillisConversion();

    @Override
    public Class<LocalDateTime> getConvertedType() {
      return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "local-timestamp-millis";
    }

    @Override
    public LocalDateTime fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      Instant instant = timestampMillisConversion.fromLong(millisFromEpoch, schema, type);
      return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    public Long toLong(LocalDateTime timestamp, Schema schema, LogicalType type) {
      Instant instant = timestamp.toInstant(ZoneOffset.UTC);
      return timestampMillisConversion.toLong(instant, schema, type);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class LocalTimestampMicrosConversion extends Conversion<LocalDateTime> {
    private final TimestampMicrosConversion timestampMicrosConversion = new TimestampMicrosConversion();

    @Override
    public Class<LocalDateTime> getConvertedType() {
      return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "local-timestamp-micros";
    }

    @Override
    public LocalDateTime fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      Instant instant = timestampMicrosConversion.fromLong(microsFromEpoch, schema, type);
      return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    public Long toLong(LocalDateTime timestamp, Schema schema, LogicalType type) {
      Instant instant = timestamp.toInstant(ZoneOffset.UTC);
      return timestampMicrosConversion.toLong(instant, schema, type);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }
}
