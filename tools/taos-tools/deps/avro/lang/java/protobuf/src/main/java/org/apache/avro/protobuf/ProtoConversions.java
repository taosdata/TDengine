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
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class ProtoConversions {

  private static final int THOUSAND = 1000;
  private static final int MILLION = 1000000;

  // second value must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z
  // inclusive.
  static final long SECONDS_LOWERLIMIT = -62135596800L;
  static final long SECONDS_UPPERLIMIT = 253402300799L;

  // nano value Must be from 0 to 999,999,999 inclusive.
  private static final int NANOSECONDS_LOWERLIMIT = 0;
  private static final int NANOSECONDS_UPPERLIMIT = 999999999;

  // timestamp precise of conversion from long
  private enum TimestampPrecise {
    Millis, Micros
  };

  public static class TimestampMillisConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public Timestamp fromLong(Long millisFromEpoch, Schema schema, LogicalType type) throws IllegalArgumentException {
      return ProtoConversions.fromLong(millisFromEpoch, TimestampPrecise.Millis);
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      return ProtoConversions.toLong(value, TimestampPrecise.Millis);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMicrosConversion extends Conversion<Timestamp> {
    @Override
    public Class<Timestamp> getConvertedType() {
      return Timestamp.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public Timestamp fromLong(Long microsFromEpoch, Schema schema, LogicalType type) throws IllegalArgumentException {
      return ProtoConversions.fromLong(microsFromEpoch, TimestampPrecise.Micros);
    }

    @Override
    public Long toLong(Timestamp value, Schema schema, LogicalType type) {
      return ProtoConversions.toLong(value, TimestampPrecise.Micros);
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  private static long toLong(Timestamp value, TimestampPrecise precise) {
    long rv = 0L;

    switch (precise) {
    case Millis:
      rv = value.getSeconds() * THOUSAND + value.getNanos() / MILLION;
      break;
    case Micros:
      rv = value.getSeconds() * MILLION + value.getNanos() / THOUSAND;
      break;
    }

    return rv;
  }

  private static Timestamp fromLong(Long elapsedSinceEpoch, TimestampPrecise precise) throws IllegalArgumentException {
    long seconds = 0L;
    int nanos = 0;

    switch (precise) {
    case Millis:
      seconds = Math.floorDiv(elapsedSinceEpoch, (long) THOUSAND);
      nanos = (int) Math.floorMod(elapsedSinceEpoch, (long) THOUSAND) * MILLION;
      break;
    case Micros:
      seconds = Math.floorDiv(elapsedSinceEpoch, (long) MILLION);
      nanos = (int) Math.floorMod(elapsedSinceEpoch, (long) MILLION) * THOUSAND;
      break;
    }

    if (seconds < SECONDS_LOWERLIMIT || seconds > SECONDS_UPPERLIMIT) {
      throw new IllegalArgumentException("given seconds is out of range");
    }

    if (nanos < NANOSECONDS_LOWERLIMIT || nanos > NANOSECONDS_UPPERLIMIT) {
      // NOTE here is unexpected cases because exceeded part is
      // moved to seconds by floor methods
      throw new IllegalArgumentException("given nanos is out of range");
    }

    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
  }
}
