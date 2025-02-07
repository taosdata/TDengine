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

package org.apache.avro;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalTypes {

  private static final Logger LOG = LoggerFactory.getLogger(LogicalTypes.class);

  public interface LogicalTypeFactory {
    LogicalType fromSchema(Schema schema);

    default String getTypeName() {
      throw new UnsupportedOperationException("LogicalTypeFactory TypeName has not been provided");
    }
  }

  private static final Map<String, LogicalTypeFactory> REGISTERED_TYPES = new ConcurrentHashMap<>();

  /**
   * Register a logical type.
   *
   * @param factory The logical type factory
   *
   * @throws NullPointerException if {@code factory} or
   *                              {@code factory.getTypedName()} is {@code null}
   */
  public static void register(LogicalTypeFactory factory) {
    Objects.requireNonNull(factory, "Logical type factory cannot be null");
    register(factory.getTypeName(), factory);
  }

  /**
   * Register a logical type.
   *
   * @param logicalTypeName The logical type name
   * @param factory         The logical type factory
   *
   * @throws NullPointerException if {@code logicalTypeName} or {@code factory} is
   *                              {@code null}
   */
  public static void register(String logicalTypeName, LogicalTypeFactory factory) {
    Objects.requireNonNull(logicalTypeName, "Logical type name cannot be null");
    Objects.requireNonNull(factory, "Logical type factory cannot be null");

    try {
      String factoryTypeName = factory.getTypeName();
      if (!logicalTypeName.equals(factoryTypeName)) {
        LOG.debug("Provided logicalTypeName '{}' does not match factory typeName '{}'", logicalTypeName,
            factoryTypeName);
      }
    } catch (UnsupportedOperationException ignore) {
      // Ignore exception, as the default interface method throws
      // UnsupportedOperationException.
    }

    REGISTERED_TYPES.put(logicalTypeName, factory);
  }

  /**
   * Return an unmodifiable map of any registered custom {@link LogicalType}
   */
  public static Map<String, LogicalTypes.LogicalTypeFactory> getCustomRegisteredTypes() {
    return Collections.unmodifiableMap(REGISTERED_TYPES);
  }

  /**
   * Returns the {@link LogicalType} from the schema, if one is present.
   */
  public static LogicalType fromSchema(Schema schema) {
    return fromSchemaImpl(schema, true);
  }

  public static LogicalType fromSchemaIgnoreInvalid(Schema schema) {
    return fromSchemaImpl(schema, false);
  }

  private static LogicalType fromSchemaImpl(Schema schema, boolean throwErrors) {
    final LogicalType logicalType;
    final String typeName = schema.getProp(LogicalType.LOGICAL_TYPE_PROP);

    if (typeName == null) {
      return null;
    }

    try {
      switch (typeName) {
      case TIMESTAMP_MILLIS:
        logicalType = TIMESTAMP_MILLIS_TYPE;
        break;
      case DECIMAL:
        logicalType = new Decimal(schema);
        break;
      case UUID:
        logicalType = UUID_TYPE;
        break;
      case DATE:
        logicalType = DATE_TYPE;
        break;
      case TIMESTAMP_MICROS:
        logicalType = TIMESTAMP_MICROS_TYPE;
        break;
      case TIME_MILLIS:
        logicalType = TIME_MILLIS_TYPE;
        break;
      case TIME_MICROS:
        logicalType = TIME_MICROS_TYPE;
        break;
      case LOCAL_TIMESTAMP_MICROS:
        logicalType = LOCAL_TIMESTAMP_MICROS_TYPE;
        break;
      case LOCAL_TIMESTAMP_MILLIS:
        logicalType = LOCAL_TIMESTAMP_MILLIS_TYPE;
        break;
      default:
        final LogicalTypeFactory typeFactory = REGISTERED_TYPES.get(typeName);
        logicalType = (typeFactory == null) ? null : typeFactory.fromSchema(schema);
        break;
      }

      // make sure the type is valid before returning it
      if (logicalType != null) {
        logicalType.validate(schema);
      }
    } catch (RuntimeException e) {
      LOG.debug("Invalid logical type found", e);
      if (throwErrors) {
        throw e;
      }
      LOG.warn("Ignoring invalid logical type for name: {}", typeName);
      // ignore invalid types
      return null;
    }

    return logicalType;
  }

  private static final String DECIMAL = "decimal";
  private static final String UUID = "uuid";
  private static final String DATE = "date";
  private static final String TIME_MILLIS = "time-millis";
  private static final String TIME_MICROS = "time-micros";
  private static final String TIMESTAMP_MILLIS = "timestamp-millis";
  private static final String TIMESTAMP_MICROS = "timestamp-micros";
  private static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  private static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

  /** Create a Decimal LogicalType with the given precision and scale 0 */
  public static Decimal decimal(int precision) {
    return decimal(precision, 0);
  }

  /** Create a Decimal LogicalType with the given precision and scale */
  public static Decimal decimal(int precision, int scale) {
    return new Decimal(precision, scale);
  }

  private static final LogicalType UUID_TYPE = new LogicalType("uuid");

  public static LogicalType uuid() {
    return UUID_TYPE;
  }

  private static final Date DATE_TYPE = new Date();

  public static Date date() {
    return DATE_TYPE;
  }

  private static final TimeMillis TIME_MILLIS_TYPE = new TimeMillis();

  public static TimeMillis timeMillis() {
    return TIME_MILLIS_TYPE;
  }

  private static final TimeMicros TIME_MICROS_TYPE = new TimeMicros();

  public static TimeMicros timeMicros() {
    return TIME_MICROS_TYPE;
  }

  private static final TimestampMillis TIMESTAMP_MILLIS_TYPE = new TimestampMillis();

  public static TimestampMillis timestampMillis() {
    return TIMESTAMP_MILLIS_TYPE;
  }

  private static final TimestampMicros TIMESTAMP_MICROS_TYPE = new TimestampMicros();

  public static TimestampMicros timestampMicros() {
    return TIMESTAMP_MICROS_TYPE;
  }

  private static final LocalTimestampMillis LOCAL_TIMESTAMP_MILLIS_TYPE = new LocalTimestampMillis();

  public static LocalTimestampMillis localTimestampMillis() {
    return LOCAL_TIMESTAMP_MILLIS_TYPE;
  }

  private static final LocalTimestampMicros LOCAL_TIMESTAMP_MICROS_TYPE = new LocalTimestampMicros();

  public static LocalTimestampMicros localTimestampMicros() {
    return LOCAL_TIMESTAMP_MICROS_TYPE;
  }

  /** Decimal represents arbitrary-precision fixed-scale decimal numbers */
  public static class Decimal extends LogicalType {
    private static final String PRECISION_PROP = "precision";
    private static final String SCALE_PROP = "scale";

    private final int precision;
    private final int scale;

    private Decimal(int precision, int scale) {
      super(DECIMAL);
      this.precision = precision;
      this.scale = scale;
    }

    private Decimal(Schema schema) {
      super("decimal");
      if (!hasProperty(schema, PRECISION_PROP)) {
        throw new IllegalArgumentException("Invalid decimal: missing precision");
      }

      this.precision = getInt(schema, PRECISION_PROP);

      if (hasProperty(schema, SCALE_PROP)) {
        this.scale = getInt(schema, SCALE_PROP);
      } else {
        this.scale = 0;
      }
    }

    @Override
    public Schema addToSchema(Schema schema) {
      super.addToSchema(schema);
      schema.addProp(PRECISION_PROP, precision);
      schema.addProp(SCALE_PROP, scale);
      return schema;
    }

    public int getPrecision() {
      return precision;
    }

    public int getScale() {
      return scale;
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      // validate the type
      if (schema.getType() != Schema.Type.FIXED && schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Logical type decimal must be backed by fixed or bytes");
      }
      if (precision <= 0) {
        throw new IllegalArgumentException("Invalid decimal precision: " + precision + " (must be positive)");
      } else if (precision > maxPrecision(schema)) {
        throw new IllegalArgumentException("fixed(" + schema.getFixedSize() + ") cannot store " + precision
            + " digits (max " + maxPrecision(schema) + ")");
      }
      if (scale < 0) {
        throw new IllegalArgumentException("Invalid decimal scale: " + scale + " (must be positive)");
      } else if (scale > precision) {
        throw new IllegalArgumentException(
            "Invalid decimal scale: " + scale + " (greater than precision: " + precision + ")");
      }
    }

    private long maxPrecision(Schema schema) {
      if (schema.getType() == Schema.Type.BYTES) {
        // not bounded
        return Integer.MAX_VALUE;
      } else if (schema.getType() == Schema.Type.FIXED) {
        int size = schema.getFixedSize();
        return Math.round(Math.floor(Math.log10(2) * (8 * size - 1)));
      } else {
        // not valid for any other type
        return 0;
      }
    }

    private boolean hasProperty(Schema schema, String name) {
      return (schema.getObjectProp(name) != null);
    }

    private int getInt(Schema schema, String name) {
      Object obj = schema.getObjectProp(name);
      if (obj instanceof Integer) {
        return (Integer) obj;
      }
      throw new IllegalArgumentException(
          "Expected int " + name + ": " + (obj == null ? "null" : obj + ":" + obj.getClass().getSimpleName()));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      Decimal decimal = (Decimal) o;

      if (precision != decimal.precision)
        return false;
      return scale == decimal.scale;
    }

    @Override
    public int hashCode() {
      int result = precision;
      result = 31 * result + scale;
      return result;
    }
  }

  /** Date represents a date without a time */
  public static class Date extends LogicalType {
    private Date() {
      super(DATE);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.INT) {
        throw new IllegalArgumentException("Date can only be used with an underlying int type");
      }
    }
  }

  /** TimeMillis represents a time in milliseconds without a date */
  public static class TimeMillis extends LogicalType {
    private TimeMillis() {
      super(TIME_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.INT) {
        throw new IllegalArgumentException("Time (millis) can only be used with an underlying int type");
      }
    }
  }

  /** TimeMicros represents a time in microseconds without a date */
  public static class TimeMicros extends LogicalType {
    private TimeMicros() {
      super(TIME_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Time (micros) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMillis represents a date and time in milliseconds */
  public static class TimestampMillis extends LogicalType {
    private TimestampMillis() {
      super(TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  /** TimestampMicros represents a date and time in microseconds */
  public static class TimestampMicros extends LogicalType {
    private TimestampMicros() {
      super(TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Timestamp (micros) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMillis extends LogicalType {
    private LocalTimestampMillis() {
      super(LOCAL_TIMESTAMP_MILLIS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (millis) can only be used with an underlying long type");
      }
    }
  }

  public static class LocalTimestampMicros extends LogicalType {
    private LocalTimestampMicros() {
      super(LOCAL_TIMESTAMP_MICROS);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.LONG) {
        throw new IllegalArgumentException("Local timestamp (micros) can only be used with an underlying long type");
      }
    }
  }

}
