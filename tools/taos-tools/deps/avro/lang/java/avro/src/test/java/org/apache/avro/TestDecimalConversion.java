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

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static java.math.RoundingMode.HALF_EVEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestDecimalConversion {

  private static final Conversion<BigDecimal> CONVERSION = new Conversions.DecimalConversion();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Schema smallerSchema;
  private LogicalType smallerLogicalType;
  private Schema largerSchema;
  private LogicalType largerLogicalType;

  @Before
  public void setup() {
    smallerSchema = Schema.createFixed("smallFixed", null, null, 3);
    smallerSchema.addProp("logicalType", "decimal");
    smallerSchema.addProp("precision", 5);
    smallerSchema.addProp("scale", 2);
    smallerLogicalType = LogicalTypes.fromSchema(smallerSchema);

    largerSchema = Schema.createFixed("largeFixed", null, null, 12);
    largerSchema.addProp("logicalType", "decimal");
    largerSchema.addProp("precision", 28);
    largerSchema.addProp("scale", 15);
    largerLogicalType = LogicalTypes.fromSchema(largerSchema);
  }

  @Test
  public void testToFromBytes() {
    final BigDecimal value = BigDecimal.valueOf(10.99).setScale(15, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, largerSchema, largerLogicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFromBytesMaxPrecision() {
    final BigDecimal value = new BigDecimal("4567335489766.99834").setScale(15, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, largerSchema, largerLogicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToBytesPrecisionError() {
    final BigDecimal value = new BigDecimal("1.07046455859736525E+18").setScale(15, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with precision 34 as max precision 28");
    CONVERSION.toBytes(value, largerSchema, largerLogicalType);
  }

  @Test
  public void testToBytesFixedSmallerScale() {
    final BigDecimal value = new BigDecimal("99892.1234").setScale(10, HALF_EVEN);
    final ByteBuffer byteBuffer = CONVERSION.toBytes(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromBytes(byteBuffer, largerSchema, largerLogicalType);
    assertEquals(new BigDecimal("99892.123400000000000"), result);
  }

  @Test
  public void testToBytesScaleError() {
    final BigDecimal value = new BigDecimal("4567335489766.989989998435899453").setScale(16, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with scale 16 as scale 15 without rounding");
    CONVERSION.toBytes(value, largerSchema, largerLogicalType);
  }

  @Test
  public void testToFromFixed() {
    final BigDecimal value = new BigDecimal("3").setScale(15, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, largerSchema, largerLogicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFromFixedMaxPrecision() {
    final BigDecimal value = new BigDecimal("4567335489766.99834").setScale(15, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, largerSchema, largerLogicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFixedPrecisionError() {
    final BigDecimal value = new BigDecimal("1.07046455859736525E+18").setScale(15, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with precision 34 as max precision 28");
    CONVERSION.toFixed(value, largerSchema, largerLogicalType);
  }

  @Test
  public void testToFromFixedSmallerScale() {
    final BigDecimal value = new BigDecimal("99892.1234").setScale(10, HALF_EVEN);
    final GenericFixed fixed = CONVERSION.toFixed(value, largerSchema, largerLogicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, largerSchema, largerLogicalType);
    assertEquals(new BigDecimal("99892.123400000000000"), result);
  }

  @Test
  public void testToFixedScaleError() {
    final BigDecimal value = new BigDecimal("4567335489766.3453453453453453453453").setScale(16, HALF_EVEN);
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with scale 16 as scale 15 without rounding");
    CONVERSION.toFixed(value, largerSchema, largerLogicalType);
  }

  @Test
  public void testToFromFixedMatchScaleAndPrecision() {
    final BigDecimal value = new BigDecimal("123.45");
    final GenericFixed fixed = CONVERSION.toFixed(value, smallerSchema, smallerLogicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, smallerSchema, smallerLogicalType);
    assertEquals(value, result);
  }

  @Test
  public void testToFromFixedRepresentedInLogicalTypeAllowRoundUnneccesary() {
    final BigDecimal value = new BigDecimal("123.4500");
    final GenericFixed fixed = CONVERSION.toFixed(value, smallerSchema, smallerLogicalType);
    final BigDecimal result = CONVERSION.fromFixed(fixed, smallerSchema, smallerLogicalType);
    assertEquals(new BigDecimal("123.45"), result);
  }

  @Test
  public void testToFromFixedPrecisionErrorAfterAdjustingScale() {
    final BigDecimal value = new BigDecimal("1234.560");
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage(
        "Cannot encode decimal with precision 6 as max precision 5. This is after safely adjusting scale from 3 to required 2");
    CONVERSION.toFixed(value, smallerSchema, smallerLogicalType);
  }

  @Test
  public void testToFixedRepresentedInLogicalTypeErrorIfRoundingRequired() {
    final BigDecimal value = new BigDecimal("123.456");
    expectedException.expect(AvroTypeException.class);
    expectedException.expectMessage("Cannot encode decimal with scale 3 as scale 2 without rounding");
    CONVERSION.toFixed(value, smallerSchema, smallerLogicalType);
  }

  @Test
  public void testImportanceOfEnsuringCorrectScaleWhenConvertingFixed() {
    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) smallerLogicalType;

    final BigDecimal bigDecimal = new BigDecimal("1234.5");
    assertEquals(decimal.getPrecision(), bigDecimal.precision());
    assertTrue(decimal.getScale() >= bigDecimal.scale());

    final byte[] bytes = bigDecimal.unscaledValue().toByteArray();

    final BigDecimal fromFixed = CONVERSION.fromFixed(new GenericData.Fixed(smallerSchema, bytes), smallerSchema,
        decimal);

    assertNotEquals(0, bigDecimal.compareTo(fromFixed));
    assertNotEquals(bigDecimal, fromFixed);

    assertEquals(new BigDecimal("123.45"), fromFixed);
  }

  @Test
  public void testImportanceOfEnsuringCorrectScaleWhenConvertingBytes() {
    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) smallerLogicalType;

    final BigDecimal bigDecimal = new BigDecimal("1234.5");
    assertEquals(decimal.getPrecision(), bigDecimal.precision());
    assertTrue(decimal.getScale() >= bigDecimal.scale());

    final byte[] bytes = bigDecimal.unscaledValue().toByteArray();

    final BigDecimal fromBytes = CONVERSION.fromBytes(ByteBuffer.wrap(bytes), smallerSchema, decimal);

    assertNotEquals(0, bigDecimal.compareTo(fromBytes));
    assertNotEquals(bigDecimal, fromBytes);

    assertEquals(new BigDecimal("123.45"), fromBytes);
  }
}
