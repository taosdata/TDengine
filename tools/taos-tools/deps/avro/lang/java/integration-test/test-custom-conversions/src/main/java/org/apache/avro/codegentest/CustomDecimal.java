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

package org.apache.avro.codegentest;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Wraps a BigDecimal just to demonstrate that it is possible to use custom
 * implementation classes with custom conversions.
 */
public class CustomDecimal implements Comparable<CustomDecimal> {

  private final BigDecimal internalValue;

  public CustomDecimal(BigInteger value, int scale) {
    internalValue = new BigDecimal(value, scale);
  }

  public byte[] toByteArray(int scale) {
    final BigDecimal correctlyScaledValue;
    if (scale != internalValue.scale()) {
      correctlyScaledValue = internalValue.setScale(scale, RoundingMode.HALF_UP);
    } else {
      correctlyScaledValue = internalValue;
    }
    return correctlyScaledValue.unscaledValue().toByteArray();

  }

  int signum() {
    return internalValue.signum();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    CustomDecimal that = (CustomDecimal) o;

    return internalValue.equals(that.internalValue);
  }

  @Override
  public int hashCode() {
    return internalValue.hashCode();
  }

  @Override
  public int compareTo(CustomDecimal o) {
    return this.internalValue.compareTo(o.internalValue);
  }
}
