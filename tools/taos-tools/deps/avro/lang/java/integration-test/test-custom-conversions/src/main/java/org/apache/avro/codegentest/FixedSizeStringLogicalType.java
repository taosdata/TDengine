/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.codegentest;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import static java.util.Objects.isNull;

public class FixedSizeStringLogicalType extends LogicalType {

  private static final String MIN_LENGTH = "minLength";
  private static final String MAX_LENGTH = "maxLength";

  private final Integer minLength;
  private final Integer maxLength;

  public FixedSizeStringLogicalType() {
    super(FixedSizeStringFactory.NAME);
    this.minLength = Integer.MIN_VALUE;
    this.maxLength = Integer.MAX_VALUE;
  }

  public FixedSizeStringLogicalType(Schema schema) {
    super(FixedSizeStringFactory.NAME);
    this.minLength = getInteger(schema, MIN_LENGTH);
    this.maxLength = getInteger(schema, MAX_LENGTH);
  }

  public Integer getMinLength() {
    return minLength;
  }

  public Integer getMaxLength() {
    return maxLength;
  }

  private int getInteger(Schema schema, String name) {
    Object value = schema.getObjectProp(name);
    if (isNull(value)) {
      throw new IllegalArgumentException(String.format("Invalid %s: missing %s", FixedSizeStringFactory.NAME, name));
    }
    if (value instanceof Integer) {
      return (int) value;
    }
    throw new IllegalArgumentException(
        String.format("Expected integer %s but get %s", name, value.getClass().getSimpleName()));
  }
}
