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

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Objects.nonNull;

public class FixedSizeStringConversion extends Conversion<FixedSizeString> {
  @Override
  public Class<FixedSizeString> getConvertedType() {
    return FixedSizeString.class;
  }

  @Override
  public String getLogicalTypeName() {
    return FixedSizeStringFactory.NAME;
  }

  @Override
  public FixedSizeString fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    String stringValue = StandardCharsets.UTF_8.decode(value).toString();
    validate(stringValue, type);
    return new FixedSizeString(stringValue);
  }

  @Override
  public ByteBuffer toBytes(FixedSizeString value, Schema schema, LogicalType type) {
    validate(value.getValue(), type);
    return StandardCharsets.UTF_8.encode(value.getValue());
  }

  private void validate(String value, LogicalType logicalType) {
    FixedSizeStringLogicalType fixedSizeStringLogicalType = (FixedSizeStringLogicalType) logicalType;
    int minValue = fixedSizeStringLogicalType.getMinLength();
    int maxValue = fixedSizeStringLogicalType.getMaxLength();
    if (nonNull(value) && (value.length() < minValue || value.length() > maxValue)) {
      throw new IllegalArgumentException(
          String.format("Incorrect size. Must satisfy %d <= value <= %d", minValue, maxValue));
    }
  }
}
