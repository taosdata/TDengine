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

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;

public class CustomEnumConversion extends Conversion<CustomEnumType> {

  @Override
  public Class<CustomEnumType> getConvertedType() {
    return CustomEnumType.class;
  }

  @Override
  public Schema getRecommendedSchema() {
    return new LogicalType("custom-enum").addToSchema(Schema.create(Schema.Type.ENUM));
  }

  @Override
  public String getLogicalTypeName() {
    return "custom-enum";
  }

  @Override
  public CustomEnumType fromEnumSymbol(GenericEnumSymbol value, Schema schema, LogicalType type) {
    return new CustomEnumType(value.toString());
  }

  @Override
  public GenericEnumSymbol toEnumSymbol(CustomEnumType value, Schema schema, LogicalType type) {
    return new GenericData.EnumSymbol(schema, value.getUnderlying());
  }
}
