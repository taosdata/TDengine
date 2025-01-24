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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class CustomEnumLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
  private static final LogicalType CUSTOM_ENUM = new CustomEnumLogicalType("custom-enum");

  @Override
  public LogicalType fromSchema(Schema schema) {
    return CUSTOM_ENUM;
  }

  @Override
  public String getTypeName() {
    return CUSTOM_ENUM.getName();
  }

  public static class CustomEnumLogicalType extends LogicalType {

    public CustomEnumLogicalType(String logicalTypeName) {
      super(logicalTypeName);
    }

    @Override
    public void validate(Schema schema) {
      super.validate(schema);
      if (schema.getType() != Schema.Type.ENUM) {
        throw new IllegalArgumentException("Custom Enum can only be used with an underlying Enum type");
      }
    }
  }
}
