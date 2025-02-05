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
import org.apache.avro.specific.SpecificData;

/**
 * Logical types provides an opt-in way to extend Avro's types. Logical types
 * specify a way of representing a high-level type as a base Avro type. For
 * example, a date is specified as the number of days after the unix epoch (or
 * before using a negative value). This enables extensions to Avro's type system
 * without breaking binary compatibility. Older versions see the base type and
 * ignore the logical type.
 */
public class LogicalType {

  public static final String LOGICAL_TYPE_PROP = "logicalType";

  private static final String[] INCOMPATIBLE_PROPS = new String[] { GenericData.STRING_PROP, SpecificData.CLASS_PROP,
      SpecificData.KEY_CLASS_PROP, SpecificData.ELEMENT_PROP };

  private final String name;

  public LogicalType(String logicalTypeName) {
    this.name = logicalTypeName.intern();
  }

  /**
   * Get the name of this logical type.
   * <p>
   * This name is set as the Schema property "logicalType".
   *
   * @return the String name of the logical type
   */
  public String getName() {
    return name;
  }

  /**
   * Add this logical type to the given Schema.
   * <p>
   * The "logicalType" property will be set to this type's name, and other
   * type-specific properties may be added. The Schema is first validated to
   * ensure it is compatible.
   *
   * @param schema a Schema
   * @return the modified Schema
   * @throws IllegalArgumentException if the type and schema are incompatible
   */
  public Schema addToSchema(Schema schema) {
    validate(schema);
    schema.addProp(LOGICAL_TYPE_PROP, name);
    schema.setLogicalType(this);
    return schema;
  }

  /**
   * Validate this logical type for the given Schema.
   * <p>
   * This will throw an exception if the Schema is incompatible with this type.
   * For example, a date is stored as an int and is incompatible with a fixed
   * Schema.
   *
   * @param schema a Schema
   * @throws IllegalArgumentException if the type and schema are incompatible
   */
  public void validate(Schema schema) {
    for (String incompatible : INCOMPATIBLE_PROPS) {
      if (schema.getProp(incompatible) != null) {
        throw new IllegalArgumentException(LOGICAL_TYPE_PROP + " cannot be used with " + incompatible);
      }
    }
  }

}
