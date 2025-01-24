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

import static org.apache.avro.TestSchemaCompatibility.validateIncompatibleSchemas;
import static org.apache.avro.TestSchemas.*;

import java.util.Arrays;

import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestSchemaCompatibilityMissingEnumSymbols {

  private static final Schema RECORD1_WITH_ENUM_AB = SchemaBuilder.record("Record1").fields() //
      .name("field1").type(ENUM1_AB_SCHEMA).noDefault() //
      .endRecord();
  private static final Schema RECORD1_WITH_ENUM_ABC = SchemaBuilder.record("Record1").fields() //
      .name("field1").type(ENUM1_ABC_SCHEMA).noDefault() //
      .endRecord();

  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = { //
        { ENUM1_AB_SCHEMA, ENUM1_ABC_SCHEMA, "[C]", "/symbols" },
        { ENUM1_BC_SCHEMA, ENUM1_ABC_SCHEMA, "[A]", "/symbols" },
        { RECORD1_WITH_ENUM_AB, RECORD1_WITH_ENUM_ABC, "[C]", "/fields/0/type/symbols" } };
    return Arrays.asList(fields);
  }

  @Parameter(0)
  public Schema reader;
  @Parameter(1)
  public Schema writer;
  @Parameter(2)
  public String details;
  @Parameter(3)
  public String location;

  @Test
  public void testTypeMismatchSchemas() throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, details, location);
  }
}
