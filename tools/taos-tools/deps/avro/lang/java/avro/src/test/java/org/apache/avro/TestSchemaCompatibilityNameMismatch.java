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
public class TestSchemaCompatibilityNameMismatch {

  private static final Schema FIXED_4_ANOTHER_NAME = Schema.createFixed("AnotherName", null, null, 4);

  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = { //
        { ENUM1_AB_SCHEMA, ENUM2_AB_SCHEMA, "expected: Enum2", "/name" },
        { EMPTY_RECORD2, EMPTY_RECORD1, "expected: Record1", "/name" },
        { FIXED_4_BYTES, FIXED_4_ANOTHER_NAME, "expected: AnotherName", "/name" },
        { A_DINT_B_DENUM_1_RECORD1, A_DINT_B_DENUM_2_RECORD1, "expected: Enum2", "/fields/1/type/name" } };
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
  public void testNameMismatchSchemas() throws Exception {
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.NAME_MISMATCH, details, location);
  }
}
