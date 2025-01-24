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
public class TestSchemaCompatibilityTypeMismatch {
  @Parameters(name = "r: {0} | w: {1}")
  public static Iterable<Object[]> data() {
    Object[][] fields = { //
        { NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/" },
        { NULL_SCHEMA, LONG_SCHEMA, "reader type: NULL not compatible with writer type: LONG", "/" },

        { BOOLEAN_SCHEMA, INT_SCHEMA, "reader type: BOOLEAN not compatible with writer type: INT", "/" },

        { INT_SCHEMA, NULL_SCHEMA, "reader type: INT not compatible with writer type: NULL", "/" },
        { INT_SCHEMA, BOOLEAN_SCHEMA, "reader type: INT not compatible with writer type: BOOLEAN", "/" },
        { INT_SCHEMA, LONG_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/" },
        { INT_SCHEMA, FLOAT_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/" },
        { INT_SCHEMA, DOUBLE_SCHEMA, "reader type: INT not compatible with writer type: DOUBLE", "/" },

        { LONG_SCHEMA, FLOAT_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/" },
        { LONG_SCHEMA, DOUBLE_SCHEMA, "reader type: LONG not compatible with writer type: DOUBLE", "/" },

        { FLOAT_SCHEMA, DOUBLE_SCHEMA, "reader type: FLOAT not compatible with writer type: DOUBLE", "/" },

        { DOUBLE_SCHEMA, STRING_SCHEMA, "reader type: DOUBLE not compatible with writer type: STRING", "/" },

        { FIXED_4_BYTES, STRING_SCHEMA, "reader type: FIXED not compatible with writer type: STRING", "/" },

        { STRING_SCHEMA, BOOLEAN_SCHEMA, "reader type: STRING not compatible with writer type: BOOLEAN", "/" },
        { STRING_SCHEMA, INT_SCHEMA, "reader type: STRING not compatible with writer type: INT", "/" },

        { BYTES_SCHEMA, NULL_SCHEMA, "reader type: BYTES not compatible with writer type: NULL", "/" },
        { BYTES_SCHEMA, INT_SCHEMA, "reader type: BYTES not compatible with writer type: INT", "/" },

        { A_INT_RECORD1, INT_SCHEMA, "reader type: RECORD not compatible with writer type: INT", "/" },

        { INT_ARRAY_SCHEMA, LONG_ARRAY_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/items" },
        { INT_MAP_SCHEMA, INT_ARRAY_SCHEMA, "reader type: MAP not compatible with writer type: ARRAY", "/" },
        { INT_ARRAY_SCHEMA, INT_MAP_SCHEMA, "reader type: ARRAY not compatible with writer type: MAP", "/" },
        { INT_MAP_SCHEMA, LONG_MAP_SCHEMA, "reader type: INT not compatible with writer type: LONG", "/values" },

        { INT_SCHEMA, ENUM2_AB_SCHEMA, "reader type: INT not compatible with writer type: ENUM", "/" },
        { ENUM2_AB_SCHEMA, INT_SCHEMA, "reader type: ENUM not compatible with writer type: INT", "/" },

        { FLOAT_SCHEMA, INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA,
            "reader type: FLOAT not compatible with writer type: DOUBLE", "/" },
        { LONG_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: LONG not compatible with writer type: FLOAT", "/" },
        { INT_SCHEMA, INT_FLOAT_UNION_SCHEMA, "reader type: INT not compatible with writer type: FLOAT", "/" },

        { INT_LIST_RECORD, LONG_LIST_RECORD, "reader type: INT not compatible with writer type: LONG",
            "/fields/0/type" },

        { NULL_SCHEMA, INT_SCHEMA, "reader type: NULL not compatible with writer type: INT", "/" } };
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
    validateIncompatibleSchemas(reader, writer, SchemaIncompatibilityType.TYPE_MISMATCH, details, location);
  }
}
