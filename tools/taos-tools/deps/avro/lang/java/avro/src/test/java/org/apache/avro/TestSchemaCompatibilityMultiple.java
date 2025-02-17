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

import java.util.Arrays;
import java.util.List;

import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;
import org.junit.Test;

public class TestSchemaCompatibilityMultiple {

  @Test
  public void testMultipleIncompatibilities() throws Exception {
    Schema reader = SchemaBuilder.record("base").fields()
        // 0
        .name("check_enum_symbols_field").type().enumeration("check_enum_symbols_type").symbols("A", "C").noDefault()
        // 1
        .name("check_enum_name_field").type().enumeration("check_enum_name_type").symbols("A", "B", "C", "D")
        .noDefault()
        // 2
        .name("type_mismatch_field").type().stringType().noDefault()
        // 3
        .name("sub_record").type().record("sub_record_type").fields()
        // 3.0
        .name("identical_1_field").type().longType().longDefault(42L)
        // 3.1
        .name("extra_no_default_field").type().longType().noDefault()
        // 3.2
        .name("fixed_length_mismatch_field").type().fixed("fixed_length_mismatch_type").size(4).noDefault()
        // 3.3
        .name("union_missing_branches_field").type().unionOf().booleanType().endUnion().noDefault()
        // 3.4
        .name("reader_union_does_not_support_type_field").type().unionOf().booleanType().endUnion().noDefault()
        // 3.5
        .name("record_fqn_mismatch_field").type().record("recordA").namespace("not_nsA").fields()
        // 3.5.0
        .name("A_field_0").type().booleanType().booleanDefault(true)
        // 3.5.1
        .name("array_type_mismatch_field").type().array().items().stringType().noDefault()
        // EOR
        .endRecord().noDefault()
        // EOR
        .endRecord().noDefault()
        // EOR
        .endRecord();

    Schema writer = SchemaBuilder.record("base").fields()
        // 0
        .name("check_enum_symbols_field").type().enumeration("check_enum_symbols_type").symbols("A", "B", "C", "D")
        .noDefault()
        // 1
        .name("check_enum_name_field").type().enumeration("check_enum_name_type_ERR").symbols("A", "B", "C", "D")
        .noDefault()
        // 2
        .name("type_mismatch_field").type().longType().noDefault()
        // 3
        .name("sub_record").type().record("sub_record_type").fields()
        // 3.0
        .name("identical_1_field").type().longType().longDefault(42L)
        // 3.1
        // MISSING FIELD
        // 3.2
        .name("fixed_length_mismatch_field").type().fixed("fixed_length_mismatch_type").size(8).noDefault()
        // 3.3
        .name("union_missing_branches_field").type().unionOf().booleanType().and().doubleType().and().stringType()
        .endUnion().noDefault()
        // 3.4
        .name("reader_union_does_not_support_type_field").type().longType().noDefault()
        // 3.5
        .name("record_fqn_mismatch_field").type().record("recordA").namespace("nsA").fields()
        // 3.5.0
        .name("A_field_0").type().booleanType().booleanDefault(true)
        // 3.5.1
        .name("array_type_mismatch_field").type().array().items().booleanType().noDefault()
        // EOR
        .endRecord().noDefault()
        // EOR
        .endRecord().noDefault()
        // EOR
        .endRecord();

    List<SchemaIncompatibilityType> types = Arrays.asList(SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS,
        SchemaIncompatibilityType.NAME_MISMATCH, SchemaIncompatibilityType.TYPE_MISMATCH,
        SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE, SchemaIncompatibilityType.FIXED_SIZE_MISMATCH,
        SchemaIncompatibilityType.MISSING_UNION_BRANCH, SchemaIncompatibilityType.MISSING_UNION_BRANCH,
        SchemaIncompatibilityType.MISSING_UNION_BRANCH, SchemaIncompatibilityType.TYPE_MISMATCH);
    List<String> details = Arrays.asList("[B, D]", "expected: check_enum_name_type_ERR",
        "reader type: STRING not compatible with writer type: LONG", "extra_no_default_field", "expected: 8, found: 4",
        "reader union lacking writer type: DOUBLE", "reader union lacking writer type: STRING",
        "reader union lacking writer type: LONG", "reader type: STRING not compatible with writer type: BOOLEAN");
    List<String> location = Arrays.asList("/fields/0/type/symbols", "/fields/1/type/name", "/fields/2/type",
        "/fields/3/type/fields/1", "/fields/3/type/fields/2/type/size", "/fields/3/type/fields/3/type/1",
        "/fields/3/type/fields/3/type/2", "/fields/3/type/fields/4/type",
        "/fields/3/type/fields/5/type/fields/1/type/items");

    validateIncompatibleSchemas(reader, writer, types, details, location);
  }
}
