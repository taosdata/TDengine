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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.avro.Schema.Field;

/** Schemas used by other tests in this package. Therefore package protected. */
public class TestSchemas {

  static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  static final Schema BYTES_SCHEMA = Schema.create(Schema.Type.BYTES);

  static final Schema INT_ARRAY_SCHEMA = Schema.createArray(INT_SCHEMA);
  static final Schema LONG_ARRAY_SCHEMA = Schema.createArray(LONG_SCHEMA);
  static final Schema STRING_ARRAY_SCHEMA = Schema.createArray(STRING_SCHEMA);

  static final Schema INT_MAP_SCHEMA = Schema.createMap(INT_SCHEMA);
  static final Schema LONG_MAP_SCHEMA = Schema.createMap(LONG_SCHEMA);
  static final Schema STRING_MAP_SCHEMA = Schema.createMap(STRING_SCHEMA);

  static final Schema ENUM1_AB_SCHEMA = Schema.createEnum("Enum1", null, null, list("A", "B"));
  static final Schema ENUM1_ABC_SCHEMA = Schema.createEnum("Enum1", null, null, list("A", "B", "C"));
  static final Schema ENUM1_BC_SCHEMA = Schema.createEnum("Enum1", null, null, list("B", "C"));
  static final Schema ENUM2_AB_SCHEMA = Schema.createEnum("Enum2", null, null, list("A", "B"));
  static final Schema ENUM_ABC_ENUM_DEFAULT_A_SCHEMA = Schema.createEnum("Enum", null, null, list("A", "B", "C"), "A");
  static final Schema ENUM_AB_ENUM_DEFAULT_A_SCHEMA = Schema.createEnum("Enum", null, null, list("A", "B"), "A");
  static final Schema ENUM_ABC_ENUM_DEFAULT_A_RECORD = Schema.createRecord("Record", null, null, false);
  static final Schema ENUM_AB_ENUM_DEFAULT_A_RECORD = Schema.createRecord("Record", null, null, false);
  static final Schema ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD = Schema.createRecord("Record", null, null, false);
  static final Schema ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD = Schema.createRecord("Record", null, null, false);
  static {
    ENUM_ABC_ENUM_DEFAULT_A_RECORD.setFields(
        list(new Schema.Field("Field", Schema.createEnum("Schema", null, null, list("A", "B", "C"), "A"), null, null)));
    ENUM_AB_ENUM_DEFAULT_A_RECORD.setFields(
        list(new Schema.Field("Field", Schema.createEnum("Schema", null, null, list("A", "B"), "A"), null, null)));
    ENUM_ABC_FIELD_DEFAULT_B_ENUM_DEFAULT_A_RECORD.setFields(
        list(new Schema.Field("Field", Schema.createEnum("Schema", null, null, list("A", "B", "C"), "A"), null, "B")));
    ENUM_AB_FIELD_DEFAULT_A_ENUM_DEFAULT_B_RECORD.setFields(
        list(new Schema.Field("Field", Schema.createEnum("Schema", null, null, list("A", "B"), "B"), null, "A")));
  }

  static final Schema EMPTY_UNION_SCHEMA = Schema.createUnion(new ArrayList<>());
  static final Schema NULL_UNION_SCHEMA = Schema.createUnion(list(NULL_SCHEMA));
  static final Schema INT_UNION_SCHEMA = Schema.createUnion(list(INT_SCHEMA));
  static final Schema LONG_UNION_SCHEMA = Schema.createUnion(list(LONG_SCHEMA));
  static final Schema FLOAT_UNION_SCHEMA = Schema.createUnion(list(FLOAT_SCHEMA));
  static final Schema DOUBLE_UNION_SCHEMA = Schema.createUnion(list(DOUBLE_SCHEMA));
  static final Schema STRING_UNION_SCHEMA = Schema.createUnion(list(STRING_SCHEMA));
  static final Schema BYTES_UNION_SCHEMA = Schema.createUnion(list(BYTES_SCHEMA));
  static final Schema INT_STRING_UNION_SCHEMA = Schema.createUnion(list(INT_SCHEMA, STRING_SCHEMA));
  static final Schema STRING_INT_UNION_SCHEMA = Schema.createUnion(list(STRING_SCHEMA, INT_SCHEMA));
  static final Schema INT_FLOAT_UNION_SCHEMA = Schema.createUnion(list(INT_SCHEMA, FLOAT_SCHEMA));
  static final Schema INT_LONG_UNION_SCHEMA = Schema.createUnion(list(INT_SCHEMA, LONG_SCHEMA));
  static final Schema INT_LONG_FLOAT_DOUBLE_UNION_SCHEMA = Schema
      .createUnion(list(INT_SCHEMA, LONG_SCHEMA, FLOAT_SCHEMA, DOUBLE_SCHEMA));

  static final Schema NULL_INT_ARRAY_UNION_SCHEMA = Schema.createUnion(list(NULL_SCHEMA, INT_ARRAY_SCHEMA));
  static final Schema NULL_INT_MAP_UNION_SCHEMA = Schema.createUnion(list(NULL_SCHEMA, INT_MAP_SCHEMA));

  // Non recursive records:
  static final Schema EMPTY_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema EMPTY_RECORD2 = Schema.createRecord("Record2", null, null, false);
  static final Schema A_INT_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_LONG_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_INT_B_INT_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_RECORD1 = // DTYPE means TYPE with default value
      Schema.createRecord("Record1", null, null, false);
  static final Schema A_INT_B_DINT_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DINT_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DFIXED_4_BYTES_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DFIXED_8_BYTES_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DINT_STRING_UNION_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DINT_UNION_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DENUM_1_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema A_DINT_B_DENUM_2_RECORD1 = Schema.createRecord("Record1", null, null, false);

  static final Schema FIXED_4_BYTES = Schema.createFixed("Fixed", null, null, 4);
  static final Schema FIXED_8_BYTES = Schema.createFixed("Fixed", null, null, 8);

  static final Schema NS_RECORD1 = Schema.createRecord("Record1", null, null, false);
  static final Schema NS_RECORD2 = Schema.createRecord("Record1", null, null, false);
  static final Schema NS_INNER_RECORD1 = Schema.createRecord("InnerRecord1", null, "ns1", false);
  static final Schema NS_INNER_RECORD2 = Schema.createRecord("InnerRecord1", null, "ns2", false);

  static {
    EMPTY_RECORD1.setFields(Collections.emptyList());
    EMPTY_RECORD2.setFields(Collections.emptyList());
    A_INT_RECORD1.setFields(list(new Field("a", INT_SCHEMA, null, null)));
    A_LONG_RECORD1.setFields(list(new Field("a", LONG_SCHEMA, null, null)));
    A_INT_B_INT_RECORD1.setFields(list(new Field("a", INT_SCHEMA, null, null), new Field("b", INT_SCHEMA, null, null)));
    A_DINT_RECORD1.setFields(list(new Field("a", INT_SCHEMA, null, 0)));
    A_INT_B_DINT_RECORD1.setFields(list(new Field("a", INT_SCHEMA, null, null), new Field("b", INT_SCHEMA, null, 0)));
    A_DINT_B_DINT_RECORD1.setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", INT_SCHEMA, null, 0)));
    A_DINT_B_DFIXED_4_BYTES_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", FIXED_4_BYTES, null, null)));
    A_DINT_B_DFIXED_8_BYTES_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", FIXED_8_BYTES, null, null)));
    A_DINT_B_DINT_STRING_UNION_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", INT_STRING_UNION_SCHEMA, null, 0)));
    A_DINT_B_DINT_UNION_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", INT_UNION_SCHEMA, null, 0)));
    A_DINT_B_DENUM_1_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", ENUM1_AB_SCHEMA, null, null)));
    A_DINT_B_DENUM_2_RECORD1
        .setFields(list(new Field("a", INT_SCHEMA, null, 0), new Field("b", ENUM2_AB_SCHEMA, null, null)));

    NS_INNER_RECORD1.setFields(list(new Schema.Field("a", INT_SCHEMA)));
    NS_INNER_RECORD2.setFields(list(new Schema.Field("a", INT_SCHEMA)));

    NS_RECORD1
        .setFields(list(new Schema.Field("f1", Schema.createUnion(NULL_SCHEMA, Schema.createArray(NS_INNER_RECORD1)))));
    NS_RECORD2
        .setFields(list(new Schema.Field("f1", Schema.createUnion(NULL_SCHEMA, Schema.createArray(NS_INNER_RECORD2)))));
  }

  // Recursive records
  static final Schema INT_LIST_RECORD = Schema.createRecord("List", null, null, false);
  static final Schema LONG_LIST_RECORD = Schema.createRecord("List", null, null, false);
  static {
    INT_LIST_RECORD
        .setFields(list(new Field("head", INT_SCHEMA, null, null), new Field("tail", INT_LIST_RECORD, null, null)));
    LONG_LIST_RECORD
        .setFields(list(new Field("head", LONG_SCHEMA, null, null), new Field("tail", LONG_LIST_RECORD, null, null)));
  }

  // -----------------------------------------------------------------------------------------------

  /** Reader/writer schema pair. */
  static final class ReaderWriter {
    private final Schema mReader;
    private final Schema mWriter;

    public ReaderWriter(final Schema reader, final Schema writer) {
      mReader = reader;
      mWriter = writer;
    }

    public Schema getReader() {
      return mReader;
    }

    public Schema getWriter() {
      return mWriter;
    }
  }

  /** Borrowed from the Guava library. */
  static <E> ArrayList<E> list(E... elements) {
    final ArrayList<E> list = new ArrayList<>();
    Collections.addAll(list, elements);
    return list;
  }

  static void assertSchemaContains(Schema schemaSubset, Schema original) {
    String subset = schemaSubset.toString(false);
    String whole = original.toString(false);
    assertTrue(String.format("Subset '%s' not found in '%s'", subset, whole), whole.contains(subset));
  }

}
