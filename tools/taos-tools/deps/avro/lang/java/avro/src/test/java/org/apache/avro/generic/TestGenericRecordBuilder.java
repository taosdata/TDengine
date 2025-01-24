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
package org.apache.avro.generic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the GenericRecordBuilder class.
 */
public class TestGenericRecordBuilder {
  @Test
  public void testGenericBuilder() {
    Schema schema = recordSchema();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

    // Verify that builder has no fields set after initialization:
    for (Field field : schema.getFields()) {
      Assert.assertFalse("RecordBuilder should not have field " + field.name(), builder.has(field.name()));
      Assert.assertNull("Field " + field.name() + " should be null", builder.get(field.name()));
    }

    // Set field in builder:
    builder.set("intField", 1);
    List<String> anArray = Arrays.asList("one", "two", "three");
    builder.set("anArray", anArray);
    Assert.assertTrue("anArray should be set", builder.has("anArray"));
    Assert.assertEquals(anArray, builder.get("anArray"));
    Assert.assertFalse("id should not be set", builder.has("id"));
    Assert.assertNull(builder.get("id"));

    // Build the record, and verify that fields are set:
    Record record = builder.build();
    Assert.assertEquals(1, record.get("intField"));
    Assert.assertEquals(anArray, record.get("anArray"));
    Assert.assertNotNull(record.get("id"));
    Assert.assertEquals("0", record.get("id").toString());

    // Test copy constructors:
    Assert.assertEquals(builder, new GenericRecordBuilder(builder));
    Assert.assertEquals(record, new GenericRecordBuilder(record).build());

    // Test clear:
    builder.clear("intField");
    Assert.assertFalse(builder.has("intField"));
    Assert.assertNull(builder.get("intField"));
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void attemptToSetNonNullableFieldToNull() {
    new GenericRecordBuilder(recordSchema()).set("intField", null);
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void buildWithoutSettingRequiredFields1() {
    new GenericRecordBuilder(recordSchema()).build();
  }

  @Test()
  public void buildWithoutSettingRequiredFields2() {
    try {
      new GenericRecordBuilder(recordSchema()).set("anArray", Collections.singletonList("one")).build();
      Assert.fail("Should have thrown " + AvroRuntimeException.class.getCanonicalName());
    } catch (AvroRuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("intField"));
    }
  }

  /** Creates a test record schema */
  private static Schema recordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("id", Schema.create(Type.STRING), null, "0"));
    fields.add(new Field("intField", Schema.create(Type.INT), null, null));
    fields.add(new Field("anArray", Schema.createArray(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("optionalInt",
        Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.INT))), null, Schema.NULL_VALUE));
    Schema schema = Schema.createRecord("Foo", "test", "mytest", false);
    schema.setFields(fields);
    return schema;
  }
}
