/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.file;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Test;

public class TestSeekableByteArrayInput {

  private byte[] getSerializedMessage(IndexedRecord message, Schema schema) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
    SpecificDatumWriter<IndexedRecord> writer = new SpecificDatumWriter<>();
    try (DataFileWriter<IndexedRecord> dfw = new DataFileWriter<>(writer).create(schema, baos)) {
      dfw.append(message);
    }
    return baos.toByteArray();
  }

  private Schema getTestSchema() throws Exception {
    Schema schema = Schema.createRecord("TestRecord", "this is a test record", "org.apache.avro.file", false);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("name", Schema.create(Type.STRING), "this is a test field"));
    schema.setFields(fields);
    return schema;
  }

  @Test
  public void testSerialization() throws Exception {
    Schema testSchema = getTestSchema();
    GenericRecord message = new Record(testSchema);
    message.put("name", "testValue");

    byte[] data = getSerializedMessage(message, testSchema);

    GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(testSchema);
    final IndexedRecord result;
    try (SeekableInput in = new SeekableByteArrayInput(data);
        FileReader<IndexedRecord> dfr = DataFileReader.openReader(in, reader)) {
      result = dfr.next();
    }
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof GenericRecord);
    Assert.assertEquals(new Utf8("testValue"), ((GenericRecord) result).get("name"));
  }
}
