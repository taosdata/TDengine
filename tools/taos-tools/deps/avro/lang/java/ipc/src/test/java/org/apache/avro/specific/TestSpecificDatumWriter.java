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
package org.apache.avro.specific;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.test.Kind;
import org.apache.avro.test.MD5;
import org.apache.avro.test.TestRecordWithUnion;
import org.apache.avro.test.TestRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSpecificDatumWriter {
  @Test
  public void testResolveUnion() throws IOException {
    final SpecificDatumWriter<TestRecordWithUnion> writer = new SpecificDatumWriter<>();
    Schema schema = TestRecordWithUnion.SCHEMA$;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);

    writer.setSchema(schema);

    TestRecordWithUnion c = TestRecordWithUnion.newBuilder().setKind(Kind.BAR).setValue("rab").build();
    writer.write(c, encoder);
    encoder.flush();
    out.close();

    String expectedJson = String.format("{'kind':{'org.apache.avro.test.Kind':'%s'},'value':{'string':'%s'}}",
        c.getKind().toString(), c.getValue()).replace('\'', '"');

    assertEquals(expectedJson, out.toString("UTF-8"));
  }

  @Test
  public void testIncompleteRecord() throws IOException {
    final SpecificDatumWriter<TestRecord> writer = new SpecificDatumWriter<>();
    Schema schema = TestRecord.SCHEMA$;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);

    writer.setSchema(schema);

    TestRecord testRecord = new TestRecord();
    testRecord.setKind(Kind.BAR);
    testRecord.setHash(new MD5(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));

    try {
      writer.write(testRecord, encoder);
      fail("Exception not thrown");
    } catch (NullPointerException e) {
      assertTrue(e.getMessage().contains("null of string in field 'name'"));
    } finally {
      out.close();
    }
  }
}
