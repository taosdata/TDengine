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
package org.apache.avro.io;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

public class TestJsonDecoder {

  @Test
  public void testInt() throws Exception {
    checkNumeric("int", 1);
  }

  @Test
  public void testLong() throws Exception {
    checkNumeric("long", 1L);
  }

  @Test
  public void testFloat() throws Exception {
    checkNumeric("float", 1.0F);
  }

  @Test
  public void testDouble() throws Exception {
    checkNumeric("double", 1.0);
  }

  private void checkNumeric(String type, Object value) throws Exception {
    String def = "{\"type\":\"record\",\"name\":\"X\",\"fields\":" + "[{\"type\":\"" + type + "\",\"name\":\"n\"}]}";
    Schema schema = new Schema.Parser().parse(def);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);

    String[] records = { "{\"n\":1}", "{\"n\":1.0}" };

    for (String record : records) {
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, record);
      GenericRecord r = reader.read(null, decoder);
      Assert.assertEquals(value, r.get("n"));
    }
  }

  // Ensure that even if the order of fields in JSON is different from the order
  // in schema,
  // it works.
  @Test
  public void testReorderFields() throws Exception {
    String w = "{\"type\":\"record\",\"name\":\"R\",\"fields\":" + "[{\"type\":\"long\",\"name\":\"l\"},"
        + "{\"type\":{\"type\":\"array\",\"items\":\"int\"},\"name\":\"a\"}" + "]}";
    Schema ws = new Schema.Parser().parse(w);
    DecoderFactory df = DecoderFactory.get();
    String data = "{\"a\":[1,2],\"l\":100}{\"l\": 200, \"a\":[1,2]}";
    JsonDecoder in = df.jsonDecoder(ws, data);
    Assert.assertEquals(100, in.readLong());
    in.skipArray();
    Assert.assertEquals(200, in.readLong());
    in.skipArray();
  }
}
