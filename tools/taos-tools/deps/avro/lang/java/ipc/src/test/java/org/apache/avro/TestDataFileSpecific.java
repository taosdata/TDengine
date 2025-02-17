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

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;

public class TestDataFileSpecific {

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  /*
   * Test when using SpecificDatumReader<T>() constructor to read from a file with
   * a different schema that both reader & writer schemas are found.
   */
  @Test
  public void testSpecificDatumReaderDefaultCtor() throws IOException {
    File file = new File(DIR.getRoot().getPath(), "testSpecificDatumReaderDefaultCtor");

    // like the specific Foo, but with another field
    Schema s1 = new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"Foo\"," + "\"namespace\":\"org.apache.avro\",\"fields\":["
            + "{\"name\":\"label\",\"type\":\"string\"}," + "{\"name\":\"id\",\"type\":\"int\"}]}");

    // write a file using generic objects
    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<Record>(s1)).create(s1, file)) {
      for (int i = 0; i < 10; i++) {
        Record r = new Record(s1);
        r.put("label", "" + i);
        r.put("id", i);
        writer.append(r);
      }
    }

    // read using a 'new SpecificDatumReader<T>()' to force inference of
    // reader's schema from runtime
    try (DataFileReader<Foo> reader = new DataFileReader<>(file, new SpecificDatumReader<>())) {
      int i = 0;
      for (Foo f : reader) {
        Assert.assertEquals("" + (i++), f.getLabel());
      }
      Assert.assertEquals(10, i);
    }
  }

}
