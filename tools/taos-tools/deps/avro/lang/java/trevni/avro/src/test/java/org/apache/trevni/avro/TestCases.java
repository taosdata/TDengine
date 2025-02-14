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
package org.apache.trevni.avro;

import java.io.File;
import java.io.EOFException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;

import org.apache.trevni.ColumnFileMetaData;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.generic.GenericDatumReader;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestCases {

  private static final File DIR = new File("src/test/cases/");
  private static final File FILE = new File("target", "case.trv");

  @Test
  public void testCases() throws Exception {
    for (File f : DIR.listFiles())
      if (f.isDirectory() && !f.getName().startsWith("."))
        runCase(f);
  }

  private void runCase(File dir) throws Exception {
    Schema schema = new Schema.Parser().parse(new File(dir, "input.avsc"));
    List<Object> data = fromJson(schema, new File(dir, "input.json"));

    // write full data
    AvroColumnWriter<Object> writer = new AvroColumnWriter<>(schema, new ColumnFileMetaData());
    for (Object datum : data)
      writer.write(datum);
    writer.writeTo(FILE);

    // test that the full schema reads correctly
    checkRead(schema, data);

    // test that sub-schemas read correctly
    for (File f : dir.listFiles())
      if (f.isDirectory() && !f.getName().startsWith(".")) {
        Schema s = new Schema.Parser().parse(new File(f, "sub.avsc"));
        checkRead(s, fromJson(s, new File(f, "sub.json")));
      }
  }

  private void checkRead(Schema s, List<Object> data) throws Exception {
    try (AvroColumnReader<Object> reader = new AvroColumnReader<>(new AvroColumnReader.Params(FILE).setSchema(s))) {
      for (Object datum : data)
        assertEquals(datum, reader.next());
    }
  }

  private List<Object> fromJson(Schema schema, File file) throws Exception {
    List<Object> data = new ArrayList<>();
    try (InputStream in = new FileInputStream(file)) {
      DatumReader reader = new GenericDatumReader(schema);
      Decoder decoder = DecoderFactory.get().jsonDecoder(schema, in);
      while (true)
        data.add(reader.read(null, decoder));
    } catch (EOFException e) {
    }
    return data;
  }

}
