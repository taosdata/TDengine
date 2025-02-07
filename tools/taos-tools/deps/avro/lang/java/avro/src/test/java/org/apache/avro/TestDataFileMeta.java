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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDataFileMeta {

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  @Test(expected = AvroRuntimeException.class)
  public void testUseReservedMeta() throws IOException {
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.setMeta("avro.foo", "bar");
    }
  }

  @Test()
  public void testUseMeta() throws IOException {
    File f = new File(DIR.getRoot().getPath(), "testDataFileMeta.avro");
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.setMeta("hello", "bar");
      w.create(Schema.create(Type.NULL), f);
    }

    try (DataFileStream<Void> r = new DataFileStream<>(new FileInputStream(f), new GenericDatumReader<>())) {
      assertTrue(r.getMetaKeys().contains("hello"));

      assertEquals("bar", r.getMetaString("hello"));
    }

  }

  @Test(expected = AvroRuntimeException.class)
  public void testUseMetaAfterCreate() throws IOException {
    try (DataFileWriter<?> w = new DataFileWriter<>(new GenericDatumWriter<>())) {
      w.create(Schema.create(Type.NULL), new ByteArrayOutputStream());
      w.setMeta("foo", "bar");
    }

  }

  @Test
  public void testBlockSizeSetInvalid() {
    int exceptions = 0;
    for (int i = -1; i < 33; i++) {
      // 33 invalid, one valid
      try {
        new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(i);
      } catch (IllegalArgumentException iae) {
        exceptions++;
      }
    }
    assertEquals(33, exceptions);
  }
}
