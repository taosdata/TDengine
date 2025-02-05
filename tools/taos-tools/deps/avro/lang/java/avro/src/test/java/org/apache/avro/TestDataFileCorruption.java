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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestDataFileCorruption {

  private static final File DIR = new File(System.getProperty("java.io.tmpdir"));

  private File makeFile(String name) {
    return new File(DIR, "test-" + name + ".avro");
  }

  @Test
  public void testCorruptedFile() throws IOException {
    Schema schema = Schema.create(Type.STRING);

    // Write a data file
    DataFileWriter<Utf8> w = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    w.create(schema, baos);
    w.append(new Utf8("apple"));
    w.append(new Utf8("banana"));
    w.sync();
    w.append(new Utf8("celery"));
    w.append(new Utf8("date"));
    long pos = w.sync();
    w.append(new Utf8("endive"));
    w.append(new Utf8("fig"));
    w.close();

    // Corrupt the input by inserting some zero bytes before the sync marker for the
    // penultimate block
    byte[] original = baos.toByteArray();
    int corruptPosition = (int) pos - DataFileConstants.SYNC_SIZE;
    int corruptedBytes = 3;
    byte[] corrupted = new byte[original.length + corruptedBytes];
    System.arraycopy(original, 0, corrupted, 0, corruptPosition);
    System.arraycopy(original, corruptPosition, corrupted, corruptPosition + corruptedBytes,
        original.length - corruptPosition);

    File file = makeFile("corrupt");
    file.deleteOnExit();
    FileOutputStream out = new FileOutputStream(file);
    out.write(corrupted);
    out.close();

    // Read the data file
    try (DataFileReader r = new DataFileReader<>(file, new GenericDatumReader<>(schema))) {
      assertEquals("apple", r.next().toString());
      assertEquals("banana", r.next().toString());
      long prevSync = r.previousSync();
      r.next();
      fail("Corrupt block should throw exception");
      r.sync(prevSync); // go to sync point after previous successful one
      assertEquals("endive", r.next().toString());
      assertEquals("fig", r.next().toString());
      assertFalse(r.hasNext());
    } catch (AvroRuntimeException e) {
      assertEquals("Invalid sync!", e.getCause().getMessage());
    }

  }
}
