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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestDataFileCustomSync {
  private byte[] createDataFile(byte[] sync) throws IOException {
    Schema schema = Schema.create(Type.STRING);
    DataFileWriter<Utf8> w = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    w.create(schema, baos, sync);
    w.append(new Utf8("apple"));
    w.append(new Utf8("banana"));
    w.sync();
    w.append(new Utf8("celery"));
    w.append(new Utf8("date"));
    w.sync();
    w.append(new Utf8("endive"));
    w.append(new Utf8("fig"));
    w.close();
    return baos.toByteArray();
  }

  private static byte[] generateSync() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((UUID.randomUUID() + "@" + time).getBytes(UTF_8));
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expected = IOException.class)
  public void testInvalidSync() throws IOException {
    // Invalid size (must be 16):
    byte[] sync = new byte[8];
    createDataFile(sync);
  }

  @Test
  public void testRandomSync() throws IOException {
    byte[] sync = generateSync();
    byte[] randSyncFile = createDataFile(null);
    byte[] customSyncFile = createDataFile(sync);
    assertFalse(Arrays.equals(randSyncFile, customSyncFile));
  }

  @Test
  public void testCustomSync() throws IOException {
    byte[] sync = generateSync();
    byte[] customSyncFile = createDataFile(sync);
    byte[] sameCustomSyncFile = createDataFile(sync);
    assertTrue(Arrays.equals(customSyncFile, sameCustomSyncFile));
  }
}
