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

package org.apache.avro.reflect;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestByteBuffer {

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  static class X {
    String name = "";
    ByteBuffer content;
  }

  File content;

  @Before
  public void before() throws IOException {
    content = new File(DIR.getRoot().getPath(), "test-content");
    try (FileOutputStream out = new FileOutputStream(content)) {
      for (int i = 0; i < 100000; i++) {
        out.write("hello world\n".getBytes(UTF_8));
      }
    }
  }

  @Test
  public void test() throws Exception {
    Schema schema = ReflectData.get().getSchema(X.class);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    writeOneXAsAvro(schema, bout);
    X record = readOneXFromAvro(schema, bout);

    String expected = getmd5(content);
    String actual = getmd5(record.content);
    assertEquals("md5 for result differed from input", expected, actual);
  }

  private X readOneXFromAvro(Schema schema, ByteArrayOutputStream bout) throws IOException {
    SeekableByteArrayInput input = new SeekableByteArrayInput(bout.toByteArray());
    ReflectDatumReader<X> datumReader = new ReflectDatumReader<>(schema);
    FileReader<X> reader = DataFileReader.openReader(input, datumReader);
    Iterator<X> it = reader.iterator();
    assertTrue("missing first record", it.hasNext());
    X record = it.next();
    assertFalse("should be no more records - only wrote one out", it.hasNext());
    return record;
  }

  private void writeOneXAsAvro(Schema schema, ByteArrayOutputStream bout) throws IOException, FileNotFoundException {
    DatumWriter<X> datumWriter = new ReflectDatumWriter<>(schema);
    try (DataFileWriter<X> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(schema, bout);
      X x = new X();
      x.name = "xxx";
      try (FileInputStream fis = new FileInputStream(content)) {
        try (FileChannel channel = fis.getChannel()) {
          long contentLength = content.length();
          // set the content to be a file channel.
          ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, contentLength);
          x.content = buffer;
          writer.append(x);
        }
      }
      writer.flush();
    }
  }

  private String getmd5(File content) throws Exception {
    try (FileInputStream fis = new FileInputStream(content)) {
      try (FileChannel channel = fis.getChannel()) {
        long contentLength = content.length();
        ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, contentLength);
        return getmd5(buffer);
      }
    }
  }

  String getmd5(ByteBuffer buffer) throws NoSuchAlgorithmException {
    MessageDigest mdEnc = MessageDigest.getInstance("MD5");
    mdEnc.reset();
    mdEnc.update(buffer);
    return new BigInteger(1, mdEnc.digest()).toString(16);
  }
}
