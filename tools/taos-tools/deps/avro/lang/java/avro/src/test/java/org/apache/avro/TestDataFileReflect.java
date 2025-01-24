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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDataFileReflect {

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  /*
   * Test that using multiple schemas in a file works doing a union before writing
   * any records.
   */
  @Test
  public void testMultiReflectWithUnionBeforeWriting() throws IOException {
    File file = new File(DIR.getRoot().getPath(), "testMultiReflectWithUnionBeforeWriting.avro");
    CheckList<Object> check = new CheckList<>();
    try (FileOutputStream fos = new FileOutputStream(file)) {

      ReflectData reflectData = ReflectData.get();
      List<Schema> schemas = Arrays.asList(reflectData.getSchema(FooRecord.class),
          reflectData.getSchema(BarRecord.class));
      Schema union = Schema.createUnion(schemas);

      try (DataFileWriter<Object> writer = new DataFileWriter<>(new ReflectDatumWriter<>(union))) {
        writer.create(union, fos);

        // test writing to a file
        write(writer, new BarRecord("One beer please"), check);
        write(writer, new FooRecord(10), check);
        write(writer, new BarRecord("Two beers please"), check);
        write(writer, new FooRecord(20), check);
      }
    }
    // new File(DIR.getRoot().getPath(), "test.avro");
    ReflectDatumReader<Object> din = new ReflectDatumReader<>();
    SeekableFileInput sin = new SeekableFileInput(file);
    try (DataFileReader<Object> reader = new DataFileReader<>(sin, din)) {
      int count = 0;
      for (Object datum : reader) {
        check.assertEquals(datum, count++);
      }
      Assert.assertEquals(count, check.size());
    }
  }

  /*
   * Test that writing a record with a field that is null.
   */
  @Test
  public void testNull() throws IOException {
    File file = new File(DIR.getRoot().getPath(), "testNull.avro");
    CheckList<BarRecord> check = new CheckList<>();

    try (FileOutputStream fos = new FileOutputStream(file)) {
      ReflectData reflectData = ReflectData.AllowNull.get();
      Schema schema = reflectData.getSchema(BarRecord.class);
      try (DataFileWriter<BarRecord> writer = new DataFileWriter<>(
          new ReflectDatumWriter<>(BarRecord.class, reflectData))) {
        writer.create(schema, fos);
        // test writing to a file
        write(writer, new BarRecord("One beer please"), check);
        // null record here, fails when using the default reflectData instance
        write(writer, new BarRecord(), check);
        write(writer, new BarRecord("Two beers please"), check);
      }
    }

    ReflectDatumReader<BarRecord> din = new ReflectDatumReader<>();
    try (SeekableFileInput sin = new SeekableFileInput(file)) {
      try (DataFileReader<BarRecord> reader = new DataFileReader<>(sin, din)) {
        int count = 0;
        for (BarRecord datum : reader) {
          check.assertEquals(datum, count++);
        }
        Assert.assertEquals(count, check.size());
      }
    }
  }

  @Test
  public void testNew() throws IOException {
    ByteBuffer payload = ByteBuffer.allocateDirect(8 * 1024);
    for (int i = 0; i < 500; i++) {
      payload.putInt(1);
    }
    payload.flip();
    ByteBufferRecord bbr = new ByteBufferRecord();
    bbr.setPayload(payload);
    bbr.setTp(TypeEnum.b);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ReflectDatumWriter<ByteBufferRecord> writer = new ReflectDatumWriter<>(ByteBufferRecord.class);
    BinaryEncoder avroEncoder = EncoderFactory.get().blockingBinaryEncoder(outputStream, null);
    writer.write(bbr, avroEncoder);
    avroEncoder.flush();

    byte[] bytes = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    ReflectDatumReader<ByteBufferRecord> datumReader = new ReflectDatumReader<>(ByteBufferRecord.class);
    BinaryDecoder avroDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    ByteBufferRecord deserialized = datumReader.read(null, avroDecoder);

    Assert.assertEquals(bbr, deserialized);
  }

  /*
   * Test that writing out and reading in a nested class works
   */
  @Test
  public void testNestedClass() throws IOException {
    File file = new File(DIR.getRoot().getPath(), "testNull.avro");

    CheckList<BazRecord> check = new CheckList<>();
    try (FileOutputStream fos = new FileOutputStream(file)) {
      Schema schema = ReflectData.get().getSchema(BazRecord.class);
      try (DataFileWriter<BazRecord> writer = new DataFileWriter<>(new ReflectDatumWriter<>(schema))) {
        writer.create(schema, fos);

        // test writing to a file
        write(writer, new BazRecord(10), check);
        write(writer, new BazRecord(20), check);
      }
    }

    ReflectDatumReader<BazRecord> din = new ReflectDatumReader<>();
    try (SeekableFileInput sin = new SeekableFileInput(file)) {
      try (DataFileReader<BazRecord> reader = new DataFileReader<>(sin, din)) {
        int count = 0;
        for (BazRecord datum : reader) {
          check.assertEquals(datum, count++);
        }
        Assert.assertEquals(count, check.size());
      }
    }
  }

  private <T> void write(DataFileWriter<T> writer, T o, CheckList<T> l) throws IOException {
    writer.append(l.addAndReturn(o));
  }

  @SuppressWarnings("serial")
  private static class CheckList<T> extends ArrayList<T> {
    T addAndReturn(T check) {
      add(check);
      return check;
    }

    void assertEquals(Object toCheck, int i) {
      Assert.assertNotNull(toCheck);
      Object o = get(i);
      Assert.assertNotNull(o);
      Assert.assertEquals(toCheck, o);
    }
  }

  private static class BazRecord {
    private int nbr;

    @SuppressWarnings("unused")
    public BazRecord() {
    }

    public BazRecord(int nbr) {
      this.nbr = nbr;
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof BazRecord) {
        return this.nbr == ((BazRecord) that).nbr;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return nbr;
    }

    @Override
    public String toString() {
      return BazRecord.class.getSimpleName() + "{cnt=" + nbr + "}";
    }
  }
}
