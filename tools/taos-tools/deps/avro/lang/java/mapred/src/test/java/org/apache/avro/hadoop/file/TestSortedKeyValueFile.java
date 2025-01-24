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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.hadoop.file;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSortedKeyValueFile {
  private static final Logger LOG = LoggerFactory.getLogger(TestSortedKeyValueFile.class);

  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Test(expected = IllegalArgumentException.class)
  public void testWriteOutOfSortedOrder() throws IOException {
    LOG.debug("Writing some records to a SortedKeyValueFile...");

    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "myfile")).withIndexInterval(2); // Index
                                                                                                                  // every
                                                                                                                  // other
                                                                                                                  // record.

    try (SortedKeyValueFile.Writer<CharSequence, CharSequence> writer = new SortedKeyValueFile.Writer<>(options)) {
      Utf8 key = new Utf8(); // re-use key, to test copied
      writer.append(key.set("banana"), "Banana");
      writer.append(key.set("apple"), "Apple"); // Ruh, roh!
    }
  }

  @Test
  public void testNamedCodecs() throws IOException {
    Configuration conf = new Configuration();
    Path myfile = new Path(mTempDir.getRoot().getPath(), "myfile");
    Schema key = Schema.create(Schema.Type.STRING);
    Schema value = Schema.create(Schema.Type.STRING);
    Schema recordSchema = AvroKeyValue.getSchema(key, value);
    DatumReader<GenericRecord> datumReader = SpecificData.get().createDatumReader(recordSchema);
    DataFileReader<GenericRecord> reader;

    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options().withKeySchema(key)
        .withValueSchema(value).withConfiguration(conf).withPath(myfile);

    SortedKeyValueFile.Writer<CharSequence, CharSequence> writer;

    for (String codec : new String[] { "null", "deflate", "snappy", "bzip2" }) {
      LOG.debug("Using " + codec + "codec for a SortedKeyValueFile...");

      options.withCodec(codec);

      writer = new SortedKeyValueFile.Writer<>(options);
      writer.close();

      reader = new DataFileReader<>(new FsInput(new Path(myfile, SortedKeyValueFile.DATA_FILENAME), conf), datumReader);

      assertEquals(codec, reader.getMetaString("avro.codec"));
      reader.close();
    }
  }

  @Test
  public void testDeflateClassCodec() throws IOException {
    Configuration conf = new Configuration();
    Path myfile = new Path(mTempDir.getRoot().getPath(), "myfile");
    Schema key = Schema.create(Schema.Type.STRING);
    Schema value = Schema.create(Schema.Type.STRING);
    Schema recordSchema = AvroKeyValue.getSchema(key, value);
    DatumReader<GenericRecord> datumReader = SpecificData.get().createDatumReader(recordSchema);
    DataFileReader<GenericRecord> reader;

    LOG.debug("Using CodecFactory.deflateCodec() for a SortedKeyValueFile...");
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options().withKeySchema(key)
        .withValueSchema(value).withConfiguration(conf).withPath(myfile).withCodec(CodecFactory.deflateCodec(9));

    SortedKeyValueFile.Writer<CharSequence, CharSequence> writer = new SortedKeyValueFile.Writer<>(options);
    writer.close();

    reader = new DataFileReader<>(new FsInput(new Path(myfile, SortedKeyValueFile.DATA_FILENAME), conf), datumReader);

    assertEquals("deflate", reader.getMetaString("avro.codec"));
    reader.close();
  }

  @Test
  public void testBadCodec() throws IOException {
    LOG.debug("Using a bad codec for a SortedKeyValueFile...");

    try {
      new SortedKeyValueFile.Writer.Options().withCodec("foobar");
    } catch (AvroRuntimeException e) {
      assertEquals("Unrecognized codec: foobar", e.getMessage());
    }
  }

  @Test
  public void testWriter() throws IOException {
    LOG.debug("Writing some records to a SortedKeyValueFile...");

    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "myfile")).withIndexInterval(2); // Index
                                                                                                                  // every
                                                                                                                  // other
                                                                                                                  // record.

    try (SortedKeyValueFile.Writer<CharSequence, CharSequence> writer = new SortedKeyValueFile.Writer<>(options)) {
      writer.append("apple", "Apple"); // Will be indexed.
      writer.append("banana", "Banana");
      writer.append("carrot", "Carrot"); // Will be indexed.
      writer.append("durian", "Durian");
    }

    LOG.debug("Checking the generated directory...");
    File directory = new File(mTempDir.getRoot().getPath(), "myfile");
    assertTrue(directory.exists());

    LOG.debug("Checking the generated index file...");
    File indexFile = new File(directory, SortedKeyValueFile.INDEX_FILENAME);
    DatumReader<GenericRecord> indexReader = new GenericDatumReader<>(
        AvroKeyValue.getSchema(options.getKeySchema(), Schema.create(Schema.Type.LONG)));

    List<AvroKeyValue<CharSequence, Long>> indexRecords = new ArrayList<>();
    try (FileReader<GenericRecord> indexFileReader = DataFileReader.openReader(indexFile, indexReader)) {
      for (GenericRecord indexRecord : indexFileReader) {
        indexRecords.add(new AvroKeyValue<>(indexRecord));
      }
    }

    assertEquals(2, indexRecords.size());
    assertEquals("apple", indexRecords.get(0).getKey().toString());
    LOG.debug("apple's position in the file: " + indexRecords.get(0).getValue());
    assertEquals("carrot", indexRecords.get(1).getKey().toString());
    LOG.debug("carrot's position in the file: " + indexRecords.get(1).getValue());

    LOG.debug("Checking the generated data file...");
    File dataFile = new File(directory, SortedKeyValueFile.DATA_FILENAME);
    DatumReader<GenericRecord> dataReader = new GenericDatumReader<>(
        AvroKeyValue.getSchema(options.getKeySchema(), options.getValueSchema()));

    try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(dataFile, dataReader)) {
      dataFileReader.seek(indexRecords.get(0).getValue());
      assertTrue(dataFileReader.hasNext());
      AvroKeyValue<CharSequence, CharSequence> appleRecord = new AvroKeyValue<>(dataFileReader.next());
      assertEquals("apple", appleRecord.getKey().toString());
      assertEquals("Apple", appleRecord.getValue().toString());

      dataFileReader.seek(indexRecords.get(1).getValue());
      assertTrue(dataFileReader.hasNext());
      AvroKeyValue<CharSequence, CharSequence> carrotRecord = new AvroKeyValue<>(dataFileReader.next());
      assertEquals("carrot", carrotRecord.getKey().toString());
      assertEquals("Carrot", carrotRecord.getValue().toString());

      assertTrue(dataFileReader.hasNext());
      AvroKeyValue<CharSequence, CharSequence> durianRecord = new AvroKeyValue<>(dataFileReader.next());
      assertEquals("durian", durianRecord.getKey().toString());
      assertEquals("Durian", durianRecord.getValue().toString());
    }
  }

  @Test
  public void testReader() throws IOException {
    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options writerOptions = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "myfile")).withIndexInterval(2); // Index
                                                                                                                  // every
                                                                                                                  // other
                                                                                                                  // record.

    try (
        SortedKeyValueFile.Writer<CharSequence, CharSequence> writer = new SortedKeyValueFile.Writer<>(writerOptions)) {
      writer.append("apple", "Apple"); // Will be indexed.
      writer.append("banana", "Banana");
      writer.append("carrot", "Carrot"); // Will be indexed.
      writer.append("durian", "Durian");
    }

    LOG.debug("Reading the file back using a reader...");
    SortedKeyValueFile.Reader.Options readerOptions = new SortedKeyValueFile.Reader.Options()
        .withKeySchema(Schema.create(Schema.Type.STRING)).withValueSchema(Schema.create(Schema.Type.STRING))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "myfile"));

    try (
        SortedKeyValueFile.Reader<CharSequence, CharSequence> reader = new SortedKeyValueFile.Reader<>(readerOptions)) {
      assertEquals("Carrot", reader.get("carrot").toString());
      assertEquals("Banana", reader.get("banana").toString());
      assertNull(reader.get("a-vegetable"));
      assertNull(reader.get("beet"));
      assertNull(reader.get("zzz"));
    }
  }

  public static class Stringy implements Comparable<Stringy> {
    private String s;

    public Stringy() {
    };

    public Stringy(String s) {
      this.s = s;
    }

    @Override
    public String toString() {
      return s;
    }

    @Override
    public int hashCode() {
      return s.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      return this.s.equals(that.toString());
    }

    @Override
    public int compareTo(Stringy that) {
      return this.s.compareTo(that.s);
    }
  }

  @Test
  public void testAlternateModel() throws Exception {
    LOG.debug("Writing some reflect records...");

    ReflectData model = ReflectData.get();

    Configuration conf = new Configuration();
    SortedKeyValueFile.Writer.Options options = new SortedKeyValueFile.Writer.Options()
        .withKeySchema(model.getSchema(Stringy.class)).withValueSchema(model.getSchema(Stringy.class))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "reflect")).withDataModel(model)
        .withIndexInterval(2);

    try (SortedKeyValueFile.Writer<Stringy, Stringy> writer = new SortedKeyValueFile.Writer<>(options)) {
      writer.append(new Stringy("apple"), new Stringy("Apple"));
      writer.append(new Stringy("banana"), new Stringy("Banana"));
      writer.append(new Stringy("carrot"), new Stringy("Carrot"));
      writer.append(new Stringy("durian"), new Stringy("Durian"));
    }

    LOG.debug("Reading the file back using a reader...");
    SortedKeyValueFile.Reader.Options readerOptions = new SortedKeyValueFile.Reader.Options()
        .withKeySchema(model.getSchema(Stringy.class)).withValueSchema(model.getSchema(Stringy.class))
        .withConfiguration(conf).withPath(new Path(mTempDir.getRoot().getPath(), "reflect")).withDataModel(model);

    try (SortedKeyValueFile.Reader<Stringy, Stringy> reader = new SortedKeyValueFile.Reader<>(readerOptions)) {
      assertEquals(new Stringy("Carrot"), reader.get(new Stringy("carrot")));
      assertEquals(new Stringy("Banana"), reader.get(new Stringy("banana")));
      assertNull(reader.get(new Stringy("a-vegetable")));
      assertNull(reader.get(new Stringy("beet")));
      assertNull(reader.get(new Stringy("zzz")));
    }

  }

}
