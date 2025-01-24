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

package org.apache.avro.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroDatumConverterFactory;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

public class TestAvroKeyValueRecordWriter {
  @Test
  public void testWriteRecords() throws IOException {
    Job job = Job.getInstance();
    AvroJob.setOutputValueSchema(job, TextStats.SCHEMA$);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    AvroDatumConverterFactory factory = new AvroDatumConverterFactory(job.getConfiguration());
    AvroDatumConverter<Text, ?> keyConverter = factory.create(Text.class);
    AvroValue<TextStats> avroValue = new AvroValue<>(null);
    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroValue<TextStats>, ?> valueConverter = factory
        .create((Class<AvroValue<TextStats>>) avroValue.getClass());
    CodecFactory compressionCodec = CodecFactory.nullCodec();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // Use a writer to generate a Avro container file in memory.
    // Write two records: <'apple', TextStats('apple')> and <'banana',
    // TextStats('banana')>.
    AvroKeyValueRecordWriter<Text, AvroValue<TextStats>> writer = new AvroKeyValueRecordWriter<>(keyConverter,
        valueConverter, new ReflectData(), compressionCodec, outputStream);
    TextStats appleStats = new TextStats();
    appleStats.setName("apple");
    writer.write(new Text("apple"), new AvroValue<>(appleStats));
    TextStats bananaStats = new TextStats();
    bananaStats.setName("banana");
    writer.write(new Text("banana"), new AvroValue<>(bananaStats));
    writer.close(context);

    verify(context);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.STRING), TextStats.SCHEMA$);
    DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(readerSchema);
    DataFileStream<GenericRecord> avroFileReader = new DataFileStream<>(inputStream, datumReader);

    // Verify that the first record was written.
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> firstRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(firstRecord.get());
    assertEquals("apple", firstRecord.getKey().toString());
    assertEquals("apple", firstRecord.getValue().getName().toString());

    // Verify that the second record was written;
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> secondRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(secondRecord.get());
    assertEquals("banana", secondRecord.getKey().toString());
    assertEquals("banana", secondRecord.getValue().getName().toString());

    // That's all, folks.
    assertFalse(avroFileReader.hasNext());
    avroFileReader.close();
  }

  public static class R1 {
    String attribute;
  }

  @Test
  public void testUsingReflection() throws Exception {
    Job job = Job.getInstance();
    Schema schema = ReflectData.get().getSchema(R1.class);
    AvroJob.setOutputValueSchema(job, schema);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    replay(context);

    R1 record = new R1();
    record.attribute = "test";
    AvroValue<R1> avroValue = new AvroValue<>(record);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    AvroDatumConverterFactory factory = new AvroDatumConverterFactory(job.getConfiguration());

    AvroDatumConverter<Text, ?> keyConverter = factory.create(Text.class);

    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroValue<R1>, R1> valueConverter = factory.create((Class<AvroValue<R1>>) avroValue.getClass());

    AvroKeyValueRecordWriter<Text, AvroValue<R1>> writer = new AvroKeyValueRecordWriter<>(keyConverter, valueConverter,
        new ReflectData(), CodecFactory.nullCodec(), outputStream);

    writer.write(new Text("reflectionData"), avroValue);
    writer.close(context);

    verify(context);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.STRING), schema);
    DatumReader<GenericRecord> datumReader = new ReflectDatumReader<>(readerSchema);
    DataFileStream<GenericRecord> avroFileReader = new DataFileStream<>(inputStream, datumReader);

    // Verify that the first record was written.
    assertTrue(avroFileReader.hasNext());

    // Verify that the record holds the same data that we've written
    AvroKeyValue<CharSequence, R1> firstRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(firstRecord.get());
    assertEquals("reflectionData", firstRecord.getKey().toString());
    assertEquals(record.attribute, firstRecord.getValue().attribute);
    avroFileReader.close();
  }

  @Test
  public void testSyncableWriteRecords() throws IOException {
    Job job = Job.getInstance();
    AvroJob.setOutputValueSchema(job, TextStats.SCHEMA$);
    TaskAttemptContext context = createMock(TaskAttemptContext.class);

    replay(context);

    AvroDatumConverterFactory factory = new AvroDatumConverterFactory(job.getConfiguration());
    AvroDatumConverter<Text, ?> keyConverter = factory.create(Text.class);
    AvroValue<TextStats> avroValue = new AvroValue<>(null);
    @SuppressWarnings("unchecked")
    AvroDatumConverter<AvroValue<TextStats>, ?> valueConverter = factory
        .create((Class<AvroValue<TextStats>>) avroValue.getClass());
    CodecFactory compressionCodec = CodecFactory.nullCodec();
    FileOutputStream outputStream = new FileOutputStream(new File("target/temp.avro"));

    // Write a marker followed by each record: <'apple', TextStats('apple')> and
    // <'banana', TextStats('banana')>.
    AvroKeyValueRecordWriter<Text, AvroValue<TextStats>> writer = new AvroKeyValueRecordWriter<>(keyConverter,
        valueConverter, new ReflectData(), compressionCodec, outputStream);
    TextStats appleStats = new TextStats();
    appleStats.setName("apple");
    long pointOne = writer.sync();
    writer.write(new Text("apple"), new AvroValue<>(appleStats));
    TextStats bananaStats = new TextStats();
    bananaStats.setName("banana");
    long pointTwo = writer.sync();
    writer.write(new Text("banana"), new AvroValue<>(bananaStats));
    writer.close(context);

    verify(context);

    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    Path avroFile = new Path("target/temp.avro");
    DataFileReader<GenericData.Record> avroFileReader = new DataFileReader<>(new FsInput(avroFile, conf),
        new SpecificDatumReader<>());

    avroFileReader.seek(pointTwo);
    // Verify that the second record was written;
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> secondRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(secondRecord.get());
    assertEquals("banana", secondRecord.getKey().toString());
    assertEquals("banana", secondRecord.getValue().getName().toString());

    avroFileReader.seek(pointOne);
    // Verify that the first record was written.
    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, TextStats> firstRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(firstRecord.get());
    assertEquals("apple", firstRecord.getKey().toString());
    assertEquals("apple", firstRecord.getValue().getName().toString());

    // That's all, folks.
    avroFileReader.close();
  }
}
