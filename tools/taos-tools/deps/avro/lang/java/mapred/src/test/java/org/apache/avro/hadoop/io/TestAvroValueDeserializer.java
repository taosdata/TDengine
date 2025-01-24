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

package org.apache.avro.hadoop.io;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.mapred.AvroWrapper;
import org.junit.Test;

public class TestAvroValueDeserializer {
  @Test
  public void testDeserialize() throws IOException {
    // Create a deserializer.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    ClassLoader classLoader = this.getClass().getClassLoader();
    AvroValueDeserializer<CharSequence> deserializer = new AvroValueDeserializer<>(writerSchema, readerSchema,
        classLoader);

    // Check the schemas.
    assertEquals(writerSchema, deserializer.getWriterSchema());
    assertEquals(readerSchema, deserializer.getReaderSchema());

    // Write some records to deserialize.
    DatumWriter<CharSequence> datumWriter = new GenericDatumWriter<>(writerSchema);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    datumWriter.write("record1", encoder);
    datumWriter.write("record2", encoder);
    encoder.flush();

    // Deserialize the records.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    deserializer.open(inputStream);
    AvroWrapper<CharSequence> record = null;

    record = deserializer.deserialize(record);
    assertEquals("record1", record.datum().toString());

    record = deserializer.deserialize(record);
    assertEquals("record2", record.datum().toString());

    deserializer.close();
  }
}
