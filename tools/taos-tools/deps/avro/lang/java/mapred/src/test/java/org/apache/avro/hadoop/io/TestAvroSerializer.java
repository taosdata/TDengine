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
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroKey;
import org.junit.Test;

public class TestAvroSerializer {
  @Test
  public void testSerialize() throws IOException {
    // Create a serializer.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    AvroSerializer<CharSequence> serializer = new AvroSerializer<>(writerSchema);

    // Check the writer schema.
    assertEquals(writerSchema, serializer.getWriterSchema());

    // Serialize two records, 'record1' and 'record2'.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    serializer.open(outputStream);
    serializer.serialize(new AvroKey<>("record1"));
    serializer.serialize(new AvroKey<>("record2"));
    serializer.close();

    // Make sure the records were serialized correctly.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    DatumReader<CharSequence> datumReader = new GenericDatumReader<>(readerSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
    CharSequence record = null;

    record = datumReader.read(record, decoder);
    assertEquals("record1", record.toString());

    record = datumReader.read(record, decoder);
    assertEquals("record2", record.toString());

    inputStream.close();
  }
}
