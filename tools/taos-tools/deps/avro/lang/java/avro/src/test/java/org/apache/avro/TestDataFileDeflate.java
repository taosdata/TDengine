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
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Test;

/** Simple test of DataFileWriter and DataFileStream with deflate codec. */
public class TestDataFileDeflate {
  @Test
  public void testWriteAndRead() throws IOException {
    Schema schema = Schema.create(Type.STRING);

    // Write it
    DataFileWriter<Utf8> w = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    w.setCodec(CodecFactory.deflateCodec(6));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    w.create(schema, baos);
    w.append(new Utf8("hello world"));
    w.append(new Utf8("hello moon"));
    w.sync();
    w.append(new Utf8("bye bye world"));
    w.append(new Utf8("bye bye moon"));
    w.close();

    // Read it
    try (DataFileStream<Utf8> r = new DataFileStream<>(new ByteArrayInputStream(baos.toByteArray()),
        new GenericDatumReader<>(schema))) {
      assertEquals("hello world", r.next().toString());
      assertEquals("hello moon", r.next().toString());
      assertEquals("bye bye world", r.next().toString());
      assertEquals("bye bye moon", r.next().toString());
      assertFalse(r.hasNext());
    }
  }
}
