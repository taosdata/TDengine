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

package org.apache.avro.codegentest;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

abstract class AbstractSpecificRecordTest {

  @SuppressWarnings("unchecked")
  <T extends SpecificRecordBase> void verifySerDeAndStandardMethods(T original) {
    final SpecificDatumWriter<T> datumWriterFromSchema = new SpecificDatumWriter<>(original.getSchema());
    final SpecificDatumReader<T> datumReaderFromSchema = new SpecificDatumReader<>(original.getSchema(),
        original.getSchema());
    verifySerDeAndStandardMethods(original, datumWriterFromSchema, datumReaderFromSchema);
    final SpecificDatumWriter<T> datumWriterFromClass = new SpecificDatumWriter(original.getClass());
    final SpecificDatumReader<T> datumReaderFromClass = new SpecificDatumReader(original.getClass());
    verifySerDeAndStandardMethods(original, datumWriterFromClass, datumReaderFromClass);
  }

  private <T extends SpecificRecordBase> void verifySerDeAndStandardMethods(T original,
      SpecificDatumWriter<T> datumWriter, SpecificDatumReader<T> datumReader) {
    final byte[] serialized = serialize(original, datumWriter);
    final T copy = deserialize(serialized, datumReader);
    Assert.assertEquals(original, copy);
    // In addition to equals() tested above, make sure the other methods that use
    // SpecificData work as intended
    // compareTo() throws an exception for maps, otherwise we would have tested it
    // here
    // Assert.assertEquals(0, original.compareTo(copy));
    Assert.assertEquals(original.hashCode(), copy.hashCode());
    Assert.assertEquals(original.toString(), copy.toString());
  }

  private <T extends SpecificRecordBase> byte[] serialize(T object, SpecificDatumWriter<T> datumWriter) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      datumWriter.write(object, EncoderFactory.get().directBinaryEncoder(outputStream, null));
      return outputStream.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T extends SpecificRecordBase> T deserialize(byte[] bytes, SpecificDatumReader<T> datumReader) {
    try {
      final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
      return datumReader.read(null, DecoderFactory.get().directBinaryDecoder(byteArrayInputStream, null));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
