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

package org.apache.avro.specific;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.avro.FooBarSpecificRecord;
import org.apache.avro.FooBarSpecificRecord.Builder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import test.StringablesRecord;

public class TestSpecificDatumReader {

  public static byte[] serializeRecord(FooBarSpecificRecord fooBarSpecificRecord) throws IOException {
    SpecificDatumWriter<FooBarSpecificRecord> datumWriter = new SpecificDatumWriter<>(FooBarSpecificRecord.SCHEMA$);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    datumWriter.write(fooBarSpecificRecord, encoder);
    encoder.flush();
    return byteArrayOutputStream.toByteArray();
  }

  public static byte[] serializeRecord(StringablesRecord stringablesRecord) throws IOException {
    SpecificDatumWriter<StringablesRecord> datumWriter = new SpecificDatumWriter<>(StringablesRecord.SCHEMA$);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
    datumWriter.write(stringablesRecord, encoder);
    encoder.flush();
    return byteArrayOutputStream.toByteArray();
  }

  @Test
  public void testRead() throws IOException {
    Builder newBuilder = FooBarSpecificRecord.newBuilder();
    newBuilder.setId(42);
    newBuilder.setName("foo");
    newBuilder.setNicknames(Collections.singletonList("bar"));
    newBuilder.setRelatedids(Arrays.asList(1, 2, 3));
    FooBarSpecificRecord specificRecord = newBuilder.build();

    byte[] recordBytes = serializeRecord(specificRecord);

    Decoder decoder = DecoderFactory.get().binaryDecoder(recordBytes, null);
    SpecificDatumReader<FooBarSpecificRecord> specificDatumReader = new SpecificDatumReader<>(
        FooBarSpecificRecord.SCHEMA$);
    FooBarSpecificRecord deserialized = new FooBarSpecificRecord();
    specificDatumReader.read(deserialized, decoder);

    assertEquals(specificRecord, deserialized);
  }

  @Test
  public void testStringables() throws IOException {
    StringablesRecord.Builder newBuilder = StringablesRecord.newBuilder();
    newBuilder.setValue(new BigDecimal("42.11"));
    HashMap<String, BigDecimal> mapWithBigDecimalElements = new HashMap<>();
    mapWithBigDecimalElements.put("test", new BigDecimal("11.11"));
    newBuilder.setMapWithBigDecimalElements(mapWithBigDecimalElements);
    HashMap<BigInteger, String> mapWithBigIntKeys = new HashMap<>();
    mapWithBigIntKeys.put(BigInteger.ONE, "test");
    newBuilder.setMapWithBigIntKeys(mapWithBigIntKeys);
    StringablesRecord stringablesRecord = newBuilder.build();

    byte[] recordBytes = serializeRecord(stringablesRecord);

    Decoder decoder = DecoderFactory.get().binaryDecoder(recordBytes, null);
    SpecificDatumReader<StringablesRecord> specificDatumReader = new SpecificDatumReader<>(StringablesRecord.SCHEMA$);
    StringablesRecord deserialized = new StringablesRecord();
    specificDatumReader.read(deserialized, decoder);

    assertEquals(stringablesRecord, deserialized);

  }

}
