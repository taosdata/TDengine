/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestBinaryMessageEncoding {
  private static final Schema SCHEMA_V1 = SchemaBuilder.record("TestRecord").fields().requiredInt("id")
      .optionalString("msg").endRecord();

  private static final GenericRecordBuilder V1_BUILDER = new GenericRecordBuilder(SCHEMA_V1);

  private static final List<Record> V1_RECORDS = Arrays.asList(V1_BUILDER.set("id", 1).set("msg", "m-1").build(),
      V1_BUILDER.set("id", 2).set("msg", "m-2").build(), V1_BUILDER.set("id", 4).set("msg", "m-4").build(),
      V1_BUILDER.set("id", 6).set("msg", "m-6").build());

  private static final Schema SCHEMA_V2 = SchemaBuilder.record("TestRecord").fields().requiredLong("id").name("message")
      .aliases("msg").type().optional().stringType().optionalDouble("data").endRecord();

  private static final GenericRecordBuilder V2_BUILDER = new GenericRecordBuilder(SCHEMA_V2);

  private static final List<Record> V2_RECORDS = Arrays.asList(
      V2_BUILDER.set("id", 3L).set("message", "m-3").set("data", 12.3).build(),
      V2_BUILDER.set("id", 5L).set("message", "m-5").set("data", 23.4).build(),
      V2_BUILDER.set("id", 7L).set("message", "m-7").set("data", 34.5).build(),
      V2_BUILDER.set("id", 8L).set("message", "m-8").set("data", 35.6).build());

  @Test
  public void testByteBufferRoundTrip() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    Record copy = decoder.decode(encoder.encode(V2_RECORDS.get(0)));

    Assert.assertNotSame("Copy should not be the same object", copy, V2_RECORDS.get(0));
    Assert.assertEquals("Record should be identical after round-trip", V2_RECORDS.get(0), copy);
  }

  @Test
  public void testSchemaEvolution() throws Exception {
    List<ByteBuffer> buffers = new ArrayList<>();
    List<Record> records = new ArrayList<>();

    records.addAll(V1_RECORDS);
    records.addAll(V2_RECORDS);

    MessageEncoder<Record> v1Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);
    MessageEncoder<Record> v2Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);

    for (Record record : records) {
      if (record.getSchema().equals(SCHEMA_V1)) {
        buffers.add(v1Encoder.encode(record));
      } else {
        buffers.add(v2Encoder.encode(record));
      }
    }

    Set<Record> allAsV2 = new HashSet<>(V2_RECORDS);

    allAsV2.add(V2_BUILDER.set("id", 1L).set("message", "m-1").clear("data").build());
    allAsV2.add(V2_BUILDER.set("id", 2L).set("message", "m-2").clear("data").build());
    allAsV2.add(V2_BUILDER.set("id", 4L).set("message", "m-4").clear("data").build());
    allAsV2.add(V2_BUILDER.set("id", 6L).set("message", "m-6").clear("data").build());

    BinaryMessageDecoder<Record> v2Decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);
    v2Decoder.addSchema(SCHEMA_V1);

    Set<Record> decodedUsingV2 = new HashSet<>();
    for (ByteBuffer buffer : buffers) {
      decodedUsingV2.add(v2Decoder.decode(buffer));
    }

    Assert.assertEquals(allAsV2, decodedUsingV2);
  }

  @Test(expected = MissingSchemaException.class)
  public void testCompatibleReadFailsWithoutSchema() throws Exception {
    MessageEncoder<Record> v1Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);
    BinaryMessageDecoder<Record> v2Decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(3));

    v2Decoder.decode(v1Buffer);
  }

  @Test
  public void testCompatibleReadWithSchema() throws Exception {
    MessageEncoder<Record> v1Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);
    BinaryMessageDecoder<Record> v2Decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);
    v2Decoder.addSchema(SCHEMA_V1);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(3));

    Record record = v2Decoder.decode(v1Buffer);

    Assert.assertEquals(V2_BUILDER.set("id", 6L).set("message", "m-6").clear("data").build(), record);
  }

  @Test
  public void testCompatibleReadWithSchemaFromLookup() throws Exception {
    MessageEncoder<Record> v1Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);

    SchemaStore.Cache schemaCache = new SchemaStore.Cache();
    schemaCache.addSchema(SCHEMA_V1);
    BinaryMessageDecoder<Record> v2Decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2, schemaCache);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(2));

    Record record = v2Decoder.decode(v1Buffer);

    Assert.assertEquals(V2_BUILDER.set("id", 4L).set("message", "m-4").clear("data").build(), record);
  }

  @Test
  public void testIdenticalReadWithSchemaFromLookup() throws Exception {
    MessageEncoder<Record> v1Encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);

    SchemaStore.Cache schemaCache = new SchemaStore.Cache();
    schemaCache.addSchema(SCHEMA_V1);
    // The null readSchema should not throw an NPE, but trigger the
    // BinaryMessageEncoder to use the write schema as read schema
    BinaryMessageDecoder<Record> genericDecoder = new BinaryMessageDecoder<>(GenericData.get(), null, schemaCache);

    ByteBuffer v1Buffer = v1Encoder.encode(V1_RECORDS.get(2));

    Record record = genericDecoder.decode(v1Buffer);

    Assert.assertEquals(V1_RECORDS.get(2), record);
  }

  @Test
  public void testBufferReuse() throws Exception {
    // This test depends on the serialized version of record 1 being smaller or
    // the same size as record 0 so that the reused ByteArrayOutputStream won't
    // expand its internal buffer.
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1, false);

    ByteBuffer b0 = encoder.encode(V1_RECORDS.get(0));
    ByteBuffer b1 = encoder.encode(V1_RECORDS.get(1));

    Assert.assertEquals(b0.array(), b1.array());

    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V1);
    Assert.assertEquals("Buffer was reused, decode(b0) should be record 1", V1_RECORDS.get(1), decoder.decode(b0));
  }

  @Test
  public void testBufferCopy() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V1);

    ByteBuffer b0 = encoder.encode(V1_RECORDS.get(0));
    ByteBuffer b1 = encoder.encode(V1_RECORDS.get(1));

    Assert.assertNotEquals(b0.array(), b1.array());

    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V1);

    // bytes are not changed by reusing the encoder
    Assert.assertEquals("Buffer was copied, decode(b0) should be record 0", V1_RECORDS.get(0), decoder.decode(b0));
  }

  @Test(expected = AvroRuntimeException.class)
  public void testByteBufferMissingPayload() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));

    buffer.limit(12);

    decoder.decode(buffer);
  }

  @Test(expected = BadHeaderException.class)
  public void testByteBufferMissingFullHeader() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));

    buffer.limit(8);

    decoder.decode(buffer);
  }

  @Test(expected = BadHeaderException.class)
  public void testByteBufferBadMarkerByte() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[0] = 0x00;

    decoder.decode(buffer);
  }

  @Test(expected = BadHeaderException.class)
  public void testByteBufferBadVersionByte() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[1] = 0x00;

    decoder.decode(buffer);
  }

  @Test(expected = MissingSchemaException.class)
  public void testByteBufferUnknownSchema() throws Exception {
    MessageEncoder<Record> encoder = new BinaryMessageEncoder<>(GenericData.get(), SCHEMA_V2);
    MessageDecoder<Record> decoder = new BinaryMessageDecoder<>(GenericData.get(), SCHEMA_V2);

    ByteBuffer buffer = encoder.encode(V2_RECORDS.get(0));
    buffer.array()[4] = 0x00;

    decoder.decode(buffer);
  }
}
