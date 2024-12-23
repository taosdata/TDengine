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
package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * See AVRO-2908
 */
public class TestSkipEnumSchema {
  @Test
  public void testSkipEnum() throws IOException {
    Schema enumSchema = SchemaBuilder.builder().enumeration("enum").symbols("en1", "en2");
    EnumSymbol enumSymbol = new EnumSymbol(enumSchema, "en1");

    GenericDatumWriter<EnumSymbol> datumWriter = new GenericDatumWriter<>(enumSchema);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().validatingEncoder(enumSchema,
        EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null));
    datumWriter.write(enumSymbol, encoder);
    encoder.flush();

    Decoder decoder = DecoderFactory.get().validatingDecoder(enumSchema,
        DecoderFactory.get().binaryDecoder(byteArrayOutputStream.toByteArray(), null));

    GenericDatumReader.skip(enumSchema, decoder);
  }
}
