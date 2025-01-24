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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link MessageDecoder} that deserializes from raw datum bytes.
 * <p>
 * This class uses the schema passed to its constructor when decoding buffers.
 * To decode buffers that have different schemas, use
 * {@link BinaryMessageEncoder} and {@link BinaryMessageDecoder}.
 * <p>
 * This will not throw {@link BadHeaderException} because it expects no header,
 * and will not throw {@link MissingSchemaException} because it always uses the
 * read schema from its constructor.
 * <p>
 * This class is thread-safe.
 */
public class RawMessageDecoder<D> extends MessageDecoder.BaseDecoder<D> {

  private static final ThreadLocal<BinaryDecoder> DECODER = new ThreadLocal<>();

  private final DatumReader<D> reader;

  /**
   * Creates a new {@link RawMessageDecoder} that uses the given
   * {@link GenericData data model} to construct datum instances described by the
   * {@link Schema schema}.
   * <p>
   * The {@code schema} is used as both the expected schema (read schema) and for
   * the schema of payloads that are decoded (written schema).
   *
   * @param model  the {@link GenericData data model} for datum instances
   * @param schema the {@link Schema} used to construct datum instances and to
   *               decode buffers.
   */
  public RawMessageDecoder(GenericData model, Schema schema) {
    this(model, schema, schema);
  }

  /**
   * Creates a new {@link RawMessageDecoder} that uses the given
   * {@link GenericData data model} to construct datum instances described by the
   * {@link Schema readSchema}.
   * <p>
   * The {@code readSchema} is used for the expected schema and the
   * {@code writeSchema} is the schema used to decode buffers. The
   * {@code writeSchema} must be the schema that was used to encode all buffers
   * decoded by this class.
   *
   * @param model       the {@link GenericData data model} for datum instances
   * @param readSchema  the {@link Schema} used to construct datum instances
   * @param writeSchema the {@link Schema} used to decode buffers
   */
  public RawMessageDecoder(GenericData model, Schema writeSchema, Schema readSchema) {
    Schema writeSchema1 = writeSchema;
    Schema readSchema1 = readSchema;
    this.reader = model.createDatumReader(writeSchema1, readSchema1);
  }

  @Override
  public D decode(InputStream stream, D reuse) {
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(stream, DECODER.get());
    DECODER.set(decoder);
    try {
      return reader.read(reuse, decoder);
    } catch (IOException e) {
      throw new AvroRuntimeException("Decoding datum failed", e);
    }
  }
}
