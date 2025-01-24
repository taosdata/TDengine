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

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.internal.ThreadLocalWithInitial;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link MessageDecoder} that reads a binary-encoded datum. This checks for
 * the datum header and decodes the payload with the schema that corresponds to
 * the 8-byte schema fingerprint.
 * <p>
 * Instances can decode message payloads for known {@link Schema schemas}, which
 * are schemas added using {@link #addSchema(Schema)}, schemas resolved by the
 * {@link SchemaStore} passed to the constructor, or the expected schema passed
 * to the constructor. Messages encoded using an unknown schema will cause
 * instances to throw a {@link MissingSchemaException}.
 * <p>
 * It is safe to continue using instances of this class after {@link #decode}
 * throws {@link BadHeaderException} or {@link MissingSchemaException}.
 * <p>
 * This class is thread-safe.
 */
public class BinaryMessageDecoder<D> extends MessageDecoder.BaseDecoder<D> {

  private static final ThreadLocal<byte[]> HEADER_BUFFER = ThreadLocalWithInitial.of(() -> new byte[10]);

  private static final ThreadLocal<ByteBuffer> FP_BUFFER = ThreadLocalWithInitial.of(() -> {
    byte[] header = HEADER_BUFFER.get();
    return ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN);
  });

  private final GenericData model;
  private final Schema readSchema;
  private final SchemaStore resolver;

  private final Map<Long, RawMessageDecoder<D>> codecByFingerprint = new ConcurrentHashMap<>();

  /**
   * Creates a new {@link BinaryMessageEncoder} that uses the given
   * {@link GenericData data model} to construct datum instances described by the
   * {@link Schema schema}.
   * <p>
   * The {@code readSchema} is as used the expected schema (read schema). Datum
   * instances created by this class will be described by the expected schema.
   * <p>
   * If {@code readSchema} is {@code null}, the write schema of an incoming buffer
   * is used as read schema for that datum instance.
   * <p>
   * The schema used to decode incoming buffers is determined by the schema
   * fingerprint encoded in the message header. This class can decode messages
   * that were encoded using the {@code readSchema} (if any) and other schemas
   * that are added using {@link #addSchema(Schema)}.
   *
   * @param model      the {@link GenericData data model} for datum instances
   * @param readSchema the {@link Schema} used to construct datum instances
   */
  public BinaryMessageDecoder(GenericData model, Schema readSchema) {
    this(model, readSchema, null);
  }

  /**
   * Creates a new {@link BinaryMessageEncoder} that uses the given
   * {@link GenericData data model} to construct datum instances described by the
   * {@link Schema schema}.
   * <p>
   * The {@code readSchema} is used as the expected schema (read schema). Datum
   * instances created by this class will be described by the expected schema.
   * <p>
   * If {@code readSchema} is {@code null}, the write schema of an incoming buffer
   * is used as read schema for that datum instance.
   * <p>
   * The schema used to decode incoming buffers is determined by the schema
   * fingerprint encoded in the message header. This class can decode messages
   * that were encoded using the {@code readSchema} (if any), other schemas that
   * are added using {@link #addSchema(Schema)}, or schemas returned by the
   * {@code resolver}.
   *
   * @param model      the {@link GenericData data model} for datum instances
   * @param readSchema the {@link Schema} used to construct datum instances
   * @param resolver   a {@link SchemaStore} used to find schemas by fingerprint
   */
  public BinaryMessageDecoder(GenericData model, Schema readSchema, SchemaStore resolver) {
    this.model = model;
    this.readSchema = readSchema;
    this.resolver = resolver;
    if (readSchema != null) {
      addSchema(readSchema);
    }
  }

  /**
   * Adds a {@link Schema} that can be used to decode buffers.
   *
   * @param writeSchema a {@link Schema} to use when decoding buffers
   */
  public void addSchema(Schema writeSchema) {
    long fp = SchemaNormalization.parsingFingerprint64(writeSchema);
    final Schema actualReadSchema = this.readSchema != null ? this.readSchema : writeSchema;
    codecByFingerprint.put(fp, new RawMessageDecoder<D>(model, writeSchema, actualReadSchema));
  }

  private RawMessageDecoder<D> getDecoder(long fp) {
    RawMessageDecoder<D> decoder = codecByFingerprint.get(fp);
    if (decoder != null) {
      return decoder;
    }

    if (resolver != null) {
      Schema writeSchema = resolver.findByFingerprint(fp);
      if (writeSchema != null) {
        addSchema(writeSchema);
        return codecByFingerprint.get(fp);
      }
    }

    throw new MissingSchemaException("Cannot resolve schema for fingerprint: " + fp);
  }

  @Override
  public D decode(InputStream stream, D reuse) throws IOException {
    byte[] header = HEADER_BUFFER.get();
    try {
      if (!readFully(stream, header)) {
        throw new BadHeaderException("Not enough header bytes");
      }
    } catch (IOException e) {
      throw new IOException("Failed to read header and fingerprint bytes", e);
    }

    if (BinaryMessageEncoder.V1_HEADER[0] != header[0] || BinaryMessageEncoder.V1_HEADER[1] != header[1]) {
      throw new BadHeaderException(String.format("Unrecognized header bytes: 0x%02X 0x%02X", header[0], header[1]));
    }

    RawMessageDecoder<D> decoder = getDecoder(FP_BUFFER.get().getLong(2));

    return decoder.decode(stream, reuse);
  }

  /**
   * Reads a buffer from a stream, making multiple read calls if necessary.
   *
   * @param stream an InputStream to read from
   * @param bytes  a buffer
   * @return true if the buffer is complete, false otherwise (stream ended)
   * @throws IOException
   */
  private boolean readFully(InputStream stream, byte[] bytes) throws IOException {
    int pos = 0;
    int bytesRead;
    while ((bytes.length - pos) > 0 && (bytesRead = stream.read(bytes, pos, bytes.length - pos)) > 0) {
      pos += bytesRead;
    }
    return (pos == bytes.length);
  }
}
