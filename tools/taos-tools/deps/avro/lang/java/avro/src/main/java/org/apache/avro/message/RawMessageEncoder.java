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
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.internal.ThreadLocalWithInitial;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A {@link MessageEncoder} that encodes only a datum's bytes, without
 * additional information (such as a schema fingerprint).
 * <p>
 * This class is thread-safe.
 */
public class RawMessageEncoder<D> implements MessageEncoder<D> {

  private static final ThreadLocal<BufferOutputStream> TEMP = ThreadLocalWithInitial.of(BufferOutputStream::new);

  private static final ThreadLocal<BinaryEncoder> ENCODER = new ThreadLocal<>();

  private final boolean copyOutputBytes;
  private final DatumWriter<D> writer;

  /**
   * Creates a new {@link RawMessageEncoder} that uses the given
   * {@link GenericData data model} to deconstruct datum instances described by
   * the {@link Schema schema}.
   * <p>
   * Buffers returned by {@link RawMessageEncoder#encode} are copied and will not
   * be modified by future calls to {@code encode}.
   *
   * @param model  the {@link GenericData data model} for datum instances
   * @param schema the {@link Schema} for datum instances
   */
  public RawMessageEncoder(GenericData model, Schema schema) {
    this(model, schema, true);
  }

  /**
   * Creates a new {@link RawMessageEncoder} that uses the given
   * {@link GenericData data model} to deconstruct datum instances described by
   * the {@link Schema schema}.
   * <p>
   * If {@code shouldCopy} is true, then buffers returned by
   * {@link RawMessageEncoder#encode} are copied and will not be modified by
   * future calls to {@code encode}.
   * <p>
   * If {@code shouldCopy} is false, then buffers returned by {@code encode} wrap
   * a thread-local buffer that can be reused by future calls to {@code encode},
   * but may not be. Callers should only set {@code shouldCopy} to false if the
   * buffer will be copied before the current thread's next call to
   * {@code encode}.
   *
   * @param model      the {@link GenericData data model} for datum instances
   * @param schema     the {@link Schema} for datum instances
   * @param shouldCopy whether to copy buffers before returning encoded results
   */
  public RawMessageEncoder(GenericData model, Schema schema, boolean shouldCopy) {
    Schema writeSchema = schema;
    this.copyOutputBytes = shouldCopy;
    this.writer = model.createDatumWriter(writeSchema);
  }

  @Override
  public ByteBuffer encode(D datum) throws IOException {
    BufferOutputStream temp = TEMP.get();
    temp.reset();

    encode(datum, temp);

    if (copyOutputBytes) {
      return temp.toBufferWithCopy();
    } else {
      return temp.toBufferWithoutCopy();
    }
  }

  @Override
  public void encode(D datum, OutputStream stream) throws IOException {
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(stream, ENCODER.get());
    ENCODER.set(encoder);
    writer.write(datum, encoder);
    encoder.flush();
  }

  private static class BufferOutputStream extends ByteArrayOutputStream {
    BufferOutputStream() {
    }

    ByteBuffer toBufferWithoutCopy() {
      return ByteBuffer.wrap(buf, 0, count);
    }

    ByteBuffer toBufferWithCopy() {
      return ByteBuffer.wrap(toByteArray());
    }
  }
}
