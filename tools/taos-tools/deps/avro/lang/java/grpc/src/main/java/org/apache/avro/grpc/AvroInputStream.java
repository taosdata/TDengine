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

package org.apache.avro.grpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.grpc.Drainable;

/**
 * An {@link InputStream} backed by Avro RPC request/response that can drained
 * to a{@link OutputStream}.
 */
public abstract class AvroInputStream extends InputStream implements Drainable {
  /**
   * Container to hold the serialized Avro payload when its read before draining
   * it.
   */
  private ByteArrayInputStream partial;

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return getPartialInternal().read(b, off, len);
  }

  @Override
  public int read() throws IOException {
    return getPartialInternal().read();
  }

  private ByteArrayInputStream getPartialInternal() throws IOException {
    if (partial == null) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      drainTo(outputStream);
      partial = new ByteArrayInputStream(outputStream.toByteArray());
    }
    return partial;
  }

  protected ByteArrayInputStream getPartial() {
    return partial;
  }

  /**
   * An {@link OutputStream} that writes to a target {@link OutputStream} and
   * provides total number of bytes written to it.
   */
  protected static class CountingOutputStream extends OutputStream {
    private final OutputStream target;
    private int writtenCount = 0;

    public CountingOutputStream(OutputStream target) {
      this.target = target;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      target.write(b, off, len);
      writtenCount += len;
    }

    @Override
    public void write(int b) throws IOException {
      target.write(b);
      writtenCount += 1;
    }

    @Override
    public void flush() throws IOException {
      target.flush();
    }

    @Override
    public void close() throws IOException {
      target.close();
    }

    public int getWrittenCount() {
      return writtenCount;
    }
  }
}
