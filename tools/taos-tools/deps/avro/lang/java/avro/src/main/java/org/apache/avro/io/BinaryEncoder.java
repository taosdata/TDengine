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
package org.apache.avro.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.util.Utf8;

/**
 * An abstract {@link Encoder} for Avro's binary encoding.
 * <p/>
 * To construct and configure instances, use {@link EncoderFactory}
 *
 * @see EncoderFactory
 * @see BufferedBinaryEncoder
 * @see DirectBinaryEncoder
 * @see BlockingBinaryEncoder
 * @see Encoder
 * @see Decoder
 */
public abstract class BinaryEncoder extends Encoder {

  @Override
  public void writeNull() throws IOException {
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    this.writeBytes(utf8.getBytes(), 0, utf8.getByteLength());
  }

  @Override
  public void writeString(String string) throws IOException {
    if (0 == string.length()) {
      writeZero();
      return;
    }
    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    writeInt(bytes.length);
    writeFixed(bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    int len = bytes.limit() - bytes.position();
    if (0 == len) {
      writeZero();
    } else {
      writeInt(len);
      writeFixed(bytes);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    if (0 == len) {
      writeZero();
      return;
    }
    this.writeInt(len);
    this.writeFixed(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    this.writeInt(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    if (itemCount > 0) {
      this.writeLong(itemCount);
    }
  }

  @Override
  public void startItem() throws IOException {
  }

  @Override
  public void writeArrayEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeMapStart() throws IOException {
  }

  @Override
  public void writeMapEnd() throws IOException {
    writeZero();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    writeInt(unionIndex);
  }

  /** Write a zero byte to the underlying output. **/
  protected abstract void writeZero() throws IOException;

  /**
   * Returns the number of bytes currently buffered by this encoder. If this
   * Encoder does not buffer, this will always return zero.
   * <p/>
   * Call {@link #flush()} to empty the buffer to the underlying output.
   */
  public abstract int bytesBuffered();

}
