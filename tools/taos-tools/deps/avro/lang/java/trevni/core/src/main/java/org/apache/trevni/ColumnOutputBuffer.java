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
package org.apache.trevni;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class ColumnOutputBuffer {
  private ColumnFileWriter writer;
  private ColumnMetaData meta;
  private Codec codec;
  private Checksum checksum;
  private OutputBuffer buffer;
  private List<BlockDescriptor> blockDescriptors;
  private List<byte[]> blockData;
  private List<byte[]> firstValues;
  private int rowCount;
  private long size = 4; // room for block count

  public ColumnOutputBuffer(ColumnFileWriter writer, ColumnMetaData meta) throws IOException {
    this.writer = writer;
    this.meta = meta;
    this.codec = Codec.get(meta);
    this.checksum = Checksum.get(meta);
    this.buffer = new OutputBuffer();
    this.blockDescriptors = new ArrayList<>();
    this.blockData = new ArrayList<>();
    if (meta.hasIndexValues())
      this.firstValues = new ArrayList<>();
  }

  public ColumnMetaData getMeta() {
    return meta;
  }

  public OutputBuffer getBuffer() {
    return buffer;
  }

  public void startRow() throws IOException {
    if (buffer.isFull())
      flushBuffer();
  }

  public void writeLength(int length) throws IOException {
    throw new TrevniRuntimeException("Not an array column: " + meta);
  }

  public void writeValue(Object value) throws IOException {
    buffer.writeValue(value, meta.getType());
    if (meta.hasIndexValues() && rowCount == 0)
      firstValues.add(buffer.toByteArray());
  }

  public void endRow() throws IOException {
    rowCount++;
  }

  void flushBuffer() throws IOException {
    if (rowCount == 0)
      return;
    ByteBuffer raw = buffer.asByteBuffer();
    ByteBuffer c = codec.compress(raw);

    blockDescriptors.add(new BlockDescriptor(rowCount, raw.remaining(), c.remaining()));

    ByteBuffer data = ByteBuffer.allocate(c.remaining() + checksum.size());
    data.put(c);
    data.put(checksum.compute(raw));
    blockData.add(data.array());

    int sizeIncrement = (4 * 3) // descriptor
        + (firstValues != null // firstValue
            ? firstValues.get(firstValues.size() - 1).length
            : 0)
        + data.position(); // data

    writer.incrementSize(sizeIncrement);
    size += sizeIncrement;

    buffer = new OutputBuffer();
    rowCount = 0;
  }

  public long size() throws IOException {
    flushBuffer();
    return size;
  }

  public void writeTo(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();
    header.writeFixed32(blockDescriptors.size());
    for (int i = 0; i < blockDescriptors.size(); i++) {
      blockDescriptors.get(i).writeTo(header);
      if (meta.hasIndexValues())
        header.write(firstValues.get(i));
    }
    header.writeTo(out);

    for (byte[] data : blockData)
      out.write(data);
  }

}
