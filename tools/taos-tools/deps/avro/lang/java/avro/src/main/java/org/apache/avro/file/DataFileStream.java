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
package org.apache.avro.file;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;

/**
 * Streaming access to files written by {@link DataFileWriter}. Use
 * {@link DataFileReader} for file-based input.
 *
 * @see DataFileWriter
 */
public class DataFileStream<D> implements Iterator<D>, Iterable<D>, Closeable {

  /**
   * A handle that can be used to reopen a DataFile without re-reading the header
   * of the stream.
   */
  public static final class Header {
    Schema schema;
    Map<String, byte[]> meta = new HashMap<>();
    private transient List<String> metaKeyList = new ArrayList<>();
    byte[] sync = new byte[DataFileConstants.SYNC_SIZE];

    private Header() {
    }
  }

  private DatumReader<D> reader;
  private long blockSize;
  private boolean availableBlock = false;
  private Header header;

  /** Decoder on raw input stream. (Used for metadata.) */
  BinaryDecoder vin;
  /**
   * Secondary decoder, for datums. (Different than vin for block segments.)
   */
  BinaryDecoder datumIn = null;

  ByteBuffer blockBuffer;
  long blockCount; // # entries in block
  long blockRemaining; // # entries remaining in block
  byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];
  private Codec codec;

  /**
   * Construct a reader for an input stream. For file-based input, use
   * {@link DataFileReader}. This will buffer, wrapping with a
   * {@link java.io.BufferedInputStream} is not necessary.
   */
  public DataFileStream(InputStream in, DatumReader<D> reader) throws IOException {
    this.reader = reader;
    initialize(in);
  }

  /**
   * create an uninitialized DataFileStream
   */
  protected DataFileStream(DatumReader<D> reader) throws IOException {
    this.reader = reader;
  }

  /** Initialize the stream by reading from its head. */
  void initialize(InputStream in) throws IOException {
    this.header = new Header();
    this.vin = DecoderFactory.get().binaryDecoder(in, vin);
    byte[] magic = new byte[DataFileConstants.MAGIC.length];
    try {
      vin.readFixed(magic); // read magic
    } catch (IOException e) {
      throw new IOException("Not an Avro data file.", e);
    }
    if (!Arrays.equals(DataFileConstants.MAGIC, magic))
      throw new InvalidAvroMagicException("Not an Avro data file.");

    long l = vin.readMapStart(); // read meta data
    if (l > 0) {
      do {
        for (long i = 0; i < l; i++) {
          String key = vin.readString(null).toString();
          ByteBuffer value = vin.readBytes(null);
          byte[] bb = new byte[value.remaining()];
          value.get(bb);
          header.meta.put(key, bb);
          header.metaKeyList.add(key);
        }
      } while ((l = vin.mapNext()) != 0);
    }
    vin.readFixed(header.sync); // read sync

    // finalize the header
    header.metaKeyList = Collections.unmodifiableList(header.metaKeyList);
    header.schema = new Schema.Parser().setValidate(false).setValidateDefaults(false)
        .parse(getMetaString(DataFileConstants.SCHEMA));
    this.codec = resolveCodec();
    reader.setSchema(header.schema);
  }

  /** Initialize the stream without reading from it. */
  void initialize(InputStream in, Header header) throws IOException {
    this.header = header;
    this.codec = resolveCodec();
    reader.setSchema(header.schema);
  }

  Codec resolveCodec() {
    String codecStr = getMetaString(DataFileConstants.CODEC);
    if (codecStr != null) {
      return CodecFactory.fromString(codecStr).createInstance();
    } else {
      return CodecFactory.nullCodec().createInstance();
    }
  }

  /**
   * A handle that can be used to reopen this stream without rereading the head.
   */
  public Header getHeader() {
    return header;
  }

  /** Return the schema used in this file. */
  public Schema getSchema() {
    return header.schema;
  }

  /** Return the list of keys in the metadata */
  public List<String> getMetaKeys() {
    return header.metaKeyList;
  }

  /** Return the value of a metadata property. */
  public byte[] getMeta(String key) {
    return header.meta.get(key);
  }

  /** Return the value of a metadata property. */
  public String getMetaString(String key) {
    byte[] value = getMeta(key);
    if (value == null) {
      return null;
    }
    return new String(value, StandardCharsets.UTF_8);
  }

  /** Return the value of a metadata property. */
  public long getMetaLong(String key) {
    return Long.parseLong(getMetaString(key));
  }

  /**
   * Returns an iterator over entries in this file. Note that this iterator is
   * shared with other users of the file: it does not contain a separate pointer
   * into the file.
   */
  @Override
  public Iterator<D> iterator() {
    return this;
  }

  private DataBlock block = null;

  /** True if more entries remain in this file. */
  @Override
  public boolean hasNext() {
    try {
      if (blockRemaining == 0) {
        // check that the previous block was finished
        if (null != datumIn) {
          boolean atEnd = datumIn.isEnd();
          if (!atEnd) {
            throw new IOException("Block read partially, the data may be corrupt");
          }
        }
        if (hasNextBlock()) {
          block = nextRawBlock(block);
          block.decompressUsing(codec);
          blockBuffer = block.getAsByteBuffer();
          datumIn = DecoderFactory.get().binaryDecoder(blockBuffer.array(),
              blockBuffer.arrayOffset() + blockBuffer.position(), blockBuffer.remaining(), datumIn);
        }
      }
      return blockRemaining != 0;
    } catch (EOFException e) { // at EOF
      return false;
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Read the next datum in the file.
   *
   * @throws NoSuchElementException if no more remain in the file.
   */
  @Override
  public D next() {
    try {
      return next(null);
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Read the next datum from the file.
   *
   * @param reuse an instance to reuse.
   * @throws NoSuchElementException if no more remain in the file.
   */
  public D next(D reuse) throws IOException {
    if (!hasNext())
      throw new NoSuchElementException();
    D result = reader.read(reuse, datumIn);
    if (0 == --blockRemaining) {
      blockFinished();
    }
    return result;
  }

  /** Expert: Return the next block in the file, as binary-encoded data. */
  public ByteBuffer nextBlock() throws IOException {
    if (!hasNext())
      throw new NoSuchElementException();
    if (blockRemaining != blockCount)
      throw new IllegalStateException("Not at block start.");
    blockRemaining = 0;
    datumIn = null;
    return blockBuffer;
  }

  /** Expert: Return the count of items in the current block. */
  public long getBlockCount() {
    return blockCount;
  }

  /** Expert: Return the size in bytes (uncompressed) of the current block. */
  public long getBlockSize() {
    return blockSize;
  }

  protected void blockFinished() throws IOException {
    // nothing for the stream impl
  }

  boolean hasNextBlock() {
    try {
      if (availableBlock)
        return true;
      if (vin.isEnd())
        return false;
      blockRemaining = vin.readLong(); // read block count
      blockSize = vin.readLong(); // read block size
      if (blockSize > Integer.MAX_VALUE || blockSize < 0) {
        throw new IOException("Block size invalid or too large for this " + "implementation: " + blockSize);
      }
      blockCount = blockRemaining;
      availableBlock = true;
      return true;
    } catch (EOFException eof) {
      return false;
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  DataBlock nextRawBlock(DataBlock reuse) throws IOException {
    if (!hasNextBlock()) {
      throw new NoSuchElementException();
    }
    if (reuse == null || reuse.data.length < (int) blockSize) {
      reuse = new DataBlock(blockRemaining, (int) blockSize);
    } else {
      reuse.numEntries = blockRemaining;
      reuse.blockSize = (int) blockSize;
    }
    // throws if it can't read the size requested
    vin.readFixed(reuse.data, 0, reuse.blockSize);
    vin.readFixed(syncBuffer);
    availableBlock = false;
    if (!Arrays.equals(syncBuffer, header.sync))
      throw new IOException("Invalid sync!");
    return reuse;
  }

  /** Not supported. */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** Close this reader. */
  @Override
  public void close() throws IOException {
    vin.inputStream().close();
  }

  static class DataBlock {
    private byte[] data;
    private long numEntries;
    private int blockSize;
    private int offset = 0;
    private boolean flushOnWrite = true;

    private DataBlock(long numEntries, int blockSize) {
      this.data = new byte[blockSize];
      this.numEntries = numEntries;
      this.blockSize = blockSize;
    }

    DataBlock(ByteBuffer block, long numEntries) {
      this.data = block.array();
      this.blockSize = block.remaining();
      this.offset = block.arrayOffset() + block.position();
      this.numEntries = numEntries;
    }

    byte[] getData() {
      return data;
    }

    long getNumEntries() {
      return numEntries;
    }

    int getBlockSize() {
      return blockSize;
    }

    boolean isFlushOnWrite() {
      return flushOnWrite;
    }

    void setFlushOnWrite(boolean flushOnWrite) {
      this.flushOnWrite = flushOnWrite;
    }

    ByteBuffer getAsByteBuffer() {
      return ByteBuffer.wrap(data, offset, blockSize);
    }

    void decompressUsing(Codec c) throws IOException {
      ByteBuffer result = c.decompress(getAsByteBuffer());
      data = result.array();
      blockSize = result.remaining();
    }

    void compressUsing(Codec c) throws IOException {
      ByteBuffer result = c.compress(getAsByteBuffer());
      data = result.array();
      blockSize = result.remaining();
    }

    void writeBlockTo(BinaryEncoder e, byte[] sync) throws IOException {
      e.writeLong(this.numEntries);
      e.writeLong(this.blockSize);
      e.writeFixed(this.data, offset, this.blockSize);
      e.writeFixed(sync);
      if (flushOnWrite) {
        e.flush();
      }
    }

  }
}
