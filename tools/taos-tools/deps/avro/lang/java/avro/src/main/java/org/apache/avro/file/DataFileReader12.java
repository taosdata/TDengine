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

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.Schema;
import org.apache.avro.UnknownAvroCodecException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;

/** Read files written by Avro version 1.2. */
public class DataFileReader12<D> implements FileReader<D>, Closeable {
  private static final byte VERSION = 0;
  static final byte[] MAGIC = new byte[] { (byte) 'O', (byte) 'b', (byte) 'j', VERSION };
  private static final long FOOTER_BLOCK = -1;
  private static final int SYNC_SIZE = 16;
  private static final String SCHEMA = "schema";
  private static final String SYNC = "sync";
  private static final String CODEC = "codec";
  private static final String NULL_CODEC = "null";

  private Schema schema;
  private DatumReader<D> reader;
  private DataFileReader.SeekableInputStream in;
  private BinaryDecoder vin;

  private Map<String, byte[]> meta = new HashMap<>();

  private long blockCount; // # entries in block
  private long blockStart;
  private byte[] sync = new byte[SYNC_SIZE];
  private byte[] syncBuffer = new byte[SYNC_SIZE];

  /** Construct a reader for a file. */
  public DataFileReader12(SeekableInput sin, DatumReader<D> reader) throws IOException {
    this.in = new DataFileReader.SeekableInputStream(sin);

    byte[] magic = new byte[4];
    in.read(magic);
    if (!Arrays.equals(MAGIC, magic))
      throw new InvalidAvroMagicException("Not a data file.");

    long length = in.length();
    in.seek(length - 4);
    int footerSize = (in.read() << 24) + (in.read() << 16) + (in.read() << 8) + in.read();
    seek(length - footerSize);
    long l = vin.readMapStart();
    if (l > 0) {
      do {
        for (long i = 0; i < l; i++) {
          String key = vin.readString(null).toString();
          ByteBuffer value = vin.readBytes(null);
          byte[] bb = new byte[value.remaining()];
          value.get(bb);
          meta.put(key, bb);
        }
      } while ((l = vin.mapNext()) != 0);
    }

    this.sync = getMeta(SYNC);
    String codec = getMetaString(CODEC);
    if (codec != null && !codec.equals(NULL_CODEC)) {
      throw new UnknownAvroCodecException("Unknown codec: " + codec);
    }
    this.schema = new Schema.Parser().parse(getMetaString(SCHEMA));
    this.reader = reader;

    reader.setSchema(schema);

    seek(MAGIC.length); // seek to start
  }

  /** Return the value of a metadata property. */
  public synchronized byte[] getMeta(String key) {
    return meta.get(key);
  }

  /** Return the value of a metadata property. */
  public synchronized String getMetaString(String key) {
    byte[] value = getMeta(key);
    if (value == null) {
      return null;
    }
    return new String(value, StandardCharsets.UTF_8);
  }

  /** Return the value of a metadata property. */
  public synchronized long getMetaLong(String key) {
    return Long.parseLong(getMetaString(key));
  }

  /** Return the schema used in this file. */
  @Override
  public Schema getSchema() {
    return schema;
  }

  // Iterator and Iterable implementation
  private D peek;

  @Override
  public Iterator<D> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (peek != null || blockCount != 0)
      return true;
    this.peek = next();
    return peek != null;
  }

  @Override
  public D next() {
    if (peek != null) {
      D result = peek;
      peek = null;
      return result;
    }
    try {
      return next(null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /** Return the next datum in the file. */
  @Override
  public synchronized D next(D reuse) throws IOException {
    while (blockCount == 0) { // at start of block

      if (in.tell() == in.length()) // at eof
        return null;

      skipSync(); // skip a sync

      blockCount = vin.readLong(); // read blockCount

      if (blockCount == FOOTER_BLOCK) {
        seek(vin.readLong() + in.tell()); // skip a footer
      }
    }
    blockCount--;
    return reader.read(reuse, vin);
  }

  private void skipSync() throws IOException {
    vin.readFixed(syncBuffer);
    if (!Arrays.equals(syncBuffer, sync))
      throw new IOException("Invalid sync!");
  }

  /**
   * Move to the specified synchronization point, as returned by
   * {@link DataFileWriter#sync()}.
   */
  public synchronized void seek(long position) throws IOException {
    in.seek(position);
    blockCount = 0;
    blockStart = position;
    vin = DecoderFactory.get().binaryDecoder(in, vin);
  }

  /** Move to the next synchronization point after a position. */
  @Override
  public synchronized void sync(long position) throws IOException {
    if (in.tell() + SYNC_SIZE >= in.length()) {
      seek(in.length());
      return;
    }
    in.seek(position);
    vin.readFixed(syncBuffer);
    for (int i = 0; in.tell() < in.length(); i++) {
      int j = 0;
      for (; j < sync.length; j++) {
        if (sync[j] != syncBuffer[(i + j) % sync.length])
          break;
      }
      if (j == sync.length) { // position before sync
        seek(in.tell() - SYNC_SIZE);
        return;
      }
      syncBuffer[i % sync.length] = (byte) in.read();
    }
    seek(in.length());
  }

  /** Return true if past the next synchronization point after a position. */
  @Override
  public boolean pastSync(long position) throws IOException {
    return ((blockStart >= position + SYNC_SIZE) || (blockStart >= in.length()));
  }

  /** Return the current position in the input. */
  @Override
  public long tell() throws IOException {
    return in.tell();
  }

  /** Close this reader. */
  @Override
  public synchronized void close() throws IOException {
    in.close();
  }

}
