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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream.DataBlock;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.apache.commons.compress.utils.IOUtils;

/**
 * Stores in a file a sequence of data conforming to a schema. The schema is
 * stored in the file with the data. Each datum in a file is of the same schema.
 * Data is written with a {@link DatumWriter}. Data is grouped into
 * <i>blocks</i>. A synchronization marker is written between blocks, so that
 * files may be split. Blocks may be compressed. Extensible metadata is stored
 * at the end of the file. Files may be appended to.
 * 
 * @see DataFileReader
 */
public class DataFileWriter<D> implements Closeable, Flushable {
  private Schema schema;
  private DatumWriter<D> dout;

  private OutputStream underlyingStream;

  private BufferedFileOutputStream out;
  private BinaryEncoder vout;

  private final Map<String, byte[]> meta = new HashMap<>();

  private long blockCount; // # entries in current block

  private NonCopyingByteArrayOutputStream buffer;
  private BinaryEncoder bufOut;

  private byte[] sync; // 16 random bytes
  private int syncInterval = DataFileConstants.DEFAULT_SYNC_INTERVAL;

  private boolean isOpen;
  private Codec codec;

  private boolean flushOnEveryBlock = true;

  /** Construct a writer, not yet open. */
  public DataFileWriter(DatumWriter<D> dout) {
    this.dout = dout;
  }

  private void assertOpen() {
    if (!isOpen)
      throw new AvroRuntimeException("not open");
  }

  private void assertNotOpen() {
    if (isOpen)
      throw new AvroRuntimeException("already open");
  }

  /**
   * Configures this writer to use the given codec. May not be reset after writes
   * have begun.
   */
  public DataFileWriter<D> setCodec(CodecFactory c) {
    assertNotOpen();
    this.codec = c.createInstance();
    setMetaInternal(DataFileConstants.CODEC, codec.getName());
    return this;
  }

  /**
   * Set the synchronization interval for this file, in bytes. Valid values range
   * from 32 to 2^30 Suggested values are between 2K and 2M
   *
   * The stream is flushed by default at the end of each synchronization interval.
   *
   * If {@linkplain #setFlushOnEveryBlock(boolean)} is called with param set to
   * false, then the block may not be flushed to the stream after the sync marker
   * is written. In this case, the {@linkplain #flush()} must be called to flush
   * the stream.
   *
   * Invalid values throw IllegalArgumentException
   *
   * @param syncInterval the approximate number of uncompressed bytes to write in
   *                     each block
   * @return this DataFileWriter
   */
  public DataFileWriter<D> setSyncInterval(int syncInterval) {
    if (syncInterval < 32 || syncInterval > (1 << 30)) {
      throw new IllegalArgumentException("Invalid syncInterval value: " + syncInterval);
    }
    this.syncInterval = syncInterval;
    return this;
  }

  /** Open a new file for data matching a schema with a random sync. */
  public DataFileWriter<D> create(Schema schema, File file) throws IOException {
    SyncableFileOutputStream sfos = new SyncableFileOutputStream(file);
    try {
      return create(schema, sfos, null);
    } catch (final Throwable e) {
      IOUtils.closeQuietly(sfos);
      throw e;
    }
  }

  /** Open a new file for data matching a schema with a random sync. */
  public DataFileWriter<D> create(Schema schema, OutputStream outs) throws IOException {
    return create(schema, outs, null);
  }

  /** Open a new file for data matching a schema with an explicit sync. */
  public DataFileWriter<D> create(Schema schema, OutputStream outs, byte[] sync) throws IOException {
    assertNotOpen();

    this.schema = schema;
    setMetaInternal(DataFileConstants.SCHEMA, schema.toString());
    if (sync == null) {
      this.sync = generateSync();
    } else if (sync.length == 16) {
      this.sync = sync;
    } else {
      throw new IOException("sync must be exactly 16 bytes");
    }

    init(outs);

    vout.writeFixed(DataFileConstants.MAGIC); // write magic

    vout.writeMapStart(); // write metadata
    vout.setItemCount(meta.size());
    for (Map.Entry<String, byte[]> entry : meta.entrySet()) {
      vout.startItem();
      vout.writeString(entry.getKey());
      vout.writeBytes(entry.getValue());
    }
    vout.writeMapEnd();
    vout.writeFixed(this.sync); // write initial sync
    vout.flush(); // vout may be buffered, flush before writing to out
    return this;
  }

  /**
   * Set whether this writer should flush the block to the stream every time a
   * sync marker is written. By default, the writer will flush the buffer each
   * time a sync marker is written (if the block size limit is reached or the
   * {@linkplain #sync()} is called.
   * 
   * @param flushOnEveryBlock - If set to false, this writer will not flush the
   *                          block to the stream until {@linkplain #flush()} is
   *                          explicitly called.
   */
  public void setFlushOnEveryBlock(boolean flushOnEveryBlock) {
    this.flushOnEveryBlock = flushOnEveryBlock;
  }

  /**
   * @return - true if this writer flushes the block to the stream every time a
   *         sync marker is written. Else returns false.
   */
  public boolean isFlushOnEveryBlock() {
    return this.flushOnEveryBlock;
  }

  /** Open a writer appending to an existing file. */
  public DataFileWriter<D> appendTo(File file) throws IOException {
    try (SeekableInput input = new SeekableFileInput(file)) {
      OutputStream output = new SyncableFileOutputStream(file, true);
      return appendTo(input, output);
    }
    // output does not need to be closed here. It will be closed by invoking close()
    // of this writer.
  }

  /**
   * Open a writer appending to an existing file. <strong>Since 1.9.0 this method
   * does not close in.</strong>
   * 
   * @param in  reading the existing file.
   * @param out positioned at the end of the existing file.
   */
  public DataFileWriter<D> appendTo(SeekableInput in, OutputStream out) throws IOException {
    assertNotOpen();
    DataFileReader<D> reader = new DataFileReader<>(in, new GenericDatumReader<>());
    this.schema = reader.getSchema();
    this.sync = reader.getHeader().sync;
    this.meta.putAll(reader.getHeader().meta);
    byte[] codecBytes = this.meta.get(DataFileConstants.CODEC);
    if (codecBytes != null) {
      String strCodec = new String(codecBytes, StandardCharsets.UTF_8);
      this.codec = CodecFactory.fromString(strCodec).createInstance();
    } else {
      this.codec = CodecFactory.nullCodec().createInstance();
    }

    init(out);

    return this;
  }

  private void init(OutputStream outs) throws IOException {
    this.underlyingStream = outs;
    this.out = new BufferedFileOutputStream(outs);
    EncoderFactory efactory = new EncoderFactory();
    this.vout = efactory.directBinaryEncoder(out, null);
    dout.setSchema(schema);
    buffer = new NonCopyingByteArrayOutputStream(Math.min((int) (syncInterval * 1.25), Integer.MAX_VALUE / 2 - 1));
    this.bufOut = efactory.directBinaryEncoder(buffer, null);
    if (this.codec == null) {
      this.codec = CodecFactory.nullCodec().createInstance();
    }
    this.isOpen = true;
  }

  private static byte[] generateSync() {
    try {
      MessageDigest digester = MessageDigest.getInstance("MD5");
      long time = System.currentTimeMillis();
      digester.update((UUID.randomUUID() + "@" + time).getBytes(UTF_8));
      return digester.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private DataFileWriter<D> setMetaInternal(String key, byte[] value) {
    assertNotOpen();
    meta.put(key, value);
    return this;
  }

  private DataFileWriter<D> setMetaInternal(String key, String value) {
    return setMetaInternal(key, value.getBytes(UTF_8));
  }

  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, byte[] value) {
    if (isReservedMeta(key)) {
      throw new AvroRuntimeException("Cannot set reserved meta key: " + key);
    }
    return setMetaInternal(key, value);
  }

  public static boolean isReservedMeta(String key) {
    return key.startsWith("avro.");
  }

  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, String value) {
    return setMeta(key, value.getBytes(UTF_8));
  }

  /** Set a metadata property. */
  public DataFileWriter<D> setMeta(String key, long value) {
    return setMeta(key, Long.toString(value));
  }

  /**
   * Thrown by {@link #append(Object)} when an exception occurs while writing a
   * datum to the buffer. When this is thrown, the file is unaltered and may
   * continue to be appended to.
   */
  public static class AppendWriteException extends RuntimeException {
    public AppendWriteException(Exception e) {
      super(e);
    }
  }

  /**
   * Append a datum to the file.
   * 
   * @see AppendWriteException
   */
  public void append(D datum) throws IOException {
    assertOpen();
    int usedBuffer = bufferInUse();
    try {
      dout.write(datum, bufOut);
    } catch (IOException | RuntimeException e) {
      resetBufferTo(usedBuffer);
      throw new AppendWriteException(e);
    }
    blockCount++;
    writeIfBlockFull();
  }

  // if there is an error encoding, flush the encoder and then
  // reset the buffer position to contain size bytes, discarding the rest.
  // Otherwise the file will be corrupt with a partial record.
  private void resetBufferTo(int size) throws IOException {
    bufOut.flush();
    byte[] data = buffer.toByteArray();
    buffer.reset();
    buffer.write(data, 0, size);
  }

  /**
   * Expert: Append a pre-encoded datum to the file. No validation is performed to
   * check that the encoding conforms to the file's schema. Appending
   * non-conforming data may result in an unreadable file.
   */
  public void appendEncoded(ByteBuffer datum) throws IOException {
    assertOpen();
    bufOut.writeFixed(datum);
    blockCount++;
    writeIfBlockFull();
  }

  private int bufferInUse() {
    return (buffer.size() + bufOut.bytesBuffered());
  }

  private void writeIfBlockFull() throws IOException {
    if (bufferInUse() >= syncInterval)
      writeBlock();
  }

  /**
   * Appends data from another file. otherFile must have the same schema. Data
   * blocks will be copied without de-serializing data. If the codecs of the two
   * files are compatible, data blocks are copied directly without decompression.
   * If the codecs are not compatible, blocks from otherFile are uncompressed and
   * then compressed using this file's codec.
   * <p/>
   * If the recompress flag is set all blocks are decompressed and then compressed
   * using this file's codec. This is useful when the two files have compatible
   * compression codecs but different codec options. For example, one might append
   * a file compressed with deflate at compression level 1 to a file with deflate
   * at compression level 7. If <i>recompress</i> is false, blocks will be copied
   * without changing the compression level. If true, they will be converted to
   * the new compression level.
   * 
   * @param otherFile
   * @param recompress
   * @throws IOException
   */
  public void appendAllFrom(DataFileStream<D> otherFile, boolean recompress) throws IOException {
    assertOpen();
    // make sure other file has same schema
    Schema otherSchema = otherFile.getSchema();
    if (!this.schema.equals(otherSchema)) {
      throw new IOException("Schema from file " + otherFile + " does not match");
    }
    // flush anything written so far
    writeBlock();
    Codec otherCodec = otherFile.resolveCodec();
    DataBlock nextBlockRaw = null;
    if (codec.equals(otherCodec) && !recompress) {
      // copy raw bytes
      while (otherFile.hasNextBlock()) {
        nextBlockRaw = otherFile.nextRawBlock(nextBlockRaw);
        nextBlockRaw.writeBlockTo(vout, sync);
      }
    } else {
      while (otherFile.hasNextBlock()) {
        nextBlockRaw = otherFile.nextRawBlock(nextBlockRaw);
        nextBlockRaw.decompressUsing(otherCodec);
        nextBlockRaw.compressUsing(codec);
        nextBlockRaw.writeBlockTo(vout, sync);
      }
    }
  }

  private void writeBlock() throws IOException {
    if (blockCount > 0) {
      try {
        bufOut.flush();
        ByteBuffer uncompressed = buffer.asByteBuffer();
        DataBlock block = new DataBlock(uncompressed, blockCount);
        block.setFlushOnWrite(flushOnEveryBlock);
        block.compressUsing(codec);
        block.writeBlockTo(vout, sync);
      } finally {
        buffer.reset();
        blockCount = 0;
      }
    }
  }

  /**
   * Return the current position as a value that may be passed to
   * {@link DataFileReader#seek(long)}. Forces the end of the current block,
   * emitting a synchronization marker. By default, this will also flush the block
   * to the stream.
   *
   * If {@linkplain #setFlushOnEveryBlock(boolean)} is called with param set to
   * false, then this method may not flush the block. In this case, the
   * {@linkplain #flush()} must be called to flush the stream.
   */
  public long sync() throws IOException {
    assertOpen();
    writeBlock();
    return out.tell();
  }

  /**
   * Calls {@linkplain #sync()} and then flushes the current state of the file.
   */
  @Override
  public void flush() throws IOException {
    sync();
    vout.flush();
  }

  /**
   * If this writer was instantiated using a File or using an
   * {@linkplain Syncable} instance, this method flushes all buffers for this
   * writer to disk. In other cases, this method behaves exactly like
   * {@linkplain #flush()}.
   *
   * @throws IOException
   */
  public void fSync() throws IOException {
    flush();
    if (underlyingStream instanceof Syncable) {
      ((Syncable) underlyingStream).sync();
    }
  }

  /** Flush and close the file. */
  @Override
  public void close() throws IOException {
    if (isOpen) {
      flush();
      out.close();
      isOpen = false;
    }
  }

  private class BufferedFileOutputStream extends BufferedOutputStream {
    private long position; // start of buffer

    private class PositionFilter extends FilterOutputStream {
      public PositionFilter(OutputStream out) throws IOException {
        super(out);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
        position += len; // update on write
      }
    }

    public BufferedFileOutputStream(OutputStream out) throws IOException {
      super(null);
      this.out = new PositionFilter(out);
    }

    public long tell() {
      return position + count;
    }

    @Override
    public synchronized void flush() throws IOException {
      try {
        super.flush();
      } finally {
        // Ensure that count is reset in any case to avoid writing garbage to the end of
        // the file in case of an error
        // occurred during the write
        count = 0;
      }
    }
  }

}
