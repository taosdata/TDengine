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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.InvalidNumberEncodingException;
import org.apache.avro.util.Utf8;
import org.slf4j.LoggerFactory;

/**
 * An {@link Decoder} for binary-format data.
 * <p/>
 * Instances are created using {@link DecoderFactory}.
 * <p/>
 * This class may read-ahead and buffer bytes from the source beyond what is
 * required to serve its read methods. The number of unused bytes in the buffer
 * can be accessed by inputStream().remaining(), if the BinaryDecoder is not
 * 'direct'.
 * <p/>
 * To prevent this class from making large allocations when handling potentially
 * pathological input data, set Java properties
 * <tt>org.apache.avro.limits.string.maxLength</tt> and
 * <tt>org.apache.avro.limits.bytes.maxLength</tt> before instantiating this
 * class to limit the maximum sizes of <tt>string</tt> and <tt>bytes</tt> types
 * handled. The default is to permit sizes up to Java's maximum array length.
 *
 * @see Encoder
 */

public class BinaryDecoder extends Decoder {

  /**
   * The maximum size of array to allocate. Some VMs reserve some header words in
   * an array. Attempts to allocate larger arrays may result in OutOfMemoryError:
   * Requested array size exceeds VM limit
   */
  static final long MAX_ARRAY_SIZE = (long) Integer.MAX_VALUE - 8L;

  private static final String MAX_BYTES_LENGTH_PROPERTY = "org.apache.avro.limits.bytes.maxLength";
  private final int maxBytesLength;

  private ByteSource source = null;
  // we keep the buffer and its state variables in this class and not in a
  // container class for performance reasons. This improves performance
  // over a container object by about 5% to 15%
  // for example, we could have a FastBuffer class with these state variables
  // and keep a private FastBuffer member here. This simplifies the
  // "detach source" code and source access to the buffer, but
  // hurts performance.
  private byte[] buf = null;
  private int minPos = 0;
  private int pos = 0;
  private int limit = 0;

  byte[] getBuf() {
    return buf;
  }

  int getPos() {
    return pos;
  }

  int getLimit() {
    return limit;
  }

  void setBuf(byte[] buf, int pos, int len) {
    this.buf = buf;
    this.pos = pos;
    this.limit = pos + len;
  }

  void clearBuf() {
    this.buf = null;
  }

  /** protected constructor for child classes */
  protected BinaryDecoder() {
    super();
    String o = System.getProperty(MAX_BYTES_LENGTH_PROPERTY);
    int i = Integer.MAX_VALUE;
    if (o != null) {
      try {
        i = Integer.parseUnsignedInt(o);
      } catch (NumberFormatException nfe) {
        LoggerFactory.getLogger(BinaryDecoder.class)
            .warn("Could not parse property " + MAX_BYTES_LENGTH_PROPERTY + ": " + o, nfe);
      }
    }
    maxBytesLength = i;
  }

  BinaryDecoder(InputStream in, int bufferSize) {
    this();
    configure(in, bufferSize);
  }

  BinaryDecoder(byte[] data, int offset, int length) {
    this();
    configure(data, offset, length);
  }

  BinaryDecoder configure(InputStream in, int bufferSize) {
    configureSource(bufferSize, new InputStreamByteSource(in));
    return this;
  }

  BinaryDecoder configure(byte[] data, int offset, int length) {
    configureSource(DecoderFactory.DEFAULT_BUFFER_SIZE, new ByteArrayByteSource(data, offset, length));
    return this;
  }

  /**
   * Initializes this decoder with a new ByteSource. Detaches the old source (if
   * it exists) from this Decoder. The old source's state no longer depends on
   * this Decoder and its InputStream interface will continue to drain the
   * remaining buffer and source data.
   * <p/>
   * The decoder will read from the new source. The source will generally replace
   * the buffer with its own. If the source allocates a new buffer, it will create
   * it with size bufferSize.
   */
  private void configureSource(int bufferSize, ByteSource source) {
    if (null != this.source) {
      this.source.detach();
    }
    source.attach(bufferSize, this);
    this.source = source;
  }

  @Override
  public void readNull() throws IOException {
  }

  @Override
  public boolean readBoolean() throws IOException {
    // inlined, shorter version of ensureBounds
    if (limit == pos) {
      limit = source.tryReadRaw(buf, 0, buf.length);
      pos = 0;
      if (limit == 0) {
        throw new EOFException();
      }
    }
    int n = buf[pos++] & 0xff;
    return n == 1;
  }

  @Override
  public int readInt() throws IOException {
    ensureBounds(5); // won't throw index out of bounds
    int len = 1;
    int b = buf[pos] & 0xff;
    int n = b & 0x7f;
    if (b > 0x7f) {
      b = buf[pos + len++] & 0xff;
      n ^= (b & 0x7f) << 7;
      if (b > 0x7f) {
        b = buf[pos + len++] & 0xff;
        n ^= (b & 0x7f) << 14;
        if (b > 0x7f) {
          b = buf[pos + len++] & 0xff;
          n ^= (b & 0x7f) << 21;
          if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            n ^= (b & 0x7f) << 28;
            if (b > 0x7f) {
              throw new InvalidNumberEncodingException("Invalid int encoding");
            }
          }
        }
      }
    }
    pos += len;
    if (pos > limit) {
      throw new EOFException();
    }
    return (n >>> 1) ^ -(n & 1); // back to two's-complement
  }

  @Override
  public long readLong() throws IOException {
    ensureBounds(10);
    int b = buf[pos++] & 0xff;
    int n = b & 0x7f;
    long l;
    if (b > 0x7f) {
      b = buf[pos++] & 0xff;
      n ^= (b & 0x7f) << 7;
      if (b > 0x7f) {
        b = buf[pos++] & 0xff;
        n ^= (b & 0x7f) << 14;
        if (b > 0x7f) {
          b = buf[pos++] & 0xff;
          n ^= (b & 0x7f) << 21;
          if (b > 0x7f) {
            // only the low 28 bits can be set, so this won't carry
            // the sign bit to the long
            l = innerLongDecode((long) n);
          } else {
            l = n;
          }
        } else {
          l = n;
        }
      } else {
        l = n;
      }
    } else {
      l = n;
    }
    if (pos > limit) {
      throw new EOFException();
    }
    return (l >>> 1) ^ -(l & 1); // back to two's-complement
  }

  // splitting readLong up makes it faster because of the JVM does more
  // optimizations on small methods
  private long innerLongDecode(long l) throws IOException {
    int len = 1;
    int b = buf[pos] & 0xff;
    l ^= (b & 0x7fL) << 28;
    if (b > 0x7f) {
      b = buf[pos + len++] & 0xff;
      l ^= (b & 0x7fL) << 35;
      if (b > 0x7f) {
        b = buf[pos + len++] & 0xff;
        l ^= (b & 0x7fL) << 42;
        if (b > 0x7f) {
          b = buf[pos + len++] & 0xff;
          l ^= (b & 0x7fL) << 49;
          if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            l ^= (b & 0x7fL) << 56;
            if (b > 0x7f) {
              b = buf[pos + len++] & 0xff;
              l ^= (b & 0x7fL) << 63;
              if (b > 0x7f) {
                throw new InvalidNumberEncodingException("Invalid long encoding");
              }
            }
          }
        }
      }
    }
    pos += len;
    return l;
  }

  @Override
  public float readFloat() throws IOException {
    ensureBounds(4);
    int len = 1;
    int n = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
        | ((buf[pos + len++] & 0xff) << 24);
    if ((pos + 4) > limit) {
      throw new EOFException();
    }
    pos += 4;
    return Float.intBitsToFloat(n);
  }

  @Override
  public double readDouble() throws IOException {
    ensureBounds(8);
    int len = 1;
    int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
        | ((buf[pos + len++] & 0xff) << 24);
    int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8) | ((buf[pos + len++] & 0xff) << 16)
        | ((buf[pos + len++] & 0xff) << 24);
    if ((pos + 8) > limit) {
      throw new EOFException();
    }
    pos += 8;
    return Double.longBitsToDouble((((long) n1) & 0xffffffffL) | (((long) n2) << 32));
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    long length = readLong();
    if (length > MAX_ARRAY_SIZE) {
      throw new UnsupportedOperationException("Cannot read strings longer than " + MAX_ARRAY_SIZE + " bytes");
    }
    if (length < 0L) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
    }
    Utf8 result = (old != null ? old : new Utf8());
    result.setByteLength((int) length);
    if (0L != length) {
      doReadBytes(result.getBytes(), 0, (int) length);
    }
    return result;
  }

  private final Utf8 scratchUtf8 = new Utf8();

  @Override
  public String readString() throws IOException {
    return readString(scratchUtf8).toString();
  }

  @Override
  public void skipString() throws IOException {
    doSkipBytes(readLong());
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    int length = readInt();
    if (length > MAX_ARRAY_SIZE) {
      throw new UnsupportedOperationException("Cannot read arrays longer than " + MAX_ARRAY_SIZE + " bytes");
    }
    if (length > maxBytesLength) {
      throw new AvroRuntimeException("Bytes length " + length + " exceeds maximum allowed");
    }
    if (length < 0L) {
      throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
    }
    final ByteBuffer result;
    if (old != null && length <= old.capacity()) {
      result = old;
      ((Buffer) result).clear();
    } else {
      result = ByteBuffer.allocate(length);
    }
    doReadBytes(result.array(), result.position(), length);
    ((Buffer) result).limit(length);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    doSkipBytes(readLong());
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    doReadBytes(bytes, start, length);
  }

  @Override
  public void skipFixed(int length) throws IOException {
    doSkipBytes(length);
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  protected void doSkipBytes(long length) throws IOException {
    int remaining = limit - pos;
    if (length <= remaining) {
      pos = (int) (pos + length);
    } else {
      limit = pos = 0;
      length -= remaining;
      source.skipSourceBytes(length);
    }
  }

  /**
   * Reads <tt>length</tt> bytes into <tt>bytes</tt> starting at <tt>start</tt>.
   *
   * @throws EOFException If there are not enough number of bytes in the source.
   * @throws IOException
   */
  protected void doReadBytes(byte[] bytes, int start, int length) throws IOException {
    if (length < 0)
      throw new AvroRuntimeException("Malformed data. Length is negative: " + length);
    int remaining = limit - pos;
    if (length <= remaining) {
      System.arraycopy(buf, pos, bytes, start, length);
      pos += length;
    } else {
      // read the rest of the buffer
      System.arraycopy(buf, pos, bytes, start, remaining);
      start += remaining;
      length -= remaining;
      pos = limit;
      // finish from the byte source
      source.readRaw(bytes, start, length);
    }
  }

  /**
   * Returns the number of items to follow in the current array or map. Returns 0
   * if there are no more items in the current array and the array/map has ended.
   * Arrays are encoded as a series of blocks. Each block consists of a long count
   * value, followed by that many array items. A block with count zero indicates
   * the end of the array. If a block's count is negative, its absolute value is
   * used, and the count is followed immediately by a long block size indicating
   * the number of bytes in the block.
   *
   * @throws IOException If the first byte cannot be read for any reason other
   *                     than the end of the file, if the input stream has been
   *                     closed, or if some other I/O error occurs.
   */
  protected long doReadItemCount() throws IOException {
    long result = readLong();
    if (result < 0L) {
      // Consume byte-count if present
      readLong();
      result = -result;
    }
    return result;
  }

  /**
   * Reads the count of items in the current array or map and skip those items, if
   * possible. If it could skip the items, keep repeating until there are no more
   * items left in the array or map. Arrays are encoded as a series of blocks.
   * Each block consists of a long count value, followed by that many array items.
   * A block with count zero indicates the end of the array. If a block's count is
   * negative, its absolute value is used, and the count is followed immediately
   * by a long block size indicating the number of bytes in the block. If block
   * size is missing, this method return the count of the items found. The client
   * needs to skip the items individually.
   *
   * @return Zero if there are no more items to skip and end of array/map is
   *         reached. Positive number if some items are found that cannot be
   *         skipped and the client needs to skip them individually.
   *
   * @throws IOException If the first byte cannot be read for any reason other
   *                     than the end of the file, if the input stream has been
   *                     closed, or if some other I/O error occurs.
   */
  private long doSkipItems() throws IOException {
    long result = readLong();
    while (result < 0L) {
      final long bytecount = readLong();
      doSkipBytes(bytecount);
      result = readLong();
    }
    return result;
  }

  @Override
  public long readArrayStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long arrayNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipArray() throws IOException {
    return doSkipItems();
  }

  @Override
  public long readMapStart() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long mapNext() throws IOException {
    return doReadItemCount();
  }

  @Override
  public long skipMap() throws IOException {
    return doSkipItems();
  }

  @Override
  public int readIndex() throws IOException {
    return readInt();
  }

  /**
   * Returns true if the current BinaryDecoder is at the end of its source data
   * and cannot read any further without throwing an EOFException or other
   * IOException.
   * <p/>
   * Not all implementations of BinaryDecoder support isEnd(). Implementations
   * that do not support isEnd() will throw a
   * {@link java.lang.UnsupportedOperationException}.
   *
   * @throws IOException If the first byte cannot be read for any reason other
   *                     than the end of the file, if the input stream has been
   *                     closed, or if some other I/O error occurs.
   */
  public boolean isEnd() throws IOException {
    if (pos < limit) {
      return false;
    }
    if (source.isEof()) {
      return true;
    }

    // read from source.
    final int read = source.tryReadRaw(buf, 0, buf.length);
    pos = 0;
    limit = read;
    return (0 == read);
  }

  /**
   * Ensures that buf[pos + num - 1] is not out of the buffer array bounds.
   * However, buf[pos + num -1] may be >= limit if there is not enough data left
   * in the source to fill the array with num bytes.
   * <p/>
   * This method allows readers to read ahead by num bytes safely without checking
   * for EOF at each byte. However, readers must ensure that their reads are valid
   * by checking that their read did not advance past the limit before adjusting
   * pos.
   * <p/>
   * num must be less than the buffer size and greater than 0
   */
  private void ensureBounds(int num) throws IOException {
    int remaining = limit - pos;
    if (remaining < num) {
      // move remaining to front
      source.compactAndFill(buf, pos, minPos, remaining);
      if (pos >= limit)
        throw new EOFException();
    }
  }

  /**
   * Returns an {@link java.io.InputStream} that is aware of any buffering that
   * may occur in this BinaryDecoder. Readers that need to interleave decoding
   * Avro data with other reads must access this InputStream to do so unless the
   * implementation is 'direct' and does not read beyond the minimum bytes
   * necessary from the source.
   */
  public InputStream inputStream() {
    return source;
  }

  /**
   * BufferAccessor is used by BinaryEncoder to enable {@link ByteSource}s and the
   * InputStream returned by {@link BinaryDecoder.inputStream} to access the
   * BinaryEncoder's buffer. When a BufferAccessor is created, it is attached to a
   * BinaryDecoder and its buffer. Its accessors directly reference the
   * BinaryDecoder's buffer. When detach() is called, it stores references to the
   * BinaryDecoder's buffer directly. The BinaryDecoder only detaches a
   * BufferAccessor when it is initializing to a new ByteSource. Therefore, a
   * client that is using the InputStream returned by BinaryDecoder.inputStream
   * can continue to use that stream after a BinaryDecoder has been reinitialized
   * to read from new data.
   */
  static class BufferAccessor {
    private final BinaryDecoder decoder;
    private byte[] buf;
    private int pos;
    private int limit;
    boolean detached = false;

    private BufferAccessor(BinaryDecoder decoder) {
      this.decoder = decoder;
    }

    void detach() {
      this.buf = decoder.buf;
      this.pos = decoder.pos;
      this.limit = decoder.limit;
      detached = true;
    }

    int getPos() {
      if (detached)
        return this.pos;
      else
        return decoder.pos;
    }

    int getLim() {
      if (detached)
        return this.limit;
      else
        return decoder.limit;
    }

    byte[] getBuf() {
      if (detached)
        return this.buf;
      else
        return decoder.buf;
    }

    void setPos(int pos) {
      if (detached)
        this.pos = pos;
      else
        decoder.pos = pos;
    }

    void setLimit(int limit) {
      if (detached)
        this.limit = limit;
      else
        decoder.limit = limit;
    }

    void setBuf(byte[] buf, int offset, int length) {
      if (detached) {
        this.buf = buf;
        this.limit = offset + length;
        this.pos = offset;
      } else {
        decoder.buf = buf;
        decoder.limit = offset + length;
        decoder.pos = offset;
        decoder.minPos = offset;
      }
    }
  }

  /**
   * ByteSource abstracts the source of data from the core workings of
   * BinaryDecoder. This is very important for performance reasons because
   * InputStream's API is a barrier to performance due to several quirks:
   * InputStream does not in general require that as many bytes as possible have
   * been read when filling a buffer.
   * <p/>
   * InputStream's terminating conditions for a read are two-fold: EOFException
   * and '-1' on the return from read(). Implementations are supposed to return
   * '-1' on EOF but often do not. The extra terminating conditions cause extra
   * conditionals on both sides of the API, and slow performance significantly.
   * <p/>
   * ByteSource implementations provide read() and skip() variants that have
   * stronger guarantees than InputStream, freeing client code to be simplified
   * and faster.
   * <p/>
   * {@link skipSourceBytes} and {@link readRaw} are guaranteed to have read or
   * skipped as many bytes as possible, or throw EOFException.
   * {@link trySkipBytes} and {@link tryRead} are guaranteed to attempt to read or
   * skip as many bytes as possible and never throw EOFException, while returning
   * the exact number of bytes skipped or read. {@link isEof} returns true if all
   * the source bytes have been read or skipped. This condition can also be
   * detected by a client if an EOFException is thrown from
   * {@link skipSourceBytes} or {@link readRaw}, or if {@link trySkipBytes} or
   * {@link tryRead} return 0;
   * <p/>
   * A ByteSource also implements the InputStream contract for use by APIs that
   * require it. The InputStream interface must take into account buffering in any
   * decoder that this ByteSource is attached to. The other methods do not account
   * for buffering.
   */

  abstract static class ByteSource extends InputStream {
    // maintain a reference to the buffer, so that if this
    // source is detached from the Decoder, and a client still
    // has a reference to it via inputStream(), bytes are not
    // lost
    protected BufferAccessor ba;

    protected ByteSource() {
    }

    abstract boolean isEof();

    protected void attach(int bufferSize, BinaryDecoder decoder) {
      decoder.buf = new byte[bufferSize];
      decoder.pos = 0;
      decoder.minPos = 0;
      decoder.limit = 0;
      this.ba = new BufferAccessor(decoder);
    }

    protected void detach() {
      ba.detach();
    }

    /**
     * Skips length bytes from the source. If length bytes cannot be skipped due to
     * end of file/stream/channel/etc an EOFException is thrown
     *
     * @param length the number of bytes to attempt to skip
     * @throws IOException  if an error occurs
     * @throws EOFException if length bytes cannot be skipped
     */
    protected abstract void skipSourceBytes(long length) throws IOException;

    /**
     * Attempts to skip <i>skipLength</i> bytes from the source. Returns the actual
     * number of bytes skipped. This method must attempt to skip as many bytes as
     * possible up to <i>skipLength</i> bytes. Skipping 0 bytes signals end of
     * stream/channel/file/etc
     *
     * @param skipLength the number of bytes to attempt to skip
     * @return the count of actual bytes skipped.
     */
    protected abstract long trySkipBytes(long skipLength) throws IOException;

    /**
     * Reads raw from the source, into a byte[]. Used for reads that are larger than
     * the buffer, or otherwise unbuffered. This is a mandatory read -- if there is
     * not enough bytes in the source, EOFException is thrown.
     *
     * @throws IOException  if an error occurs
     * @throws EOFException if len bytes cannot be read
     */
    protected abstract void readRaw(byte[] data, int off, int len) throws IOException;

    /**
     * Attempts to copy up to <i>len</i> bytes from the source into data, starting
     * at index <i>off</i>. Returns the actual number of bytes copied which may be
     * between 0 and <i>len</i>.
     * <p/>
     * This method must attempt to read as much as possible from the source. Returns
     * 0 when at the end of stream/channel/file/etc.
     *
     * @throws IOException if an error occurs reading
     **/
    protected abstract int tryReadRaw(byte[] data, int off, int len) throws IOException;

    /**
     * If this source buffers, compacts the buffer by placing the <i>remaining</i>
     * bytes starting at <i>pos</i> at <i>minPos</i>. This may be done in the
     * current buffer, or may replace the buffer with a new one.
     *
     * The end result must be a buffer with at least 16 bytes of remaining space.
     *
     * @param pos
     * @param minPos
     * @param remaining
     * @throws IOException
     */
    protected void compactAndFill(byte[] buf, int pos, int minPos, int remaining) throws IOException {
      System.arraycopy(buf, pos, buf, minPos, remaining);
      ba.setPos(minPos);
      int newLimit = remaining + tryReadRaw(buf, minPos + remaining, buf.length - remaining);
      ba.setLimit(newLimit);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int lim = ba.getLim();
      int pos = ba.getPos();
      byte[] buf = ba.getBuf();
      int remaining = (lim - pos);
      if (remaining >= len) {
        System.arraycopy(buf, pos, b, off, len);
        pos = pos + len;
        ba.setPos(pos);
        return len;
      } else {
        // flush buffer to array
        System.arraycopy(buf, pos, b, off, remaining);
        pos = pos + remaining;
        ba.setPos(pos);
        // get the rest from the stream (skip array)
        int inputRead = remaining + tryReadRaw(b, off + remaining, len - remaining);
        if (inputRead == 0) {
          return -1;
        } else {
          return inputRead;
        }
      }
    }

    @Override
    public long skip(long n) throws IOException {
      int lim = ba.getLim();
      int pos = ba.getPos();
      int remaining = lim - pos;
      if (remaining > n) {
        pos = (int) (pos + n);
        ba.setPos(pos);
        return n;
      } else {
        pos = lim;
        ba.setPos(pos);
        long isSkipCount = trySkipBytes(n - remaining);
        return isSkipCount + remaining;
      }
    }

    /**
     * returns the number of bytes remaining that this BinaryDecoder has buffered
     * from its source
     */
    @Override
    public int available() throws IOException {
      return (ba.getLim() - ba.getPos());
    }
  }

  private static class InputStreamByteSource extends ByteSource {
    private InputStream in;
    protected boolean isEof = false;

    private InputStreamByteSource(InputStream in) {
      super();
      this.in = in;
    }

    @Override
    protected void skipSourceBytes(long length) throws IOException {
      boolean readZero = false;
      while (length > 0) {
        long n = in.skip(length);
        if (n > 0) {
          length -= n;
          continue;
        }
        // The inputStream contract is evil.
        // zero "might" mean EOF. So check for 2 in a row, we will
        // infinite loop waiting for -1 with some classes others
        // spuriously will return 0 on occasion without EOF
        if (n == 0) {
          if (readZero) {
            isEof = true;
            throw new EOFException();
          }
          readZero = true;
          continue;
        }
        // read negative
        isEof = true;
        throw new EOFException();
      }
    }

    @Override
    protected long trySkipBytes(long length) throws IOException {
      long leftToSkip = length;
      try {
        boolean readZero = false;
        while (leftToSkip > 0) {
          long n = in.skip(length);
          if (n > 0) {
            leftToSkip -= n;
            continue;
          }
          // The inputStream contract is evil.
          // zero "might" mean EOF. So check for 2 in a row, we will
          // infinite loop waiting for -1 with some classes others
          // spuriously will return 0 on occasion without EOF
          if (n == 0) {
            if (readZero) {
              isEof = true;
              break;
            }
            readZero = true;
            continue;
          }
          // read negative
          isEof = true;
          break;

        }
      } catch (EOFException eof) {
        isEof = true;
      }
      return length - leftToSkip;
    }

    @Override
    protected void readRaw(byte[] data, int off, int len) throws IOException {
      while (len > 0) {
        int read = in.read(data, off, len);
        if (read < 0) {
          isEof = true;
          throw new EOFException();
        }
        len -= read;
        off += read;
      }
    }

    @Override
    protected int tryReadRaw(byte[] data, int off, int len) throws IOException {
      int leftToCopy = len;
      try {
        while (leftToCopy > 0) {
          int read = in.read(data, off, leftToCopy);
          if (read < 0) {
            isEof = true;
            break;
          }
          leftToCopy -= read;
          off += read;
        }
      } catch (EOFException eof) {
        isEof = true;
      }
      return len - leftToCopy;
    }

    @Override
    public int read() throws IOException {
      if (ba.getLim() - ba.getPos() == 0) {
        return in.read();
      } else {
        int position = ba.getPos();
        int result = ba.getBuf()[position] & 0xff;
        ba.setPos(position + 1);
        return result;
      }
    }

    @Override
    public boolean isEof() {
      return isEof;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /**
   * This byte source is special. It will avoid copying data by using the source's
   * byte[] as a buffer in the decoder.
   *
   */
  private static class ByteArrayByteSource extends ByteSource {
    private static final int MIN_SIZE = 16;
    private byte[] data;
    private int position;
    private int max;
    private boolean compacted = false;

    private ByteArrayByteSource(byte[] data, int start, int len) {
      super();
      // make sure data is not too small, otherwise getLong may try and
      // read 10 bytes and get index out of bounds.
      if (len < MIN_SIZE) {
        this.data = Arrays.copyOfRange(data, start, start + MIN_SIZE);
        this.position = 0;
        this.max = len;
      } else {
        // use the array passed in
        this.data = data;
        this.position = start;
        this.max = start + len;
      }
    }

    @Override
    protected void attach(int bufferSize, BinaryDecoder decoder) {
      // buffer size is not used here, the byte[] source is the buffer.
      decoder.buf = this.data;
      decoder.pos = this.position;
      decoder.minPos = this.position;
      decoder.limit = this.max;
      this.ba = new BufferAccessor(decoder);
    }

    @Override
    protected void skipSourceBytes(long length) throws IOException {
      long skipped = trySkipBytes(length);
      if (skipped < length) {
        throw new EOFException();
      }
    }

    @Override
    protected long trySkipBytes(long length) throws IOException {
      // the buffer is shared, so this should return 0
      max = ba.getLim();
      position = ba.getPos();
      long remaining = (long) max - position;
      if (remaining >= length) {
        position = (int) (position + length);
        ba.setPos(position);
        return length;
      } else {
        position += remaining;
        ba.setPos(position);
        return remaining;
      }
    }

    @Override
    protected void readRaw(byte[] data, int off, int len) throws IOException {
      int read = tryReadRaw(data, off, len);
      if (read < len) {
        throw new EOFException();
      }
    }

    @Override
    protected int tryReadRaw(byte[] data, int off, int len) throws IOException {
      // the buffer is shared, nothing to read
      return 0;
    }

    @Override
    protected void compactAndFill(byte[] buf, int pos, int minPos, int remaining) throws IOException {
      // this implementation does not want to mutate the array passed in,
      // so it makes a new tiny buffer unless it has been compacted once before
      if (!compacted) {
        // assumes ensureCapacity is never called with a size more than 16
        byte[] tinybuf = new byte[remaining + 16];
        System.arraycopy(buf, pos, tinybuf, 0, remaining);
        ba.setBuf(tinybuf, 0, remaining);
        compacted = true;
      }
    }

    @Override
    public int read() throws IOException {
      max = ba.getLim();
      position = ba.getPos();
      if (position >= max) {
        return -1;
      } else {
        int result = ba.getBuf()[position++] & 0xff;
        ba.setPos(position);
        return result;
      }
    }

    @Override
    public void close() throws IOException {
      ba.setPos(ba.getLim()); // effectively set isEof to false
    }

    @Override
    public boolean isEof() {
      int remaining = ba.getLim() - ba.getPos();
      return (remaining == 0);
    }
  }
}
