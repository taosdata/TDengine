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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;

/**
 * A {@link BinaryEncoder} implementation that writes large arrays and maps as a
 * sequence of blocks. So long as individual primitive values fit in memory,
 * arbitrarily long arrays and maps may be written and subsequently read without
 * exhausting memory. Values are buffered until the specified block size would
 * be exceeded, minimizing block overhead.
 * <p/>
 * Use {@link EncoderFactory#blockingBinaryEncoder(OutputStream, BinaryEncoder)}
 * to construct and configure.
 * <p/>
 * BlockingBinaryEncoder buffers writes, data may not appear on the output until
 * {@link #flush()} is called.
 * <p/>
 * BlockingBinaryEncoder is not thread-safe
 *
 * @see BinaryEncoder
 * @see EncoderFactory
 * @see Encoder
 */
public class BlockingBinaryEncoder extends BufferedBinaryEncoder {

  /*
   * Implementation note:
   *
   * Blocking is complicated because of nesting. If a large, nested value
   * overflows your buffer, you've got to do a lot of dancing around to output the
   * blocks correctly.
   *
   * To handle this complexity, this class keeps a stack of blocked values: each
   * time a new block is started (e.g., by a call to {@link #writeArrayStart}), an
   * entry is pushed onto this stack.
   *
   * In this stack, we keep track of the state of a block. Blocks can be in two
   * states. "Regular" blocks have a non-zero byte count. "Overflow" blocks help
   * us deal with the case where a block contains a value that's too big to
   * buffer. In this case, the block contains only one item, and we give it an
   * unknown byte-count. Because these values (1,unknown) are fixed, we're able to
   * write the header for these overflow blocks to the underlying stream without
   * seeing the entire block. After writing this header, we've freed our buffer
   * space to be fully devoted to blocking the large, inner value.
   */

  private static class BlockedValue {
    public enum State {
      /**
       * The bottom element of our stack represents being _outside_ of a blocked
       * value.
       */
      ROOT,

      /**
       * Represents the "regular" case, i.e., a blocked-value whose current block is
       * fully contained in the buffer. In this case, {@link BlockedValue#start}
       * points to the start of the blocks _data_ -- but no room has been left for a
       * header! When this block is terminated, it's data will have to be moved over a
       * bit to make room for the header.
       */
      REGULAR,

      /**
       * Represents a blocked-value whose current block is in the overflow state. In
       * this case, {@link BlockedValue#start} is zero. The header for such a block
       * has _already been written_ (we've written out a header indicating that the
       * block has a single item, and we put a "zero" down for the byte-count to
       * indicate that we don't know the physical length of the buffer. Any blocks
       * _containing_ this block must be in the {@link #OVERFLOW} state.
       */
      OVERFLOW
    }

    /** The type of this blocked value (ARRAY or MAP). */
    public Schema.Type type;

    /** The state of this BlockedValue */
    public State state;

    /** The location in the buffer where this blocked value starts */
    public int start;

    /**
     * The index one past the last byte for the previous item. If this is the first
     * item, this is same as {@link #start}.
     */
    public int lastFullItem;

    /**
     * Number of items in this blocked value that are stored in the buffer.
     */
    public int items;

    /** Number of items left to write */
    public long itemsLeftToWrite;

    /** Create a ROOT instance. */
    public BlockedValue() {
      this.type = null;
      this.state = BlockedValue.State.ROOT;
      this.start = this.lastFullItem = 0;
      this.items = 1; // Makes various assertions work out
    }

    /**
     * Check invariants of <code>this</code> and also the <code>BlockedValue</code>
     * containing <code>this</code>.
     */
    public boolean check(BlockedValue prev, int pos) {
      assert state != State.ROOT || type == null;
      assert (state == State.ROOT || type == Schema.Type.ARRAY || type == Schema.Type.MAP);

      assert 0 <= items;
      assert 0 != items || start == pos; // 0==items ==> start==pos
      assert 1 < items || start == lastFullItem; // 1<=items ==> start==lFI
      assert items <= 1 || start <= lastFullItem; // 1<items ==> start<=lFI
      assert lastFullItem <= pos;

      switch (state) {
      case ROOT:
        assert start == 0;
        assert prev == null;
        break;
      case REGULAR:
        assert start >= 0;
        assert prev.lastFullItem <= start;
        assert 1 <= prev.items;
        break;
      case OVERFLOW:
        assert start == 0;
        assert items == 1;
        assert prev.state == State.ROOT || prev.state == State.OVERFLOW;
        break;
      }
      return false;
    }
  }

  /**
   * The buffer to hold the bytes before being written into the underlying stream.
   */
  private byte[] buf;

  /**
   * Index into the location in {@link #buf}, where next byte can be written.
   */
  private int pos;

  /**
   * The state stack.
   */
  private BlockedValue[] blockStack;
  private int stackTop = -1;
  private static final int STACK_STEP = 10;

  // buffer large enough for up to two ints for a block header
  // rounded up to a multiple of 4 bytes.
  private byte[] headerBuffer = new byte[12];

  private boolean check() {
    assert buf != null;
    assert 0 <= pos;
    assert pos <= buf.length : pos + " " + buf.length;

    assert blockStack != null;
    BlockedValue prev = null;
    for (int i = 0; i <= stackTop; i++) {
      BlockedValue v = blockStack[i];
      v.check(prev, pos);
      prev = v;
    }
    return true;
  }

  BlockingBinaryEncoder(OutputStream out, int blockBufferSize, int binaryEncoderBufferSize) {
    super(out, binaryEncoderBufferSize);
    this.buf = new byte[blockBufferSize];
    this.pos = 0;
    blockStack = new BlockedValue[0];
    expandStack();
    BlockedValue bv = blockStack[++stackTop];
    bv.type = null;
    bv.state = BlockedValue.State.ROOT;
    bv.start = bv.lastFullItem = 0;
    bv.items = 1;

    assert check();
  }

  private void expandStack() {
    int oldLength = blockStack.length;
    blockStack = Arrays.copyOf(blockStack, blockStack.length + STACK_STEP);
    for (int i = oldLength; i < blockStack.length; i++) {
      blockStack[i] = new BlockedValue();
    }
  }

  BlockingBinaryEncoder configure(OutputStream out, int blockBufferSize, int binaryEncoderBufferSize) {
    super.configure(out, binaryEncoderBufferSize);
    pos = 0;
    stackTop = 0;
    if (null == buf || buf.length != blockBufferSize) {
      buf = new byte[blockBufferSize];
    }

    assert check();
    return this;
  }

  @Override
  public void flush() throws IOException {
    BlockedValue bv = blockStack[stackTop];
    if (bv.state == BlockedValue.State.ROOT) {
      super.writeFixed(buf, 0, pos);
      pos = 0;
    } else {
      while (bv.state != BlockedValue.State.OVERFLOW) {
        compact();
      }
    }
    super.flush();

    assert check();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    ensureBounds(1);
    pos += BinaryData.encodeBoolean(b, buf, pos);
  }

  @Override
  public void writeInt(int n) throws IOException {
    ensureBounds(5);
    pos += BinaryData.encodeInt(n, buf, pos);
  }

  @Override
  public void writeLong(long n) throws IOException {
    ensureBounds(10);
    pos += BinaryData.encodeLong(n, buf, pos);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    ensureBounds(4);
    pos += BinaryData.encodeFloat(f, buf, pos);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    ensureBounds(8);
    pos += BinaryData.encodeDouble(d, buf, pos);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    doWriteBytes(bytes, start, len);
  }

  @Override
  public void writeFixed(ByteBuffer bytes) throws IOException {
    int pos = bytes.position();
    int len = bytes.remaining();
    if (bytes.hasArray()) {
      doWriteBytes(bytes.array(), bytes.arrayOffset() + pos, len);
    } else {
      byte[] b = new byte[len];
      bytes.duplicate().get(b, 0, len);
      doWriteBytes(b, 0, len);
    }
  }

  @Override
  protected void writeZero() throws IOException {
    ensureBounds(1);
    buf[pos++] = (byte) 0;
  }

  @Override
  public void writeArrayStart() throws IOException {
    if (stackTop + 1 == blockStack.length) {
      expandStack();
    }

    BlockedValue bv = blockStack[++stackTop];
    bv.type = Schema.Type.ARRAY;
    bv.state = BlockedValue.State.REGULAR;
    bv.start = bv.lastFullItem = pos;
    bv.items = 0;

    assert check();
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    BlockedValue v = blockStack[stackTop];
    assert v.type == Schema.Type.ARRAY || v.type == Schema.Type.MAP;
    assert v.itemsLeftToWrite == 0;
    v.itemsLeftToWrite = itemCount;

    assert check();
  }

  @Override
  public void startItem() throws IOException {
    if (blockStack[stackTop].state == BlockedValue.State.OVERFLOW) {
      finishOverflow();
    }
    BlockedValue t = blockStack[stackTop];
    t.items++;
    t.lastFullItem = pos;
    t.itemsLeftToWrite--;

    assert check();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    BlockedValue top = blockStack[stackTop];
    if (top.type != Schema.Type.ARRAY) {
      throw new AvroTypeException("Called writeArrayEnd outside of an array.");
    }
    if (top.itemsLeftToWrite != 0) {
      throw new AvroTypeException("Failed to write expected number of array elements.");
    }
    endBlockedValue();

    assert check();
  }

  @Override
  public void writeMapStart() throws IOException {
    if (stackTop + 1 == blockStack.length) {
      expandStack();
    }

    BlockedValue bv = blockStack[++stackTop];
    bv.type = Schema.Type.MAP;
    bv.state = BlockedValue.State.REGULAR;
    bv.start = bv.lastFullItem = pos;
    bv.items = 0;

    assert check();
  }

  @Override
  public void writeMapEnd() throws IOException {
    BlockedValue top = blockStack[stackTop];
    if (top.type != Schema.Type.MAP) {
      throw new AvroTypeException("Called writeMapEnd outside of a map.");
    }
    if (top.itemsLeftToWrite != 0) {
      throw new AvroTypeException("Failed to read write expected number of array elements.");
    }
    endBlockedValue();

    assert check();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    ensureBounds(5);
    pos += BinaryData.encodeInt(unionIndex, buf, pos);
  }

  @Override
  public int bytesBuffered() {
    return pos + super.bytesBuffered();
  }

  private void endBlockedValue() throws IOException {
    for (;;) {
      assert check();
      BlockedValue t = blockStack[stackTop];
      assert t.state != BlockedValue.State.ROOT;
      if (t.state == BlockedValue.State.OVERFLOW) {
        finishOverflow();
      }
      assert t.state == BlockedValue.State.REGULAR;
      if (0 < t.items) {
        int byteCount = pos - t.start;
        if (t.start == 0 && blockStack[stackTop - 1].state != BlockedValue.State.REGULAR) { // Lucky us -- don't have to
                                                                                            // move
          super.writeInt(-t.items);
          super.writeInt(byteCount);
        } else {
          int headerSize = 0;
          headerSize += BinaryData.encodeInt(-t.items, headerBuffer, headerSize);
          headerSize += BinaryData.encodeInt(byteCount, headerBuffer, headerSize);
          if (buf.length >= pos + headerSize) {
            pos += headerSize;
            final int m = t.start;
            System.arraycopy(buf, m, buf, m + headerSize, byteCount);
            System.arraycopy(headerBuffer, 0, buf, m, headerSize);
          } else {
            compact();
            continue;
          }
        }
      }
      stackTop--;
      ensureBounds(1);
      buf[pos++] = 0; // Sentinel for last block in a blocked value
      assert check();
      if (blockStack[stackTop].state == BlockedValue.State.ROOT) {
        flush();
      }
      return;
    }
  }

  /**
   * Called when we've finished writing the last item in an overflow buffer. When
   * this is finished, the top of the stack will be an empty block in the
   * "regular" state.
   * 
   * @throws IOException
   */
  private void finishOverflow() throws IOException {
    BlockedValue s = blockStack[stackTop];
    if (s.state != BlockedValue.State.OVERFLOW) {
      throw new IllegalStateException("Not an overflow block");
    }
    assert check();

    // Flush any remaining data for this block
    super.writeFixed(buf, 0, pos);
    pos = 0;

    // Reset top of stack to be in REGULAR mode
    s.state = BlockedValue.State.REGULAR;
    s.start = s.lastFullItem = 0;
    s.items = 0;
    assert check();
  }

  private void ensureBounds(int l) throws IOException {
    while (buf.length < (pos + l)) {
      if (blockStack[stackTop].state == BlockedValue.State.REGULAR) {
        compact();
      } else {
        super.writeFixed(buf, 0, pos);
        pos = 0;
      }
    }
  }

  private void doWriteBytes(byte[] bytes, int start, int len) throws IOException {
    if (len < buf.length) {
      ensureBounds(len);
      System.arraycopy(bytes, start, buf, pos, len);
      pos += len;
    } else {
      ensureBounds(buf.length);
      assert blockStack[stackTop].state == BlockedValue.State.ROOT
          || blockStack[stackTop].state == BlockedValue.State.OVERFLOW;
      write(bytes, start, len);
    }
  }

  private void write(byte[] b, int off, int len) throws IOException {
    if (blockStack[stackTop].state == BlockedValue.State.ROOT) {
      super.writeFixed(b, off, len);
    } else {
      assert check();
      while (buf.length < (pos + len)) {
        if (blockStack[stackTop].state == BlockedValue.State.REGULAR) {
          compact();
        } else {
          super.writeFixed(buf, 0, pos);
          pos = 0;
          if (buf.length <= len) {
            super.writeFixed(b, off, len);
            len = 0;
          }
        }
      }
      System.arraycopy(b, off, buf, pos, len);
      pos += len;
    }
    assert check();
  }

  /** Only call if you're there are REGULAR-state values on the stack. */
  private void compact() throws IOException {
    assert check();

    // Find first REGULAR-state value
    BlockedValue s = null;
    int i;
    for (i = 1; i <= stackTop; i++) {
      s = blockStack[i];
      if (s.state == BlockedValue.State.REGULAR)
        break;
    }
    assert s != null;

    // We're going to transition "s" into the overflow state. To do
    // this, We're going to flush any bytes prior to "s", then write
    // any full items of "s" into a block, start an overflow
    // block, write any remaining bytes of "s" up to the start of the
    // next more deeply-nested blocked-value, and finally move over
    // any remaining bytes (which will be from more deeply-nested
    // blocked values).

    // Flush any bytes prios to "s"
    super.writeFixed(buf, 0, s.start);

    // Write any full items of "s"
    if (1 < s.items) {
      super.writeInt(-(s.items - 1));
      super.writeInt(s.lastFullItem - s.start);
      super.writeFixed(buf, s.start, s.lastFullItem - s.start);
      s.start = s.lastFullItem;
      s.items = 1;
    }

    // Start an overflow block for s
    super.writeInt(1);

    // Write any remaining bytes for "s", up to the next-most
    // deeply-nested value
    BlockedValue n = ((i + 1) <= stackTop ? blockStack[i + 1] : null);
    int end = (n == null ? pos : n.start);
    super.writeFixed(buf, s.lastFullItem, end - s.lastFullItem);

    // Move over any bytes that remain (and adjust indices)
    System.arraycopy(buf, end, buf, 0, pos - end);
    for (int j = i + 1; j <= stackTop; j++) {
      n = blockStack[j];
      n.start -= end;
      n.lastFullItem -= end;
    }
    pos -= end;

    assert s.items == 1;
    s.start = s.lastFullItem = 0;
    s.state = BlockedValue.State.OVERFLOW;

    assert check();
  }

}
