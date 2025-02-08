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
import java.io.File;
import java.util.Arrays;

import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.avro.io.DatumReader;
import static org.apache.avro.file.DataFileConstants.SYNC_SIZE;
import static org.apache.avro.file.DataFileConstants.MAGIC;

/**
 * Random access to files written with {@link DataFileWriter}.
 *
 * @see DataFileWriter
 */
public class DataFileReader<D> extends DataFileStream<D> implements FileReader<D> {
  private SeekableInputStream sin;
  private long blockStart;
  private int[] partialMatchTable;

  /** Open a reader for a file. */
  public static <D> FileReader<D> openReader(File file, DatumReader<D> reader) throws IOException {
    SeekableFileInput input = new SeekableFileInput(file);
    try {
      return openReader(input, reader);
    } catch (final Throwable e) {
      IOUtils.closeQuietly(input);
      throw e;
    }
  }

  /** Open a reader for a file. */
  public static <D> FileReader<D> openReader(SeekableInput in, DatumReader<D> reader) throws IOException {
    if (in.length() < MAGIC.length)
      throw new InvalidAvroMagicException("Not an Avro data file");

    // read magic header
    byte[] magic = new byte[MAGIC.length];
    in.seek(0);
    int offset = 0;
    int length = magic.length;
    while (length > 0) {
      int bytesRead = in.read(magic, offset, length);
      if (bytesRead < 0)
        throw new EOFException("Unexpected EOF with " + length + " bytes remaining to read");

      length -= bytesRead;
      offset += bytesRead;
    }
    in.seek(0);

    if (Arrays.equals(MAGIC, magic)) // current format
      return new DataFileReader<>(in, reader);
    if (Arrays.equals(DataFileReader12.MAGIC, magic)) // 1.2 format
      return new DataFileReader12<>(in, reader);

    throw new InvalidAvroMagicException("Not an Avro data file");
  }

  /**
   * Construct a reader for a file at the current position of the input, without
   * reading the header.
   *
   * @param sync True to read forward to the next sync point after opening, false
   *             to assume that the input is already at a valid sync point.
   */
  public static <D> DataFileReader<D> openReader(SeekableInput in, DatumReader<D> reader, Header header, boolean sync)
      throws IOException {
    DataFileReader<D> dreader = new DataFileReader<>(in, reader, header);
    // seek/sync to an (assumed) valid position
    if (sync)
      dreader.sync(in.tell());
    else
      dreader.seek(in.tell());
    return dreader;
  }

  /**
   * Construct a reader for a file. For example,if you want to read a file
   * record,you need to close the resource. You can use try-with-resource as
   * follows:
   *
   * <pre>
   * try (FileReader<User> dataFileReader =
   * DataFileReader.openReader(file,datumReader)) { //Consume the reader } catch
   * (IOException e) { throw new RunTimeIOException(e,"Failed to read metadata for
   * file: %s", file); }
   *
   * <pre/>
   */
  public DataFileReader(File file, DatumReader<D> reader) throws IOException {
    this(new SeekableFileInput(file), reader, true);
  }

  /**
   * Construct a reader for a file. For example,if you want to read a file
   * record,you need to close the resource. You can use try-with-resource as
   * follows:
   *
   * <pre>
   * try (FileReader<User> dataFileReader =
   * DataFileReader.openReader(file,datumReader)) { //Consume the reader } catch
   * (IOException e) { throw new RunTimeIOException(e,"Failed to read metadata for
   * file: %s", file); }
   *
   * <pre/>
   */
  public DataFileReader(SeekableInput sin, DatumReader<D> reader) throws IOException {
    this(sin, reader, false);
  }

  /** Construct a reader for a file. Please close resource files yourself. */
  protected DataFileReader(SeekableInput sin, DatumReader<D> reader, boolean closeOnError) throws IOException {
    super(reader);
    try {
      this.sin = new SeekableInputStream(sin);
      initialize(this.sin);
      blockFinished();
    } catch (final Throwable e) {
      if (closeOnError) {
        IOUtils.closeQuietly(sin);
      }
      throw e;
    }
  }

  /**
   * Construct using a {@link DataFileStream.Header}. Does not call
   * {@link #sync(long)} or {@link #seek(long)}.
   */
  protected DataFileReader(SeekableInput sin, DatumReader<D> reader, Header header) throws IOException {
    super(reader);
    this.sin = new SeekableInputStream(sin);
    initialize(this.sin, header);
  }

  /**
   * Move to a specific, known synchronization point, one returned from
   * {@link DataFileWriter#sync()} while writing. If synchronization points were
   * not saved while writing a file, use {@link #sync(long)} instead.
   */
  public void seek(long position) throws IOException {
    sin.seek(position);
    vin = DecoderFactory.get().binaryDecoder(this.sin, vin);
    datumIn = null;
    blockRemaining = 0;
    blockStart = position;
  }

  /**
   * Move to the next synchronization point after a position. To process a range
   * of file entires, call this with the starting position, then check
   * {@link #pastSync(long)} with the end point before each call to
   * {@link #next()}.
   */
  @Override
  public void sync(final long position) throws IOException {
    seek(position);
    // work around an issue where 1.5.4 C stored sync in metadata
    if ((position == 0L) && (getMeta("avro.sync") != null)) {
      initialize(sin); // re-init to skip header
      return;
    }

    if (this.partialMatchTable == null) {
      this.partialMatchTable = computePartialMatchTable(getHeader().sync);
    }

    final byte[] sync = getHeader().sync;
    final InputStream in = vin.inputStream();
    final int[] pm = this.partialMatchTable;

    // Search for the sequence of bytes in the stream using Knuth-Morris-Pratt
    long i = 0L;
    for (int b = in.read(), j = 0; b != -1; b = in.read(), i++) {
      final byte cb = (byte) b;
      while (j > 0 && sync[j] != cb) {
        j = pm[j - 1];
      }
      if (sync[j] == cb) {
        j++;
      }
      if (j == SYNC_SIZE) {
        this.blockStart = position + i + 1L;
        return;
      }
    }
    // if no match set start to the end position
    blockStart = sin.tell();
  }

  /**
   * Compute that Knuth-Morris-Pratt partial match table.
   *
   * @param pattern The pattern being searched
   * @return the pre-computed partial match table
   *
   * @see <a href= "https://github.com/williamfiset/Algorithms">William Fiset
   *      Algorithms</a>
   */
  private int[] computePartialMatchTable(final byte[] pattern) {
    final int[] pm = new int[pattern.length];
    for (int i = 1, len = 0; i < pattern.length;) {
      if (pattern[i] == pattern[len]) {
        pm[i++] = ++len;
      } else {
        if (len > 0) {
          len = pm[len - 1];
        } else {
          i++;
        }
      }
    }
    return pm;
  }

  @Override
  protected void blockFinished() throws IOException {
    blockStart = sin.tell() - vin.inputStream().available();
  }

  /** Return the last synchronization point before our current position. */
  public long previousSync() {
    return blockStart;
  }

  /** Return true if past the next synchronization point after a position. */
  @Override
  public boolean pastSync(long position) throws IOException {
    return ((blockStart >= position + SYNC_SIZE) || (blockStart >= sin.length()));
  }

  @Override
  public long tell() throws IOException {
    return sin.tell();
  }

  static class SeekableInputStream extends InputStream implements SeekableInput {
    private final byte[] oneByte = new byte[1];
    private SeekableInput in;

    SeekableInputStream(SeekableInput in) throws IOException {
      this.in = in;
    }

    @Override
    public void seek(long p) throws IOException {
      if (p < 0)
        throw new IOException("Illegal seek: " + p);
      in.seek(p);
    }

    @Override
    public long tell() throws IOException {
      return in.tell();
    }

    @Override
    public long length() throws IOException {
      return in.length();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return in.read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      int n = read(oneByte, 0, 1);
      if (n == 1) {
        return oneByte[0] & 0xff;
      } else {
        return n;
      }
    }

    @Override
    public long skip(long skip) throws IOException {
      long position = in.tell();
      long length = in.length();
      long remaining = length - position;
      if (remaining > skip) {
        in.seek(skip);
        return in.tell() - position;
      } else {
        in.seek(remaining);
        return in.tell() - position;
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
      super.close();
    }

    @Override
    public int available() throws IOException {
      long remaining = (in.length() - in.tell());
      return (remaining > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) remaining;
    }
  }
}
