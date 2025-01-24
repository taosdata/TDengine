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
import java.io.InputStream;

import org.apache.avro.Schema;

/**
 * A factory for creating and configuring {@link Decoder}s.
 * <p/>
 * Factories are thread-safe, and are generally cached by applications for
 * performance reasons. Multiple instances are only required if multiple
 * concurrent configurations are needed.
 *
 * @see Decoder
 */

public class DecoderFactory {
  private static final DecoderFactory DEFAULT_FACTORY = new DefaultDecoderFactory();
  static final int DEFAULT_BUFFER_SIZE = 8192;

  int binaryDecoderBufferSize = DEFAULT_BUFFER_SIZE;

  /** Constructor for factory instances */
  public DecoderFactory() {
    super();
  }

  /**
   * @deprecated use the equivalent {@link #get()} instead
   */
  @Deprecated
  public static DecoderFactory defaultFactory() {
    return get();
  }

  /**
   * Returns an immutable static DecoderFactory configured with default settings
   * All mutating methods throw IllegalArgumentExceptions. All creator methods
   * create objects with default settings.
   */
  public static DecoderFactory get() {
    return DEFAULT_FACTORY;
  }

  /**
   * Configures this factory to use the specified buffer size when creating
   * Decoder instances that buffer their input. The default buffer size is 8192
   * bytes.
   *
   * @param size The preferred buffer size. Valid values are in the range [32,
   *             16*1024*1024]. Values outside this range are rounded to the
   *             nearest value in the range. Values less than 512 or greater than
   *             1024*1024 are not recommended.
   * @return This factory, to enable method chaining:
   * 
   *         <pre>
   *         DecoderFactory myFactory = new DecoderFactory().useBinaryDecoderBufferSize(4096);
   *         </pre>
   */
  public DecoderFactory configureDecoderBufferSize(int size) {
    if (size < 32)
      size = 32;
    if (size > 16 * 1024 * 1024)
      size = 16 * 1024 * 1024;
    this.binaryDecoderBufferSize = size;
    return this;
  }

  /**
   * Returns this factory's configured preferred buffer size. Used when creating
   * Decoder instances that buffer. See {@link #configureDecoderBufferSize}
   * 
   * @return The preferred buffer size, in bytes.
   */
  public int getConfiguredBufferSize() {
    return this.binaryDecoderBufferSize;
  }

  /**
   * @deprecated use the equivalent
   *             {@link #binaryDecoder(InputStream, BinaryDecoder)} instead
   */
  @Deprecated
  public BinaryDecoder createBinaryDecoder(InputStream in, BinaryDecoder reuse) {
    return binaryDecoder(in, reuse);
  }

  /**
   * Creates or reinitializes a {@link BinaryDecoder} with the input stream
   * provided as the source of data. If <i>reuse</i> is provided, it will be
   * reinitialized to the given input stream.
   * <p/>
   * {@link BinaryDecoder} instances returned by this method buffer their input,
   * reading up to {@link #getConfiguredBufferSize()} bytes past the minimum
   * required to satisfy read requests in order to achieve better performance. If
   * the buffering is not desired, use
   * {@link #directBinaryDecoder(InputStream, BinaryDecoder)}.
   * <p/>
   * {@link BinaryDecoder#inputStream()} provides a view on the data that is
   * buffer-aware, for users that need to interleave access to data with the
   * Decoder API.
   *
   * @param in    The InputStream to initialize to
   * @param reuse The BinaryDecoder to <i>attempt</i> to reuse given the factory
   *              configuration. A BinaryDecoder implementation may not be
   *              compatible with reuse, causing a new instance to be returned. If
   *              null, a new instance is returned.
   * @return A BinaryDecoder that uses <i>in</i> as its source of data. If
   *         <i>reuse</i> is null, this will be a new instance. If <i>reuse</i> is
   *         not null, then it may be reinitialized if compatible, otherwise a new
   *         instance will be returned.
   * @see BinaryDecoder
   * @see Decoder
   */
  public BinaryDecoder binaryDecoder(InputStream in, BinaryDecoder reuse) {
    if (null == reuse || !reuse.getClass().equals(BinaryDecoder.class)) {
      return new BinaryDecoder(in, binaryDecoderBufferSize);
    } else {
      return reuse.configure(in, binaryDecoderBufferSize);
    }
  }

  /**
   * Creates or reinitializes a {@link BinaryDecoder} with the input stream
   * provided as the source of data. If <i>reuse</i> is provided, it will be
   * reinitialized to the given input stream.
   * <p/>
   * {@link BinaryDecoder} instances returned by this method do not buffer their
   * input. In most cases a buffering BinaryDecoder is sufficient in combination
   * with {@link BinaryDecoder#inputStream()} which provides a buffer-aware view
   * on the data.
   * <p/>
   * A "direct" BinaryDecoder does not read ahead from an InputStream or other
   * data source that cannot be rewound. From the perspective of a client, a
   * "direct" decoder must never read beyond the minimum necessary bytes to
   * service a {@link BinaryDecoder} API read request.
   * <p/>
   * In the case that the improved performance of a buffering implementation does
   * not outweigh the inconvenience of its buffering semantics, a "direct" decoder
   * can be used.
   * 
   * @param in    The InputStream to initialize to
   * @param reuse The BinaryDecoder to <i>attempt</i> to reuse given the factory
   *              configuration. A BinaryDecoder implementation may not be
   *              compatible with reuse, causing a new instance to be returned. If
   *              null, a new instance is returned.
   * @return A BinaryDecoder that uses <i>in</i> as its source of data. If
   *         <i>reuse</i> is null, this will be a new instance. If <i>reuse</i> is
   *         not null, then it may be reinitialized if compatible, otherwise a new
   *         instance will be returned.
   * @see DirectBinaryDecoder
   * @see Decoder
   */
  public BinaryDecoder directBinaryDecoder(InputStream in, BinaryDecoder reuse) {
    if (null == reuse || !reuse.getClass().equals(DirectBinaryDecoder.class)) {
      return new DirectBinaryDecoder(in);
    } else {
      return ((DirectBinaryDecoder) reuse).configure(in);
    }
  }

  /**
   * @deprecated use {@link #binaryDecoder(byte[], int, int, BinaryDecoder)}
   *             instead
   */
  @Deprecated
  public BinaryDecoder createBinaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse) {
    if (null == reuse || !reuse.getClass().equals(BinaryDecoder.class)) {
      return new BinaryDecoder(bytes, offset, length);
    } else {
      return reuse.configure(bytes, offset, length);
    }
  }

  /**
   * Creates or reinitializes a {@link BinaryDecoder} with the byte array provided
   * as the source of data. If <i>reuse</i> is provided, it will attempt to
   * reinitialize <i>reuse</i> to the new byte array. This instance will use the
   * provided byte array as its buffer.
   * <p/>
   * {@link BinaryDecoder#inputStream()} provides a view on the data that is
   * buffer-aware and can provide a view of the data not yet read by Decoder API
   * methods.
   *
   * @param bytes  The byte array to initialize to
   * @param offset The offset to start reading from
   * @param length The maximum number of bytes to read from the byte array
   * @param reuse  The BinaryDecoder to attempt to reinitialize. if null a new
   *               BinaryDecoder is created.
   * @return A BinaryDecoder that uses <i>bytes</i> as its source of data. If
   *         <i>reuse</i> is null, this will be a new instance. <i>reuse</i> may
   *         be reinitialized if appropriate, otherwise a new instance is
   *         returned. Clients must not assume that <i>reuse</i> is reinitialized
   *         and returned.
   */
  public BinaryDecoder binaryDecoder(byte[] bytes, int offset, int length, BinaryDecoder reuse) {
    if (null == reuse || !reuse.getClass().equals(BinaryDecoder.class)) {
      return new BinaryDecoder(bytes, offset, length);
    } else {
      return reuse.configure(bytes, offset, length);
    }
  }

  /** @deprecated use {@link #binaryDecoder(byte[], BinaryDecoder)} instead */
  @Deprecated
  public BinaryDecoder createBinaryDecoder(byte[] bytes, BinaryDecoder reuse) {
    return binaryDecoder(bytes, 0, bytes.length, reuse);
  }

  /**
   * This method is shorthand for
   * 
   * <pre>
   * createBinaryDecoder(bytes, 0, bytes.length, reuse);
   * </pre>
   * 
   * {@link #binaryDecoder(byte[], int, int, BinaryDecoder)}
   */
  public BinaryDecoder binaryDecoder(byte[] bytes, BinaryDecoder reuse) {
    return binaryDecoder(bytes, 0, bytes.length, reuse);
  }

  /**
   * Creates a {@link JsonDecoder} using the InputStream provided for reading data
   * that conforms to the Schema provided.
   * <p/>
   *
   * @param schema The Schema for data read from this JsonEncoder. Cannot be null.
   * @param input  The InputStream to read from. Cannot be null.
   * @return A JsonEncoder configured with <i>input</i> and <i>schema</i>
   * @throws IOException
   */
  public JsonDecoder jsonDecoder(Schema schema, InputStream input) throws IOException {
    return new JsonDecoder(schema, input);
  }

  /**
   * Creates a {@link JsonDecoder} using the String provided for reading data that
   * conforms to the Schema provided.
   * <p/>
   *
   * @param schema The Schema for data read from this JsonEncoder. Cannot be null.
   * @param input  The String to read from. Cannot be null.
   * @return A JsonEncoder configured with <i>input</i> and <i>schema</i>
   * @throws IOException
   */
  public JsonDecoder jsonDecoder(Schema schema, String input) throws IOException {
    return new JsonDecoder(schema, input);
  }

  /**
   * Creates a {@link ValidatingDecoder} wrapping the Decoder provided. This
   * ValidatingDecoder will ensure that operations against it conform to the
   * schema provided.
   *
   * @param schema  The Schema to validate against. Cannot be null.
   * @param wrapped The Decoder to wrap.
   * @return A ValidatingDecoder configured with <i>wrapped</i> and <i>schema</i>
   * @throws IOException
   */
  public ValidatingDecoder validatingDecoder(Schema schema, Decoder wrapped) throws IOException {
    return new ValidatingDecoder(schema, wrapped);
  }

  /**
   * Creates a {@link ResolvingDecoder} wrapping the Decoder provided. This
   * ResolvingDecoder will resolve input conforming to the <i>writer</i> schema
   * from the wrapped Decoder, and present it as the <i>reader</i> schema.
   *
   * @param writer  The Schema that the source data is in. Cannot be null.
   * @param reader  The Schema that the reader wishes to read the data as. Cannot
   *                be null.
   * @param wrapped The Decoder to wrap.
   * @return A ResolvingDecoder configured to resolve <i>writer</i> to
   *         <i>reader</i> from <i>in</i>
   * @throws IOException
   */
  public ResolvingDecoder resolvingDecoder(Schema writer, Schema reader, Decoder wrapped) throws IOException {
    return new ResolvingDecoder(writer, reader, wrapped);
  }

  private static class DefaultDecoderFactory extends DecoderFactory {
    @Override
    public DecoderFactory configureDecoderBufferSize(int bufferSize) {
      throw new IllegalArgumentException("This Factory instance is Immutable");
    }
  }
}
