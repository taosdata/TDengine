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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.hadoop.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.io.serializer.Deserializer;

/**
 * Deserializes AvroWrapper objects within Hadoop.
 *
 * <p>
 * Keys and values containing Avro types are more efficiently serialized outside
 * of the WritableSerialization model, so they are wrapper in
 * {@link org.apache.avro.mapred.AvroWrapper} objects and deserialization is
 * handled by this class.
 * </p>
 *
 * <p>
 * MapReduce jobs that use AvroWrapper objects as keys or values need to be
 * configured with {@link AvroSerialization}. Use
 * {@link org.apache.avro.mapreduce.AvroJob} to help with Job configuration.
 * </p>
 *
 * @param <T> The type of Avro wrapper.
 * @param <D> The Java type of the Avro data being wrapped.
 */
public abstract class AvroDeserializer<T extends AvroWrapper<D>, D> implements Deserializer<T> {
  /** The Avro writer schema for deserializing. */
  private final Schema mWriterSchema;

  /** The Avro reader schema for deserializing. */
  private final Schema mReaderSchema;

  /** The Avro datum reader for deserializing. */
  final DatumReader<D> mAvroDatumReader;

  /** An Avro binary decoder for deserializing. */
  private BinaryDecoder mAvroDecoder;

  /**
   * Constructor.
   *
   * @param writerSchema The Avro writer schema for the data to deserialize.
   * @param readerSchema The Avro reader schema for the data to deserialize (may
   *                     be null).
   */
  protected AvroDeserializer(Schema writerSchema, Schema readerSchema, ClassLoader classLoader) {
    mWriterSchema = writerSchema;
    mReaderSchema = null != readerSchema ? readerSchema : writerSchema;
    mAvroDatumReader = new ReflectDatumReader<>(mWriterSchema, mReaderSchema, new ReflectData(classLoader));
  }

  /**
   * Constructor.
   *
   * @param writerSchema The Avro writer schema for the data to deserialize.
   * @param readerSchema The Avro reader schema for the data to deserialize (may
   *                     be null).
   * @param datumReader  The Avro datum reader to use for deserialization.
   */
  protected AvroDeserializer(Schema writerSchema, Schema readerSchema, DatumReader<D> datumReader) {
    mWriterSchema = writerSchema;
    mReaderSchema = null != readerSchema ? readerSchema : writerSchema;
    mAvroDatumReader = datumReader;
  }

  /**
   * Gets the writer schema used for deserializing.
   *
   * @return The writer schema;
   */
  public Schema getWriterSchema() {
    return mWriterSchema;
  }

  /**
   * Gets the reader schema used for deserializing.
   *
   * @return The reader schema.
   */
  public Schema getReaderSchema() {
    return mReaderSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void open(InputStream inputStream) throws IOException {
    mAvroDecoder = DecoderFactory.get().directBinaryDecoder(inputStream, mAvroDecoder);
  }

  /** {@inheritDoc} */
  @Override
  public T deserialize(T avroWrapperToReuse) throws IOException {
    // Create a new Avro wrapper if there isn't one to reuse.
    if (null == avroWrapperToReuse) {
      avroWrapperToReuse = createAvroWrapper();
    }

    // Deserialize the Avro datum from the input stream.
    avroWrapperToReuse.datum(mAvroDatumReader.read(avroWrapperToReuse.datum(), mAvroDecoder));
    return avroWrapperToReuse;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mAvroDecoder.inputStream().close();
  }

  /**
   * Creates a new empty <code>T</code> (extends AvroWrapper) instance.
   *
   * @return A new empty <code>T</code> instance.
   */
  protected abstract T createAvroWrapper();
}
