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

package org.apache.avro.mapreduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes key/value pairs to an Avro container file.
 *
 * <p>
 * Each entry in the Avro container file will be a generic record with two
 * fields, named 'key' and 'value'. The input types may be basic Writable
 * objects like Text or IntWritable, or they may be AvroWrapper subclasses
 * (AvroKey or AvroValue). Writable objects will be converted to their
 * corresponding Avro types when written to the generic record key/value pair.
 * </p>
 *
 * @param <K> The type of key to write.
 * @param <V> The type of value to write.
 */
public class AvroKeyValueRecordWriter<K, V> extends RecordWriter<K, V> implements Syncable {
  /** A writer for the Avro container file. */
  private final DataFileWriter<GenericRecord> mAvroFileWriter;

  /**
   * The writer schema for the generic record entries of the Avro container file.
   */
  private final Schema mKeyValuePairSchema;

  /** A reusable Avro generic record for writing key/value pairs to the file. */
  private final AvroKeyValue<Object, Object> mOutputRecord;

  /** A helper object that converts the input key to an Avro datum. */
  private final AvroDatumConverter<K, ?> mKeyConverter;

  /** A helper object that converts the input value to an Avro datum. */
  private final AvroDatumConverter<V, ?> mValueConverter;

  /**
   * Constructor.
   *
   * @param keyConverter     A key to Avro datum converter.
   * @param valueConverter   A value to Avro datum converter.
   * @param dataModel        The data model for key and value.
   * @param compressionCodec A compression codec factory for the Avro container
   *                         file.
   * @param outputStream     The output stream to write the Avro container file
   *                         to.
   * @param syncInterval     The sync interval for the Avro container file.
   * @throws IOException If the record writer cannot be opened.
   */
  public AvroKeyValueRecordWriter(AvroDatumConverter<K, ?> keyConverter, AvroDatumConverter<V, ?> valueConverter,
      GenericData dataModel, CodecFactory compressionCodec, OutputStream outputStream, int syncInterval)
      throws IOException {
    // Create the generic record schema for the key/value pair.
    mKeyValuePairSchema = AvroKeyValue.getSchema(keyConverter.getWriterSchema(), valueConverter.getWriterSchema());

    // Create an Avro container file and a writer to it.
    mAvroFileWriter = new DataFileWriter<GenericRecord>(dataModel.createDatumWriter(mKeyValuePairSchema));
    mAvroFileWriter.setCodec(compressionCodec);
    mAvroFileWriter.setSyncInterval(syncInterval);
    mAvroFileWriter.create(mKeyValuePairSchema, outputStream);

    // Keep a reference to the converters.
    mKeyConverter = keyConverter;
    mValueConverter = valueConverter;

    // Create a reusable output record.
    mOutputRecord = new AvroKeyValue<>(new GenericData.Record(mKeyValuePairSchema));
  }

  /**
   * Constructor.
   *
   * @param keyConverter     A key to Avro datum converter.
   * @param valueConverter   A value to Avro datum converter.
   * @param dataModel        The data model for key and value.
   * @param compressionCodec A compression codec factory for the Avro container
   *                         file.
   * @param outputStream     The output stream to write the Avro container file
   *                         to.
   * @throws IOException If the record writer cannot be opened.
   */
  public AvroKeyValueRecordWriter(AvroDatumConverter<K, ?> keyConverter, AvroDatumConverter<V, ?> valueConverter,
      GenericData dataModel, CodecFactory compressionCodec, OutputStream outputStream) throws IOException {
    this(keyConverter, valueConverter, dataModel, compressionCodec, outputStream,
        DataFileConstants.DEFAULT_SYNC_INTERVAL);
  }

  /**
   * Gets the writer schema for the key/value pair generic record.
   *
   * @return The writer schema used for entries of the Avro container file.
   */
  public Schema getWriterSchema() {
    return mKeyValuePairSchema;
  }

  /** {@inheritDoc} */
  @Override
  public void write(K key, V value) throws IOException {
    mOutputRecord.setKey(mKeyConverter.convert(key));
    mOutputRecord.setValue(mValueConverter.convert(value));
    mAvroFileWriter.append(mOutputRecord.get());
  }

  /** {@inheritDoc} */
  @Override
  public void close(TaskAttemptContext context) throws IOException {
    mAvroFileWriter.close();
  }

  /** {@inheritDoc} */
  @Override
  public long sync() throws IOException {
    return mAvroFileWriter.sync();
  }
}
