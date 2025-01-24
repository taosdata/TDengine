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

package org.apache.trevni.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroDatumConverterFactory;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes key/value pairs to an Trevni container file.
 *
 * <p>
 * Each entry in the Trevni container file will be a generic record with two
 * fields, named 'key' and 'value'. The input types may be basic Writable
 * objects like Text or IntWritable, or they may be AvroWrapper subclasses
 * (AvroKey or AvroValue). Writable objects will be converted to their
 * corresponding Avro types when written to the generic record key/value pair.
 * </p>
 *
 * @param <K> The type of key to write.
 * @param <V> The type of value to write.
 */
public class AvroTrevniKeyValueRecordWriter<K, V>
    extends AvroTrevniRecordWriterBase<AvroKey<K>, AvroValue<V>, GenericRecord> {

  /**
   * The writer schema for the generic record entries of the Trevni container
   * file.
   */
  Schema mKeyValuePairSchema;

  /** A reusable Avro generic record for writing key/value pairs to the file. */
  AvroKeyValue<Object, Object> keyValueRecord;

  /** A helper object that converts the input key to an Avro datum. */
  AvroDatumConverter<K, ?> keyConverter;

  /** A helper object that converts the input value to an Avro datum. */
  AvroDatumConverter<V, ?> valueConverter;

  /**
   * Constructor.
   * 
   * @param context The TaskAttempContext to supply the writer with information
   *                form the job configuration
   */
  public AvroTrevniKeyValueRecordWriter(TaskAttemptContext context) throws IOException {
    super(context);

    mKeyValuePairSchema = initSchema(context);
    keyValueRecord = new AvroKeyValue<>(new GenericData.Record(mKeyValuePairSchema));
  }

  /** {@inheritDoc} */
  @Override
  public void write(AvroKey<K> key, AvroValue<V> value) throws IOException, InterruptedException {

    keyValueRecord.setKey(key.datum());
    keyValueRecord.setValue(value.datum());
    writer.write(keyValueRecord.get());
    if (writer.sizeEstimate() >= blockSize) // block full
      flush();
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  @Override
  protected Schema initSchema(TaskAttemptContext context) {
    AvroDatumConverterFactory converterFactory = new AvroDatumConverterFactory(context.getConfiguration());

    keyConverter = converterFactory.create((Class<K>) context.getOutputKeyClass());
    valueConverter = converterFactory.create((Class<V>) context.getOutputValueClass());

    // Create the generic record schema for the key/value pair.
    return AvroKeyValue.getSchema(keyConverter.getWriterSchema(), valueConverter.getWriterSchema());

  }

}
