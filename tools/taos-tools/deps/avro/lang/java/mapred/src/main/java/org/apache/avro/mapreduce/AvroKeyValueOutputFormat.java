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

import org.apache.avro.generic.GenericData;
import org.apache.avro.hadoop.io.AvroDatumConverter;
import org.apache.avro.hadoop.io.AvroDatumConverterFactory;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * FileOutputFormat for writing Avro container files of key/value pairs.
 *
 * <p>
 * Since Avro container files can only contain records (not key/value pairs),
 * this output format puts the key and value into an Avro generic record with
 * two fields, named 'key' and 'value'.
 * </p>
 *
 * <p>
 * The keys and values given to this output format may be Avro objects wrapped
 * in <code>AvroKey</code> or <code>AvroValue</code> objects. The basic Writable
 * types are also supported (e.g., IntWritable, Text); they will be converted to
 * their corresponding Avro types.
 * </p>
 *
 * @param <K> The type of key. If an Avro type, it must be wrapped in an
 *            <code>AvroKey</code>.
 * @param <V> The type of value. If an Avro type, it must be wrapped in an
 *            <code>AvroValue</code>.
 */
public class AvroKeyValueOutputFormat<K, V> extends AvroOutputFormatBase<K, V> {
  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {
    Configuration conf = context.getConfiguration();

    AvroDatumConverterFactory converterFactory = new AvroDatumConverterFactory(conf);

    AvroDatumConverter<K, ?> keyConverter = converterFactory.create((Class<K>) context.getOutputKeyClass());
    AvroDatumConverter<V, ?> valueConverter = converterFactory.create((Class<V>) context.getOutputValueClass());

    GenericData dataModel = AvroSerialization.createDataModel(conf);

    OutputStream out = getAvroFileOutputStream(context);
    try {
      return new AvroKeyValueRecordWriter<>(keyConverter, valueConverter, dataModel, getCompressionCodec(context), out,
          getSyncInterval(context));
    } catch (IOException e) {
      out.close();
      throw e;
    }
  }
}
