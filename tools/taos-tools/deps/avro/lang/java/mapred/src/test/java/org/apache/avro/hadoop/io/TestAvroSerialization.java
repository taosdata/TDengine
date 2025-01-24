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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.junit.Assert;

public class TestAvroSerialization {
  @Test
  public void testAccept() {
    AvroSerialization<CharSequence> serialization = new AvroSerialization<>();

    assertTrue(serialization.accept(AvroKey.class));
    assertTrue(serialization.accept(AvroValue.class));
    assertFalse(serialization.accept(AvroWrapper.class));
    assertFalse(serialization.accept(String.class));
  }

  @Test
  public void testGetSerializerForKey() throws IOException {
    // Set the writer schema in the job configuration.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    Job job = Job.getInstance();
    AvroJob.setMapOutputKeySchema(job, writerSchema);

    // Get a serializer from the configuration.
    AvroSerialization serialization = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Serializer<AvroWrapper> serializer = serialization.getSerializer(AvroKey.class);
    assertTrue(serializer instanceof AvroSerializer);
    AvroSerializer avroSerializer = (AvroSerializer) serializer;

    // Check that the writer schema is set correctly on the serializer.
    assertEquals(writerSchema, avroSerializer.getWriterSchema());
  }

  @Test
  public void testGetSerializerForValue() throws IOException {
    // Set the writer schema in the job configuration.
    Schema writerSchema = Schema.create(Schema.Type.STRING);
    Job job = Job.getInstance();
    AvroJob.setMapOutputValueSchema(job, writerSchema);

    // Get a serializer from the configuration.
    AvroSerialization serialization = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Serializer<AvroWrapper> serializer = serialization.getSerializer(AvroValue.class);
    assertTrue(serializer instanceof AvroSerializer);
    AvroSerializer avroSerializer = (AvroSerializer) serializer;

    // Check that the writer schema is set correctly on the serializer.
    assertEquals(writerSchema, avroSerializer.getWriterSchema());
  }

  @Test
  public void testGetDeserializerForKey() throws IOException {
    // Set the reader schema in the job configuration.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    Job job = Job.getInstance();
    AvroJob.setMapOutputKeySchema(job, readerSchema);

    // Get a deserializer from the configuration.
    AvroSerialization serialization = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Deserializer<AvroWrapper> deserializer = serialization.getDeserializer(AvroKey.class);
    assertTrue(deserializer instanceof AvroKeyDeserializer);
    AvroKeyDeserializer avroDeserializer = (AvroKeyDeserializer) deserializer;

    // Check that the reader schema is set correctly on the deserializer.
    assertEquals(readerSchema, avroDeserializer.getReaderSchema());
  }

  @Test
  public void testGetDeserializerForValue() throws IOException {
    // Set the reader schema in the job configuration.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    Job job = Job.getInstance();
    AvroJob.setMapOutputValueSchema(job, readerSchema);

    // Get a deserializer from the configuration.
    AvroSerialization serialization = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    @SuppressWarnings("unchecked")
    Deserializer<AvroWrapper> deserializer = serialization.getDeserializer(AvroValue.class);
    assertTrue(deserializer instanceof AvroValueDeserializer);
    AvroValueDeserializer avroDeserializer = (AvroValueDeserializer) deserializer;

    // Check that the reader schema is set correctly on the deserializer.
    assertEquals(readerSchema, avroDeserializer.getReaderSchema());
  }

  @Test
  public void testClassPath() throws Exception {
    Configuration conf = new Configuration();
    ClassLoader loader = conf.getClass().getClassLoader();
    AvroSerialization serialization = new AvroSerialization();
    serialization.setConf(conf);
    AvroDeserializer des = (AvroDeserializer) serialization.getDeserializer(AvroKey.class);
    ReflectData data = (ReflectData) ((ReflectDatumReader) des.mAvroDatumReader).getData();
    Assert.assertEquals(loader, data.getClassLoader());
  }

  private <T, O> O roundTrip(Schema schema, T data, Class<? extends GenericData> modelClass) throws IOException {
    Job job = Job.getInstance();
    AvroJob.setMapOutputKeySchema(job, schema);
    if (modelClass != null)
      AvroJob.setDataModelClass(job, modelClass);
    AvroSerialization serialization = ReflectionUtils.newInstance(AvroSerialization.class, job.getConfiguration());
    Serializer<AvroKey<T>> serializer = serialization.getSerializer(AvroKey.class);
    Deserializer<AvroKey<O>> deserializer = serialization.getDeserializer(AvroKey.class);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.open(baos);
    serializer.serialize(new AvroKey<>(data));
    serializer.close();

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    deserializer.open(bais);
    AvroKey<O> result = null;
    result = deserializer.deserialize(result);
    deserializer.close();

    return result.datum();
  }

  @Test
  public void testRoundTrip() throws Exception {
    Schema schema = Schema.create(Schema.Type.STRING);
    assertTrue(roundTrip(schema, "record", null) instanceof String);
    assertTrue(roundTrip(schema, "record", GenericData.class) instanceof Utf8);
  }
}
