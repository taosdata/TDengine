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

package org.apache.avro.mapred;

import java.util.Collection;
import java.lang.reflect.Constructor;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

/** Setters to configure jobs for Avro data. */
public class AvroJob {
  private AvroJob() {
  } // no public ctor

  static final String MAPPER = "avro.mapper";
  static final String COMBINER = "avro.combiner";
  static final String REDUCER = "avro.reducer";

  /** The configuration key for a job's input schema. */
  public static final String INPUT_SCHEMA = "avro.input.schema";
  /** The configuration key for a job's intermediate schema. */
  public static final String MAP_OUTPUT_SCHEMA = "avro.map.output.schema";
  /** The configuration key for a job's output schema. */
  public static final String OUTPUT_SCHEMA = "avro.output.schema";
  /**
   * The configuration key for a job's output compression codec. This takes one of
   * the strings registered in {@link org.apache.avro.file.CodecFactory}
   */
  public static final String OUTPUT_CODEC = "avro.output.codec";
  /** The configuration key prefix for a text output metadata. */
  public static final String TEXT_PREFIX = "avro.meta.text.";
  /** The configuration key prefix for a binary output metadata. */
  public static final String BINARY_PREFIX = "avro.meta.binary.";
  /** The configuration key for reflection-based input representation. */
  public static final String INPUT_IS_REFLECT = "avro.input.is.reflect";
  /** The configuration key for reflection-based map output representation. */
  public static final String MAP_OUTPUT_IS_REFLECT = "avro.map.output.is.reflect";
  /** The configuration key for the data model implementation class. */
  private static final String CONF_DATA_MODEL = "avro.serialization.data.model";

  /** Configure a job's map input schema. */
  public static void setInputSchema(JobConf job, Schema s) {
    job.set(INPUT_SCHEMA, s.toString());
    configureAvroInput(job);
  }

  /** Return a job's map input schema. */
  public static Schema getInputSchema(Configuration job) {
    String schemaString = job.get(INPUT_SCHEMA);
    return schemaString != null ? new Schema.Parser().parse(schemaString) : null;
  }

  /**
   * Configure a job's map output schema. The map output schema defaults to the
   * output schema and need only be specified when it differs. Thus must be a
   * {@link Pair} schema.
   */
  public static void setMapOutputSchema(JobConf job, Schema s) {
    job.set(MAP_OUTPUT_SCHEMA, s.toString());
    configureAvroShuffle(job);
  }

  /** Return a job's map output key schema. */
  public static Schema getMapOutputSchema(Configuration job) {
    return new Schema.Parser().parse(job.get(MAP_OUTPUT_SCHEMA, job.get(OUTPUT_SCHEMA)));
  }

  /**
   * Configure a job's output schema. Unless this is a map-only job, this must be
   * a {@link Pair} schema.
   */
  public static void setOutputSchema(JobConf job, Schema s) {
    job.set(OUTPUT_SCHEMA, s.toString());
    configureAvroOutput(job);
  }

  /** Configure a job's output compression codec. */
  public static void setOutputCodec(JobConf job, String codec) {
    job.set(OUTPUT_CODEC, codec);
  }

  /** Add metadata to job output files. */
  public static void setOutputMeta(JobConf job, String key, String value) {
    job.set(TEXT_PREFIX + key, value);
  }

  /** Add metadata to job output files. */
  public static void setOutputMeta(JobConf job, String key, long value) {
    job.set(TEXT_PREFIX + key, Long.toString(value));
  }

  /** Add metadata to job output files. */
  public static void setOutputMeta(JobConf job, String key, byte[] value) {
    try {
      job.set(BINARY_PREFIX + key,
          URLEncoder.encode(new String(value, StandardCharsets.ISO_8859_1), StandardCharsets.ISO_8859_1.name()));
    } catch (UnsupportedEncodingException e) {
    }
  }

  /** Indicate that a job's input files are in SequenceFile format. */
  public static void setInputSequenceFile(JobConf job) {
    job.setInputFormat(SequenceFileInputFormat.class);
  }

  /** Indicate that all a job's data should use the reflect representation. */
  public static void setReflect(JobConf job) {
    setInputReflect(job);
    setMapOutputReflect(job);
  }

  /** Indicate that a job's input data should use reflect representation. */
  public static void setInputReflect(JobConf job) {
    job.setBoolean(INPUT_IS_REFLECT, true);
  }

  /** Indicate that a job's map output data should use reflect representation. */
  public static void setMapOutputReflect(JobConf job) {
    job.setBoolean(MAP_OUTPUT_IS_REFLECT, true);
  }

  /** Return a job's output key schema. */
  public static Schema getOutputSchema(Configuration job) {
    return new Schema.Parser().parse(job.get(OUTPUT_SCHEMA));
  }

  private static void configureAvroInput(JobConf job) {
    if (job.get("mapred.input.format.class") == null)
      job.setInputFormat(AvroInputFormat.class);

    if (job.getMapperClass() == IdentityMapper.class)
      job.setMapperClass(HadoopMapper.class);

    configureAvroShuffle(job);
  }

  private static void configureAvroOutput(JobConf job) {
    if (job.get("mapred.output.format.class") == null)
      job.setOutputFormat(AvroOutputFormat.class);

    if (job.getReducerClass() == IdentityReducer.class)
      job.setReducerClass(HadoopReducer.class);

    job.setOutputKeyClass(AvroWrapper.class);
    configureAvroShuffle(job);
  }

  private static void configureAvroShuffle(JobConf job) {
    job.setOutputKeyComparatorClass(AvroKeyComparator.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);

    // add AvroSerialization to io.serializations
    Collection<String> serializations = job.getStringCollection("io.serializations");
    if (!serializations.contains(AvroSerialization.class.getName())) {
      serializations.add(AvroSerialization.class.getName());
      job.setStrings("io.serializations", serializations.toArray(new String[0]));
    }
  }

  /** Configure a job's mapper implementation. */
  public static void setMapperClass(JobConf job, Class<? extends AvroMapper> c) {
    job.set(MAPPER, c.getName());
  }

  /** Configure a job's combiner implementation. */
  public static void setCombinerClass(JobConf job, Class<? extends AvroReducer> c) {
    job.set(COMBINER, c.getName());
    job.setCombinerClass(HadoopCombiner.class);
  }

  /** Configure a job's reducer implementation. */
  public static void setReducerClass(JobConf job, Class<? extends AvroReducer> c) {
    job.set(REDUCER, c.getName());
  }

  /** Configure a job's data model implementation class. */
  public static void setDataModelClass(JobConf job, Class<? extends GenericData> modelClass) {
    job.setClass(CONF_DATA_MODEL, modelClass, GenericData.class);
  }

  /** Return the job's data model implementation class. */
  public static Class<? extends GenericData> getDataModelClass(Configuration conf) {
    return conf.getClass(CONF_DATA_MODEL, ReflectData.class, GenericData.class);
  }

  private static GenericData newDataModelInstance(Class<? extends GenericData> modelClass, Configuration conf) {
    GenericData dataModel;
    try {
      Constructor<? extends GenericData> ctor = modelClass.getDeclaredConstructor(ClassLoader.class);
      ctor.setAccessible(true);
      dataModel = ctor.newInstance(conf.getClassLoader());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ReflectionUtils.setConf(dataModel, conf);
    return dataModel;
  }

  public static GenericData createDataModel(Configuration conf) {
    return newDataModelInstance(getDataModelClass(conf), conf);
  }

  public static GenericData createInputDataModel(Configuration conf) {
    String className = conf.get(CONF_DATA_MODEL, null);
    Class<? extends GenericData> modelClass;
    if (className != null) {
      modelClass = getDataModelClass(conf);
    } else if (conf.getBoolean(INPUT_IS_REFLECT, false)) {
      modelClass = ReflectData.class;
    } else {
      modelClass = SpecificData.class;
    }
    return newDataModelInstance(modelClass, conf);
  }

  public static GenericData createMapOutputDataModel(Configuration conf) {
    String className = conf.get(CONF_DATA_MODEL, null);
    Class<? extends GenericData> modelClass;
    if (className != null) {
      modelClass = getDataModelClass(conf);
    } else if (conf.getBoolean(MAP_OUTPUT_IS_REFLECT, false)) {
      modelClass = ReflectData.class;
    } else {
      modelClass = SpecificData.class;
    }
    return newDataModelInstance(modelClass, conf);
  }
}
