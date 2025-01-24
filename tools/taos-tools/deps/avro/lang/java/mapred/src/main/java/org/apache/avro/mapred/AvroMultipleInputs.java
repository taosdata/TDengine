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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class supports Avro-MapReduce jobs that have multiple input paths with a
 * different {@link Schema} and {@link AvroMapper} for each path.
 *
 * <p>
 * Usage:
 * </p>
 * <p>
 * <strong>Case 1: (ReflectData based inputs)</strong>
 * </p>
 *
 * <pre>
 * // Enable ReflectData usage across job.
 * AvroJob.setReflect(job);
 *
 * Schema type1Schema = ReflectData.get().getSchema(Type1Record.class)
 * AvroMultipleInputs.addInputPath(job, inputPath1, type1Schema, Type1AvroMapper.class);
 * </pre>
 *
 * Where Type1AvroMapper would be implemented as
 *
 * <pre>
 *  class Type1AvroMapper extends AvroMapper&lt;Type1Record, Pair&lt;ComparingKeyRecord, CommonValueRecord&gt;&gt;
 * </pre>
 *
 * <pre>
 * Schema type2Schema = ReflectData.get().getSchema(Type2Record.class)
 * AvroMultipleInputs.addInputPath(job, inputPath2, type2Schema, Type2AvroMapper.class);
 * </pre>
 *
 * Where Type2AvroMapper would be implemented as
 *
 * <pre>
 *  class Type2AvroMapper extends AvroMapper&lt;Type2Record, Pair&lt;ComparingKeyRecord, CommonValueRecord&gt;&gt;
 * </pre>
 *
 * <p>
 * <strong>Case 2: (SpecificData based inputs)</strong>
 * </p>
 *
 * <pre>
 * Schema type1Schema = Type1Record.SCHEMA$;
 * AvroMultipleInputs.addInputPath(job, inputPath1, type1Schema, Type1AvroMapper.class);
 * </pre>
 *
 * Where Type1AvroMapper would be implemented as
 *
 * <pre>
 *  class Type1AvroMapper extends AvroMapper&lt;Type1Record, Pair&lt;ComparingKeyRecord, CommonValueRecord&gt;&gt;
 * </pre>
 *
 * <pre>
 * Schema type2Schema = Type2Record.SCHEMA$;
 * AvroMultipleInputs.addInputPath(job, inputPath2, type2Schema, Type2AvroMapper.class);
 * </pre>
 *
 * Where Type2AvroMapper would be implemented as
 *
 * <pre>
 *  class Type2AvroMapper extends AvroMapper&lt;Type2Record, Pair&lt;ComparingKeyRecord, CommonValueRecord&gt;&gt;
 * </pre>
 *
 * <p>
 * <strong>Note on InputFormat:</strong> The InputFormat used will always be
 * {@link AvroInputFormat} when using this class.
 * </p>
 * <p>
 * <strong>Note on collector outputs:</strong> When using this class, you will
 * need to ensure that the mapper implementations involved must all emit the
 * same Key type and Value record types, as set by
 * {@link AvroJob#setOutputSchema(JobConf, Schema)} or
 * {@link AvroJob#setMapOutputSchema(JobConf, Schema)}.
 * </p>
 */
public class AvroMultipleInputs {

  private static final Logger LOG = LoggerFactory.getLogger(AvroMultipleInputs.class);

  private static final String SCHEMA_KEY = "avro.mapreduce.input.multipleinputs.dir.schemas";
  private static final String MAPPERS_KEY = "avro.mapreduce.input.multipleinputs.dir.mappers";

  /**
   * Add a {@link Path} with a custom {@link Schema} to the list of inputs for the
   * map-reduce job.
   *
   * @param conf        The configuration of the job
   * @param path        {@link Path} to be added to the list of inputs for the job
   * @param inputSchema {@link Schema} class to use for this path
   */
  private static void addInputPath(JobConf conf, Path path, Schema inputSchema) {

    String schemaMapping = path.toString() + ";" + toBase64(inputSchema.toString());

    String schemas = conf.get(SCHEMA_KEY);
    conf.set(SCHEMA_KEY, schemas == null ? schemaMapping : schemas + "," + schemaMapping);

    conf.setInputFormat(DelegatingInputFormat.class);
  }

  /**
   * Add a {@link Path} with a custom {@link Schema} and {@link AvroMapper} to the
   * list of inputs for the map-reduce job.
   *
   * @param conf        The configuration of the job
   * @param path        {@link Path} to be added to the list of inputs for the job
   * @param inputSchema {@link Schema} to use for this path
   * @param mapperClass {@link AvroMapper} class to use for this path
   */
  public static void addInputPath(JobConf conf, Path path, Class<? extends AvroMapper> mapperClass,
      Schema inputSchema) {

    addInputPath(conf, path, inputSchema);

    String mapperMapping = path.toString() + ";" + mapperClass.getName();
    LOG.info(mapperMapping);
    String mappers = conf.get(MAPPERS_KEY);
    conf.set(MAPPERS_KEY, mappers == null ? mapperMapping : mappers + "," + mapperMapping);

    conf.setMapperClass(DelegatingMapper.class);
  }

  /**
   * Retrieves a map of {@link Path}s to the {@link AvroMapper} class that should
   * be used for them.
   *
   * @param conf The configuration of the job
   * @see #addInputPath(JobConf, Path, Class, Schema)
   * @return A map of paths-to-mappers for the job
   */
  @SuppressWarnings("unchecked")
  static Map<Path, Class<? extends AvroMapper>> getMapperTypeMap(JobConf conf) {
    if (conf.get(MAPPERS_KEY) == null) {
      return Collections.emptyMap();
    }
    Map<Path, Class<? extends AvroMapper>> m = new HashMap<>();
    String[] pathMappings = conf.get(MAPPERS_KEY).split(",");
    for (String pathMapping : pathMappings) {
      String[] split = pathMapping.split(";");
      Class<? extends AvroMapper> mapClass;
      try {
        mapClass = (Class<? extends AvroMapper>) conf.getClassByName(split[1]);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      m.put(new Path(split[0]), mapClass);
    }
    return m;
  }

  /**
   * Retrieves a map of {@link Path}s to the {@link Schema} that should be used
   * for them.
   *
   * @param conf The configuration of the job
   * @see #addInputPath(JobConf, Path, Class, Schema)
   * @return A map of paths to schemas for the job
   */
  static Map<Path, Schema> getInputSchemaMap(JobConf conf) {
    if (conf.get(SCHEMA_KEY) == null) {
      return Collections.emptyMap();
    }
    Map<Path, Schema> m = new HashMap<>();
    String[] schemaMappings = conf.get(SCHEMA_KEY).split(",");
    Schema.Parser schemaParser = new Schema.Parser();
    for (String schemaMapping : schemaMappings) {
      String[] split = schemaMapping.split(";");
      String schemaString = fromBase64(split[1]);
      Schema inputSchema;
      try {
        inputSchema = schemaParser.parse(schemaString);
      } catch (SchemaParseException e) {
        throw new RuntimeException(e);
      }
      m.put(new Path(split[0]), inputSchema);
    }
    return m;
  }

  private static String toBase64(String rawString) {
    final byte[] buf = rawString.getBytes(UTF_8);
    return new String(Base64.getMimeEncoder().encode(buf), UTF_8);
  }

  private static String fromBase64(String base64String) {
    final byte[] buf = base64String.getBytes(UTF_8);
    return new String(Base64.getMimeDecoder().decode(buf), UTF_8);
  }

}
