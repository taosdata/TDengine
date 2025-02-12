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
package org.apache.avro.hadoop.file;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;

/**
 * Encapsulates the ability to specify and configure an avro compression codec
 * from a given hadoop codec defined with the configuration parameter:
 * mapred.output.compression.codec
 *
 * Currently there are three codecs registered by default:
 * <ul>
 * <li>{@code org.apache.hadoop.io.compress.DeflateCodec} will map to
 * {@code deflate}</li>
 * <li>{@code org.apache.hadoop.io.compress.SnappyCodec} will map to
 * {@code snappy}</li>
 * <li>{@code org.apache.hadoop.io.compress.BZip2Codec} will map to
 * {@code zbip2}</li>
 * <li>{@code org.apache.hadoop.io.compress.GZipCodec} will map to
 * {@code deflate}</li>
 * </ul>
 */
public class HadoopCodecFactory {

  private static final Map<String, String> HADOOP_AVRO_NAME_MAP = new HashMap<>();

  static {
    HADOOP_AVRO_NAME_MAP.put("org.apache.hadoop.io.compress.DeflateCodec", "deflate");
    HADOOP_AVRO_NAME_MAP.put("org.apache.hadoop.io.compress.SnappyCodec", "snappy");
    HADOOP_AVRO_NAME_MAP.put("org.apache.hadoop.io.compress.BZip2Codec", "bzip2");
    HADOOP_AVRO_NAME_MAP.put("org.apache.hadoop.io.compress.GZipCodec", "deflate");
  }

  /**
   * Maps a hadoop codec name into a CodecFactory.
   *
   * Currently there are four hadoop codecs registered:
   * <ul>
   * <li>{@code org.apache.hadoop.io.compress.DeflateCodec} will map to
   * {@code deflate}</li>
   * <li>{@code org.apache.hadoop.io.compress.SnappyCodec} will map to
   * {@code snappy}</li>
   * <li>{@code org.apache.hadoop.io.compress.BZip2Codec} will map to
   * {@code zbip2}</li>
   * <li>{@code org.apache.hadoop.io.compress.GZipCodec} will map to
   * {@code deflate}</li>
   * </ul>
   */
  public static CodecFactory fromHadoopString(String hadoopCodecClass) {

    CodecFactory o = null;
    try {
      String avroCodec = HADOOP_AVRO_NAME_MAP.get(hadoopCodecClass);
      if (avroCodec != null) {
        o = CodecFactory.fromString(avroCodec);
      }
    } catch (Exception e) {
      throw new AvroRuntimeException("Unrecognized hadoop codec: " + hadoopCodecClass, e);
    }
    return o;
  }

  public static String getAvroCodecName(String hadoopCodecClass) {
    return HADOOP_AVRO_NAME_MAP.get(hadoopCodecClass);
  }
}
