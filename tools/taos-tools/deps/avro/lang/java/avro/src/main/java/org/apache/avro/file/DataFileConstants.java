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

/**
 * Constants used in data files.
 */
public class DataFileConstants {
  private DataFileConstants() {
  } // no public ctor

  public static final byte VERSION = 1;
  public static final byte[] MAGIC = new byte[] { (byte) 'O', (byte) 'b', (byte) 'j', VERSION };
  public static final long FOOTER_BLOCK = -1;
  public static final int SYNC_SIZE = 16;
  public static final int DEFAULT_SYNC_INTERVAL = 4000 * SYNC_SIZE;

  public static final String SCHEMA = "avro.schema";
  public static final String CODEC = "avro.codec";
  public static final String NULL_CODEC = "null";
  public static final String DEFLATE_CODEC = "deflate";
  public static final String SNAPPY_CODEC = "snappy";
  public static final String BZIP2_CODEC = "bzip2";
  public static final String XZ_CODEC = "xz";
  public static final String ZSTANDARD_CODEC = "zstandard";

}
