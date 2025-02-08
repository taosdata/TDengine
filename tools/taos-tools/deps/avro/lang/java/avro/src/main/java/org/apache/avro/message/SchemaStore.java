/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interface for classes that can provide avro schemas by fingerprint.
 */
public interface SchemaStore {

  /**
   * Retrieves a fingerprint by its AVRO-CRC-64 fingerprint.
   * 
   * @param fingerprint an AVRO-CRC-64 fingerprint long
   * @return a Schema with the given fingerprint, or null
   */
  Schema findByFingerprint(long fingerprint);

  /**
   * A map-based cache of schemas by AVRO-CRC-64 fingerprint.
   * <p>
   * This class is thread-safe.
   */
  class Cache implements SchemaStore {
    private final Map<Long, Schema> schemas = new ConcurrentHashMap<>();

    /**
     * Adds a schema to this cache that can be retrieved using its AVRO-CRC-64
     * fingerprint.
     *
     * @param schema a {@link Schema}
     */
    public void addSchema(Schema schema) {
      long fp = SchemaNormalization.parsingFingerprint64(schema);
      schemas.put(fp, schema);
    }

    @Override
    public Schema findByFingerprint(long fingerprint) {
      return schemas.get(fingerprint);
    }
  }
}
