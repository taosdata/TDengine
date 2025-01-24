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
package org.apache.avro.io;

import java.io.IOException;

import org.apache.avro.Schema;

/**
 * Read data of a schema.
 * <p>
 * Determines the in-memory data representation.
 */
public interface DatumReader<D> {

  /** Set the writer's schema. */
  void setSchema(Schema schema);

  /**
   * Read a datum. Traverse the schema, depth-first, reading all leaf values in
   * the schema into a datum that is returned. If the provided datum is non-null
   * it may be reused and returned.
   */
  D read(D reuse, Decoder in) throws IOException;

}
