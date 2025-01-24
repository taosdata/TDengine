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

package org.apache.avro;

/**
 * <p>
 * A SchemaValidator has one method, which validates that a {@link Schema} is
 * <b>compatible<b/> with the other schemas provided.
 * </p>
 * <p>
 * What makes one Schema compatible with another is not part of the interface
 * contract.
 * </p>
 */
public interface SchemaValidator {

  /**
   * Validate one schema against others. The order of the schemas to validate
   * against is chronological from most recent to oldest, if there is a natural
   * chronological order. This allows some validators to identify which schemas
   * are the most "recent" in order to validate only against the most recent
   * schema(s).
   *
   * @param toValidate The schema to validate
   * @param existing   The schemas to validate against, in order from most recent
   *                   to latest if applicable
   * @throws SchemaValidationException if the schema fails to validate.
   */
  void validate(Schema toValidate, Iterable<Schema> existing) throws SchemaValidationException;

}
