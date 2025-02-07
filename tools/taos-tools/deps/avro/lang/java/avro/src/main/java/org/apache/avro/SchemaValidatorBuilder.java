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
 * A Builder for creating SchemaValidators.
 * </p>
 */
public final class SchemaValidatorBuilder {
  private SchemaValidationStrategy strategy;

  public SchemaValidatorBuilder strategy(SchemaValidationStrategy strategy) {
    this.strategy = strategy;
    return this;
  }

  /**
   * Use a strategy that validates that a schema can be used to read existing
   * schema(s) according to the Avro default schema resolution.
   */
  public SchemaValidatorBuilder canReadStrategy() {
    this.strategy = new ValidateCanRead();
    return this;
  }

  /**
   * Use a strategy that validates that a schema can be read by existing schema(s)
   * according to the Avro default schema resolution.
   */
  public SchemaValidatorBuilder canBeReadStrategy() {
    this.strategy = new ValidateCanBeRead();
    return this;
  }

  /**
   * Use a strategy that validates that a schema can read existing schema(s), and
   * vice-versa, according to the Avro default schema resolution.
   */
  public SchemaValidatorBuilder mutualReadStrategy() {
    this.strategy = new ValidateMutualRead();
    return this;
  }

  public SchemaValidator validateLatest() {
    valid();
    return new ValidateLatest(strategy);
  }

  public SchemaValidator validateAll() {
    valid();
    return new ValidateAll(strategy);
  }

  private void valid() {
    if (null == strategy) {
      throw new AvroRuntimeException("SchemaValidationStrategy not specified in builder");
    }
  }

}
