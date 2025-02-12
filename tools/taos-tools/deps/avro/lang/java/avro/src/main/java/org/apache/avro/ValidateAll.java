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

package org.apache.avro;

/**
 * <p>
 * A {@link SchemaValidator} for validating the provided schema against all
 * schemas in the Iterable in {@link #validate(Schema, Iterable)}.
 * </p>
 * <p>
 * Uses the {@link SchemaValidationStrategy} provided in the constructor to
 * validate the {@link Schema} against each Schema in the Iterable, in Iterator
 * order, via {@link SchemaValidationStrategy#validate(Schema, Schema)}.
 * </p>
 */
public final class ValidateAll implements SchemaValidator {
  private final SchemaValidationStrategy strategy;

  /**
   * @param strategy The strategy to use for validation of pairwise schemas.
   */
  public ValidateAll(SchemaValidationStrategy strategy) {
    this.strategy = strategy;
  }

  @Override
  public void validate(Schema toValidate, Iterable<Schema> schemasInOrder) throws SchemaValidationException {
    for (Schema existing : schemasInOrder) {
      strategy.validate(toValidate, existing);
    }
  }

}
