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
 * A {@link SchemaValidationStrategy} that checks that the data written with the
 * {@link Schema} to validate can be read by the existing schema according to
 * the default Avro schema resolution rules.
 *
 */
class ValidateCanBeRead implements SchemaValidationStrategy {

  /**
   * Validate that data written with first schema provided can be read using the
   * second schema, according to the default Avro schema resolution rules.
   *
   * @throws SchemaValidationException if the second schema cannot read data
   *                                   written by the first.
   */
  @Override
  public void validate(Schema toValidate, Schema existing) throws SchemaValidationException {
    ValidateMutualRead.canRead(toValidate, existing);
  }

}
