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

import java.io.IOException;

import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;

/**
 * A {@link SchemaValidationStrategy} that checks that the {@link Schema} to
 * validate and the existing schema can mutually read each other according to
 * the default Avro schema resolution rules.
 *
 */
class ValidateMutualRead implements SchemaValidationStrategy {

  /**
   * Validate that the schemas provided can mutually read data written by each
   * other according to the default Avro schema resolution rules.
   *
   * @throws SchemaValidationException if the schemas are not mutually compatible.
   */
  @Override
  public void validate(Schema toValidate, Schema existing) throws SchemaValidationException {
    canRead(toValidate, existing);
    canRead(existing, toValidate);
  }

  /**
   * Validates that data written with one schema can be read using another, based
   * on the default Avro schema resolution rules.
   *
   * @param writtenWith The "writer's" schema, representing data to be read.
   * @param readUsing   The "reader's" schema, representing how the reader will
   *                    interpret data.
   * @throws SchemaValidationException if the schema <b>readUsing<b/> cannot be
   *                                   used to read data written with
   *                                   <b>writtenWith<b/>
   */
  static void canRead(Schema writtenWith, Schema readUsing) throws SchemaValidationException {
    boolean error;
    try {
      error = Symbol.hasErrors(new ResolvingGrammarGenerator().generate(writtenWith, readUsing));
    } catch (IOException e) {
      throw new SchemaValidationException(readUsing, writtenWith, e);
    }
    if (error) {
      throw new SchemaValidationException(readUsing, writtenWith);
    }
  }

}
