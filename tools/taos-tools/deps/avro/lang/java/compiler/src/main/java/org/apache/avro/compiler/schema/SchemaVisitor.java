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
package org.apache.avro.compiler.schema;

import org.apache.avro.Schema;

public interface SchemaVisitor<T> {

  /**
   * Invoked for schemas that do not have "child" schemas (like string, int ...)
   * or for a previously encountered schema with children, which will be treated
   * as a terminal. (to avoid circular recursion)
   *
   * @param terminal
   */
  SchemaVisitorAction visitTerminal(Schema terminal);

  /**
   * Invoked for schema with children before proceeding to visit the children.
   *
   * @param nonTerminal
   */
  SchemaVisitorAction visitNonTerminal(Schema nonTerminal);

  /**
   * Invoked for schemas with children after its children have been visited.
   *
   * @param nonTerminal
   */
  SchemaVisitorAction afterVisitNonTerminal(Schema nonTerminal);

  /**
   * Invoked when visiting is complete.
   *
   * @return a value which will be returned by the visit method.
   */
  T get();

}
