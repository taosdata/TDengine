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
package org.apache.avro.tool;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * Command-line "avro-tools" utilities should implement this interface for
 * delegation by {@link Main}.
 */
public interface Tool {
  /**
   * Runs the tool with supplied arguments. Input and output streams are
   * customizable for easier testing.
   *
   * @param in   Input stream to read data (typically System.in).
   * @param out  Output of tool (typically System.out).
   * @param err  Error stream (typically System.err).
   * @param args Non-null list of arguments.
   * @return result code (0 for success)
   * @throws Exception Just like main(), tools may throw Exception.
   */
  int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception;

  /**
   * Name of tool, to be used in listings.
   */
  String getName();

  /**
   * 1-line description to be used in command listings.
   */
  String getShortDescription();
}
