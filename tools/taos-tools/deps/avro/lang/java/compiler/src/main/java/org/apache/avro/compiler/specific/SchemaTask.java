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
package org.apache.avro.compiler.specific;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;

/** Ant task to generate Java interface and classes for a protocol. */
public class SchemaTask extends ProtocolTask {
  @Override
  protected void doCompile(File src, File dest) throws IOException {
    final Schema.Parser parser = new Schema.Parser();
    final Schema schema = parser.parse(src);
    final SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.setStringType(getStringType());
    compiler.compileToDestination(src, dest);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: SchemaTask <schema.avsc>... <output-folder>");
      System.exit(1);
    }
    File dst = new File(args[args.length - 1]);
    for (int i = 0; i < args.length - 1; i++)
      new SchemaTask().doCompile(new File(args[i]), dst);
  }
}
