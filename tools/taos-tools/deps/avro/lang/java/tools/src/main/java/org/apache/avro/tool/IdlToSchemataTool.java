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

import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * Extract the Avro JSON schemata of the types of a protocol defined through an
 * idl format file.
 */
public class IdlToSchemataTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.isEmpty() || args.size() > 2 || isRequestingHelp(args)) {
      err.println("Usage: idl2schemata [idl] [outdir]");
      err.println("");
      err.println("If an output directory is not specified, " + "outputs to current directory.");
      return -1;
    }

    boolean pretty = true;
    Idl parser = new Idl(new File(args.get(0)));
    File outputDirectory = getOutputDirectory(args);

    for (Schema schema : parser.CompilationUnit().getTypes()) {
      print(schema, outputDirectory, pretty);
    }
    parser.close();

    return 0;
  }

  private boolean isRequestingHelp(List<String> args) {
    return args.size() == 1 && (args.get(0).equals("--help") || args.get(0).equals("-help"));
  }

  private File getOutputDirectory(List<String> args) {
    String dirname = (args.size() == 2) ? args.get(1) : "";
    File outputDirectory = new File(dirname);
    outputDirectory.mkdirs();
    return outputDirectory;
  }

  private void print(Schema schema, File outputDirectory, boolean pretty) throws FileNotFoundException {
    String dirpath = outputDirectory.getAbsolutePath();
    String filename = dirpath + "/" + schema.getName() + ".avsc";
    FileOutputStream fileOutputStream = new FileOutputStream(filename);
    PrintStream printStream = new PrintStream(fileOutputStream);
    printStream.println(schema.toString(pretty));
    printStream.close();
  }

  @Override
  public String getName() {
    return "idl2schemata";
  }

  @Override
  public String getShortDescription() {
    return "Extract JSON schemata of the types from an Avro IDL file";
  }
}
