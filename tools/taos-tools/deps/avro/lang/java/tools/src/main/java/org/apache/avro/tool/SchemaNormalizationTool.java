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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import joptsimple.OptionParser;

import joptsimple.OptionSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/**
 * Utility to convert an Avro @{Schema} to its canonical form.
 */
public class SchemaNormalizationTool implements Tool {
  @Override
  public String getName() {
    return "canonical";
  }

  @Override
  public String getShortDescription() {
    return "Converts an Avro Schema to its canonical form";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser p = new OptionParser();
    OptionSet opts = p.parse(args.toArray(new String[0]));

    if (opts.nonOptionArguments().size() != 2) {
      err.println("Expected 2 args: infile outfile (filenames or '-' for stdin/stdout)");
      p.printHelpOn(err);
      return 1;
    }

    BufferedInputStream inStream = Util.fileOrStdin(args.get(0), stdin);
    BufferedOutputStream outStream = Util.fileOrStdout(args.get(1), out);

    Schema schema = new Schema.Parser().parse(inStream);

    String canonicalForm = SchemaNormalization.toParsingForm(schema);

    outStream.write(canonicalForm.getBytes(StandardCharsets.UTF_8));

    Util.close(inStream);
    Util.close(outStream);

    return 0;
  }
}
