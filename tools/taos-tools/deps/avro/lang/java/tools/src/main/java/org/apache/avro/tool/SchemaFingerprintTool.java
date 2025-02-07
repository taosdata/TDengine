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
import java.io.IOException;
import java.io.PrintStream;

import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

/**
 * Utility to generate fingerprint(s) from a schema.
 */
public class SchemaFingerprintTool implements Tool {

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    final OptionParser optParser = new OptionParser();
    final OptionSpec<String> fingerprintOpt = optParser
        .accepts("fingerprint",
            "Fingerprint algorithm to use. Recommended Avro practice dictiates "
                + "that \"CRC-64-AVRO\" is used for 64-bit fingerprints, \"MD5\" is "
                + "used for 128-bit fingerprints, and \"SHA-256\" is used for 256-bit " + "fingerprints.")
        .withRequiredArg().ofType(String.class).defaultsTo("CRC-64-AVRO");

    final OptionSet opts = optParser.parse(args.toArray(new String[0]));
    final Schema.Parser parser = new Schema.Parser();
    final List<String> nargs = (List<String>) opts.nonOptionArguments();
    if (nargs.size() < 1) {
      printHelp(out, optParser);
      return 0;
    }

    for (final String fileOrStdin : (List<String>) opts.nonOptionArguments()) {
      final InputStream input = Util.fileOrStdin(fileOrStdin, in);
      try {
        final Schema schema = parser.parse(input);
        final byte[] fingerprint = SchemaNormalization.parsingFingerprint(opts.valueOf(fingerprintOpt), schema);
        out.format("%s %s%n", Util.encodeHex(fingerprint), fileOrStdin);
      } finally {
        Util.close(input);
      }
    }

    return 0;
  }

  @Override
  public String getName() {
    return "fingerprint";
  }

  @Override
  public String getShortDescription() {
    return "Returns the fingerprint for the schemas.";
  }

  private void printHelp(PrintStream out, OptionParser optParser) throws IOException {
    out.println("fingerprint [--fingerprint <fingerprint>] input-file [inputfile [inputfile...]]");
    out.println();
    out.println("generates fingerprints based on Avro specification.");
    optParser.printHelpOn(out);
    out.println("A dash ('-') can be given to read a schema from stdin");
  }
}
