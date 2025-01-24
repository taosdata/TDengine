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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

/** Converts an input file from Avro binary into JSON. */
public class BinaryFragmentToJsonTool implements Tool {
  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser optionParser = new OptionParser();
    OptionSpec<Void> noPrettyOption = optionParser.accepts("no-pretty", "Turns off pretty printing.");
    OptionSpec<String> schemaFileOption = optionParser
        .accepts("schema-file", "File containing schema, must not occur with inline schema.").withOptionalArg()
        .ofType(String.class);

    OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
    Boolean noPretty = optionSet.has(noPrettyOption);
    List<String> nargs = (List<String>) optionSet.nonOptionArguments();
    String schemaFile = schemaFileOption.value(optionSet);

    if (nargs.size() != (schemaFile == null ? 2 : 1)) {
      err.println("fragtojson --no-pretty --schema-file <file> [inline-schema] input-file");
      err.println("   converts Avro fragments to JSON.");
      optionParser.printHelpOn(err);
      err.println("   A dash '-' for input-file means stdin.");
      return 1;
    }
    Schema schema;
    String inputFile;
    if (schemaFile == null) {
      schema = new Schema.Parser().parse(nargs.get(0));
      inputFile = nargs.get(1);
    } else {
      schema = Util.parseSchemaFromFS(schemaFile);
      inputFile = nargs.get(0);
    }
    InputStream input = Util.fileOrStdin(inputFile, stdin);

    try {
      DatumReader<Object> reader = new GenericDatumReader<>(schema);
      BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(input, null);
      DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, out, !noPretty);
      Object datum = null;
      while (!binaryDecoder.isEnd()) {
        datum = reader.read(datum, binaryDecoder);
        writer.write(datum, jsonEncoder);
        jsonEncoder.flush();
      }
      out.println();
      out.flush();
    } finally {
      Util.close(input);
    }
    return 0;
  }

  @Override
  public String getName() {
    return "fragtojson";
  }

  @Override
  public String getShortDescription() {
    return "Renders a binary-encoded Avro datum as JSON.";
  }
}
