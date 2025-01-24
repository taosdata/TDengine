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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/** Reads an Avro data file and writes a Trevni file. */
public class ToTrevniTool implements Tool {

  @Override
  public String getName() {
    return "totrevni";
  }

  @Override
  public String getShortDescription() {
    return "Converts an Avro data file to a Trevni file.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {

    OptionParser p = new OptionParser();
    OptionSpec<String> codec = p.accepts("codec", "Compression codec").withRequiredArg().defaultsTo("null")
        .ofType(String.class);
    OptionSet opts = p.parse(args.toArray(new String[0]));
    if (opts.nonOptionArguments().size() != 2) {
      err.println("Usage: inFile outFile (filenames or '-' for stdin/stdout)");
      p.printHelpOn(err);
      return 1;
    }
    args = (List<String>) opts.nonOptionArguments();

    DataFileStream<Object> reader = new DataFileStream(Util.fileOrStdin(args.get(0), stdin),
        new GenericDatumReader<>());
    OutputStream outs = Util.fileOrStdout(args.get(1), out);
    AvroColumnWriter<Object> writer = new AvroColumnWriter<>(reader.getSchema(),
        new ColumnFileMetaData().setCodec(codec.value(opts)));
    for (Object datum : reader)
      writer.write(datum);
    writer.writeTo(outs);
    outs.close();
    reader.close();
    return 0;
  }

}
