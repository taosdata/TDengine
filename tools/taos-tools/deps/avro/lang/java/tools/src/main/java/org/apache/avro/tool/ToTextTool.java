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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

/** Reads an avro data file into a plain text file. */
public class ToTextTool implements Tool {
  private static final String TEXT_FILE_SCHEMA = "\"bytes\"";
  private static final byte[] LINE_SEPARATOR = System.getProperty("line.separator").getBytes(StandardCharsets.UTF_8);

  @Override
  public String getName() {
    return "totext";
  }

  @Override
  public String getShortDescription() {
    return "Converts an Avro data file to a text file.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {

    OptionParser p = new OptionParser();
    OptionSet opts = p.parse(args.toArray(new String[0]));
    if (opts.nonOptionArguments().size() != 2) {
      err.println("Expected 2 args: from_file to_file (filenames or '-' for stdin/stdout");
      p.printHelpOn(err);
      return 1;
    }

    BufferedInputStream inStream = Util.fileOrStdin(args.get(0), stdin);
    BufferedOutputStream outStream = Util.fileOrStdout(args.get(1), out);

    GenericDatumReader<Object> reader = new GenericDatumReader<>();
    DataFileStream<Object> fileReader = new DataFileStream<>(inStream, reader);

    if (!fileReader.getSchema().equals(new Schema.Parser().parse(TEXT_FILE_SCHEMA))) {
      err.println("Avro file is not generic text schema");
      p.printHelpOn(err);
      fileReader.close();
      return 1;
    }

    while (fileReader.hasNext()) {
      ByteBuffer outBuff = (ByteBuffer) fileReader.next();
      outStream.write(outBuff.array());
      outStream.write(LINE_SEPARATOR);
    }
    fileReader.close();
    Util.close(inStream);
    Util.close(outStream);
    return 0;
  }

}
