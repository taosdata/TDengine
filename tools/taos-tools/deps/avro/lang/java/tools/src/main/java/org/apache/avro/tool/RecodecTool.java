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

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/** Tool to alter the codec of an Avro data file. */
public class RecodecTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {

    OptionParser optParser = new OptionParser();
    OptionSpec<String> codecOpt = Util.compressionCodecOptionWithDefault(optParser, DataFileConstants.NULL_CODEC);
    OptionSpec<Integer> levelOpt = Util.compressionLevelOption(optParser);
    OptionSet opts = optParser.parse(args.toArray(new String[0]));

    List<String> nargs = (List<String>) opts.nonOptionArguments();
    if (nargs.size() > 2) {
      err.println("Expected at most an input file and output file.");
      optParser.printHelpOn(err);
      return 1;
    }
    InputStream input = in;
    boolean inputNeedsClosing = false;
    if (nargs.size() > 0 && !nargs.get(0).equals("-")) {
      input = Util.openFromFS(nargs.get(0));
      inputNeedsClosing = true;
    }
    OutputStream output = out;
    boolean outputNeedsClosing = false;
    if (nargs.size() > 1 && !nargs.get(1).equals("-")) {
      output = Util.createFromFS(nargs.get(1));
      outputNeedsClosing = true;
    }

    DataFileStream<GenericRecord> reader = new DataFileStream<>(input, new GenericDatumReader<>());
    Schema schema = reader.getSchema();
    DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>());
    // unlike the other Avro tools, we default to a null codec, not deflate
    CodecFactory codec = Util.codecFactory(opts, codecOpt, levelOpt, DataFileConstants.NULL_CODEC);
    writer.setCodec(codec);
    for (String key : reader.getMetaKeys()) {
      if (!DataFileWriter.isReservedMeta(key)) {
        writer.setMeta(key, reader.getMeta(key));
      }
    }
    writer.create(schema, output);

    writer.appendAllFrom(reader, true);
    writer.flush();

    if (inputNeedsClosing) {
      input.close();
    }
    if (outputNeedsClosing) {
      output.close();
    }
    writer.close();
    return 0;
  }

  @Override
  public String getName() {
    return "recodec";
  }

  @Override
  public String getShortDescription() {
    return "Alters the codec of a data file.";
  }
}
