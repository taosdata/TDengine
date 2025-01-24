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

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.FsInput;

/** Reads a data file to get its metadata. */
public class DataFileGetMetaTool implements Tool {

  @Override
  public String getName() {
    return "getmeta";
  }

  @Override
  public String getShortDescription() {
    return "Prints out the metadata of an Avro data file.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {

    OptionParser p = new OptionParser();
    OptionSpec<String> keyOption = p.accepts("key", "Metadata key").withOptionalArg().ofType(String.class);
    OptionSet opts = p.parse(args.toArray(new String[0]));
    String keyName = keyOption.value(opts);

    List<String> nargs = (List<String>) opts.nonOptionArguments();
    if (nargs.size() != 1) {
      err.println("Expected 1 arg: input_file");
      p.printHelpOn(err);
      return 1;
    }
    FsInput in = Util.openSeekableFromFS(args.get(0));
    DataFileReader<Void> reader = new DataFileReader<>(in, new GenericDatumReader<>());
    if (keyName != null) {
      byte[] value = reader.getMeta(keyName);
      if (value != null) {
        out.write(value, 0, value.length);
        out.println();
      }
    } else {
      List<String> keys = reader.getMetaKeys();
      for (String key : keys) {
        out.print(escapeKey(key));
        out.print('\t');
        byte[] value = reader.getMeta(key);
        out.write(value, 0, value.length);
        out.println();
      }
    }
    reader.close();
    return 0;
  }

  // escape TAB, NL and CR in keys, so that output can be reliably parsed
  static String escapeKey(String key) {
    key = key.replace("\\", "\\\\"); // escape backslashes first
    key = key.replace("\t", "\\t"); // TAB
    key = key.replace("\n", "\\n"); // NL
    key = key.replace("\r", "\\r"); // CR
    return key;
  }

}
