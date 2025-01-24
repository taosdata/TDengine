/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.tool;

import com.google.common.collect.ImmutableList;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/** Counts the records in avro files or folders */
public class RecordCountTool implements Tool {

  @Override
  public String getName() {
    return "count";
  }

  @Override
  public String getShortDescription() {
    return "Counts the records in avro files or folders";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser optionParser = new OptionParser();
    OptionSet optionSet = optionParser.parse(args.toArray(new String[0]));
    List<String> nargs = (List<String>) optionSet.nonOptionArguments();

    if (nargs.isEmpty()) {
      printHelp(err);
      err.println();
      optionParser.printHelpOn(err);
      return 0;
    }

    long count = 0L;
    if (ImmutableList.of("-").equals(nargs)) {
      count = countRecords(stdin);
    } else {
      for (Path file : Util.getFiles(nargs)) {
        try (final InputStream inStream = Util.openFromFS(file)) {
          count += countRecords(inStream);
        }
      }
    }
    out.println(count);
    out.flush();
    return 0;
  }

  private long countRecords(InputStream inStream) throws java.io.IOException {
    long count = 0L;
    try (DataFileStream<Object> streamReader = new DataFileStream<>(inStream, new GenericDatumReader<>())) {
      while (streamReader.hasNext()) {
        count = count + streamReader.getBlockCount();
        streamReader.nextBlock();
      }
    }
    return count;
  }

  private void printHelp(PrintStream ps) {
    ps.println(getName() + " [input-files...]");
    ps.println();
    ps.println(getShortDescription());
    ps.println("A dash ('-') can be given as an input-file to use stdin");
  }
}
