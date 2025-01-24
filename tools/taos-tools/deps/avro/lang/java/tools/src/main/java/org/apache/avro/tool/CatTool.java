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

import java.io.IOException;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

/** Tool to extract samples from an Avro data file. */
public class CatTool implements Tool {

  private long totalCopied;
  private double sampleCounter;

  private GenericRecord reuse;
  private DataFileStream<GenericRecord> reader;
  private DataFileWriter<GenericRecord> writer;
  private Schema schema;
  private List<Path> inFiles;
  private int currentInput;

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser optParser = new OptionParser();
    OptionSpec<Long> offsetOpt = optParser.accepts("offset", "offset for reading input").withRequiredArg()
        .ofType(Long.class).defaultsTo(Long.valueOf(0));
    OptionSpec<Long> limitOpt = optParser.accepts("limit", "maximum number of records in the outputfile")
        .withRequiredArg().ofType(Long.class).defaultsTo(Long.MAX_VALUE);
    OptionSpec<Double> fracOpt = optParser.accepts("samplerate", "rate at which records will be collected")
        .withRequiredArg().ofType(Double.class).defaultsTo(Double.valueOf(1));

    OptionSet opts = optParser.parse(args.toArray(new String[0]));
    List<String> nargs = (List<String>) opts.nonOptionArguments();
    if (nargs.size() < 2) {
      printHelp(out);
      return 0;
    }

    inFiles = Util.getFiles(nargs.subList(0, nargs.size() - 1));

    System.out.println("List of input files:");
    for (Path p : inFiles) {
      System.out.println(p);
    }
    currentInput = -1;
    nextInput();

    OutputStream output = out;
    String lastArg = nargs.get(nargs.size() - 1);
    if (nargs.size() > 1 && !lastArg.equals("-")) {
      output = Util.createFromFS(lastArg);
    }
    writer = new DataFileWriter<>(new GenericDatumWriter<>());

    String codecName = reader.getMetaString(DataFileConstants.CODEC);
    CodecFactory codec = (codecName == null) ? CodecFactory.fromString(DataFileConstants.NULL_CODEC)
        : CodecFactory.fromString(codecName);
    writer.setCodec(codec);
    for (String key : reader.getMetaKeys()) {
      if (!DataFileWriter.isReservedMeta(key)) {
        writer.setMeta(key, reader.getMeta(key));
      }
    }
    writer.create(schema, output);

    long offset = opts.valueOf(offsetOpt);
    long limit = opts.valueOf(limitOpt);
    double samplerate = opts.valueOf(fracOpt);
    sampleCounter = 1;
    totalCopied = 0;
    reuse = null;

    if (limit < 0) {
      System.out.println("limit has to be non-negative");
      this.printHelp(out);
      return 1;
    }
    if (offset < 0) {
      System.out.println("offset has to be non-negative");
      this.printHelp(out);
      return 1;
    }
    if (samplerate < 0 || samplerate > 1) {
      System.out.println("samplerate has to be a number between 0 and 1");
      this.printHelp(out);
      return 1;
    }

    skip(offset);
    writeRecords(limit, samplerate);
    System.out.println(totalCopied + " records written.");

    writer.flush();
    writer.close();
    Util.close(out);
    return 0;
  }

  private void nextInput() throws IOException {
    currentInput++;
    Path path = inFiles.get(currentInput);
    FSDataInputStream input = new FSDataInputStream(Util.openFromFS(path));
    reader = new DataFileStream<>(input, new GenericDatumReader<>());
    if (schema == null) { // if this is the first file, the schema gets saved
      schema = reader.getSchema();
    } else if (!schema.equals(reader.getSchema())) { // subsequent files have to have equal schemas
      throw new IOException("schemas dont match");
    }
  }

  private boolean hasNextInput() {
    return inFiles.size() > (currentInput + 1);
  }

  /** skips a number of records from the input */
  private long skip(long skip) throws IOException {
    long skipped = 0;
    while (0 < skip && reader.hasNext()) {
      reader.next(reuse);
      skip--;
      skipped++;
    }
    if ((0 < skip) && hasNextInput()) { // goto next file
      nextInput();
      skipped = skipped + skip(skip);
    }
    return skipped;
  }

  /**
   * writes records with the given samplerate The record at position offset is
   * guaranteed to be taken
   */
  private long writeRecords(long count, double samplerate) throws IOException {
    long written = 0;
    while (written < count && reader.hasNext()) {
      reuse = reader.next(reuse);
      sampleCounter = sampleCounter + samplerate;
      if (sampleCounter >= 1) {
        writer.append(reuse);
        written++;
        sampleCounter--;
      }
    }
    totalCopied = totalCopied + written;
    if (written < count && hasNextInput()) { // goto next file
      nextInput();
      written = written + writeRecords(count - written, samplerate);
    }
    return written;
  }

  private void printHelp(PrintStream out) {
    out.println("cat --offset <offset> --limit <limit> --samplerate <samplerate> [input-files...] output-file");
    out.println();
    out.println("extracts records from a list of input files into a new file.");
    out.println("--offset      start of the extract");
    out.println("--limit       maximum number of records in the output file.");
    out.println("--samplerate  rate at which records will be collected");
    out.println("A dash ('-') can be given to direct output to stdout");
  }

  @Override
  public String getName() {
    return "cat";
  }

  @Override
  public String getShortDescription() {
    return "Extracts samples from files";
  }

}
