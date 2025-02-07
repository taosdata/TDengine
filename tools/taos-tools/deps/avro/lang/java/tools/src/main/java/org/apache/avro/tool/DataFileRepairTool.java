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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

/** Recovers data from a corrupt Avro Data file */
public class DataFileRepairTool implements Tool {

  @Override
  public String getName() {
    return "repair";
  }

  @Override
  public String getShortDescription() {
    return "Recovers data from a corrupt Avro Data file";
  }

  private void printInfo(PrintStream output) {
    output.println("Insufficient arguments.  Arguments:  [-o option] " + "input_file output_file \n"
        + "   Where option is one of the following: \n" + "      " + ALL
        + " (default) recover as many records as possible.\n" + "      " + PRIOR
        + "         recover only records prior to the first instance" + " of corruption \n" + "      " + AFTER
        + "         recover only records after the first instance of" + " corruption.\n" + "      " + REPORT
        + "        print the corruption report only, reporting the\n"
        + "                    number of valid and corrupted blocks and records\n"
        + "   input_file is the file to read from.  output_file is the file to\n"
        + "   create and write recovered data to.  output_file is ignored if\n" + "   using the report option.");
  }

  private static final Set<String> OPTIONS = new HashSet<>();
  private static final String ALL = "all";
  private static final String PRIOR = "prior";
  private static final String AFTER = "after";
  private static final String REPORT = "report";
  static {
    OPTIONS.add(ALL);
    OPTIONS.add(PRIOR);
    OPTIONS.add(AFTER);
    OPTIONS.add(REPORT);
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.size() < 2) {
      printInfo(err);
      return 1;
    }
    int index = 0;
    String input = args.get(index);
    String option = "all";
    if ("-o".equals(input)) {
      option = args.get(1);
      index += 2;
    }
    if (!OPTIONS.contains(option) || (args.size() - index < 1)) {
      printInfo(err);
      return 1;
    }
    input = args.get(index++);
    if (!REPORT.equals(option)) {
      if (args.size() - index < 1) {
        printInfo(err);
        return 1;
      }
    }
    if (ALL.equals(option)) {
      return recoverAll(input, args.get(index), out, err);
    } else if (PRIOR.equals(option)) {
      return recoverPrior(input, args.get(index), out, err);
    } else if (AFTER.equals(option)) {
      return recoverAfter(input, args.get(index), out, err);
    } else if (REPORT.equals(option)) {
      return reportOnly(input, out, err);
    } else {
      return 1;
    }
  }

  private int recover(String input, String output, PrintStream out, PrintStream err, boolean recoverPrior,
      boolean recoverAfter) throws IOException {
    File infile = new File(input);
    if (!infile.canRead()) {
      err.println("cannot read file: " + input);
      return 1;
    }
    out.println("Recovering file: " + input);
    GenericDatumReader<Object> reader = new GenericDatumReader<>();
    try (DataFileReader<Object> fileReader = new DataFileReader<>(infile, reader)) {
      Schema schema = fileReader.getSchema();
      String codecStr = fileReader.getMetaString(DataFileConstants.CODEC);
      CodecFactory codecFactory = CodecFactory.fromString("" + codecStr);
      List<String> metas = fileReader.getMetaKeys();
      if (recoverPrior || recoverAfter) {
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>();
        DataFileWriter<Object> fileWriter = new DataFileWriter<>(writer);
        try {
          File outfile = new File(output);
          for (String key : metas) {
            if (!key.startsWith("avro.")) {
              byte[] val = fileReader.getMeta(key);
              fileWriter.setMeta(key, val);
            }
          }
          fileWriter.setCodec(codecFactory);
          int result = innerRecover(fileReader, fileWriter, out, err, recoverPrior, recoverAfter, schema, outfile);
          return result;
        } catch (Exception e) {
          e.printStackTrace(err);
          return 1;
        }
      } else {
        return innerRecover(fileReader, null, out, err, recoverPrior, recoverAfter, null, null);
      }

    }
  }

  private int innerRecover(DataFileReader<Object> fileReader, DataFileWriter<Object> fileWriter, PrintStream out,
      PrintStream err, boolean recoverPrior, boolean recoverAfter, Schema schema, File outfile) {
    int numBlocks = 0;
    int numCorruptBlocks = 0;
    int numRecords = 0;
    int numCorruptRecords = 0;
    int recordsWritten = 0;
    long position = fileReader.previousSync();
    long blockSize = 0;
    long blockCount = 0;
    boolean fileWritten = false;
    try {
      while (true) {
        try {
          if (!fileReader.hasNext()) {
            out.println("File Summary: ");
            out.println("  Number of blocks: " + numBlocks + " Number of corrupt blocks: " + numCorruptBlocks);
            out.println("  Number of records: " + numRecords + " Number of corrupt records: " + numCorruptRecords);
            if (recoverAfter || recoverPrior) {
              out.println("  Number of records written " + recordsWritten);
            }
            out.println();
            return 0;
          }
          position = fileReader.previousSync();
          blockCount = fileReader.getBlockCount();
          blockSize = fileReader.getBlockSize();
          numRecords += blockCount;
          long blockRemaining = blockCount;
          numBlocks++;
          boolean lastRecordWasBad = false;
          long badRecordsInBlock = 0;
          while (blockRemaining > 0) {
            try {
              Object datum = fileReader.next();
              if ((recoverPrior && numCorruptBlocks == 0) || (recoverAfter && numCorruptBlocks > 0)) {
                if (!fileWritten) {
                  try {
                    fileWriter.create(schema, outfile);
                    fileWritten = true;
                  } catch (Exception e) {
                    e.printStackTrace(err);
                    return 1;
                  }
                }
                try {
                  fileWriter.append(datum);
                  recordsWritten++;
                } catch (Exception e) {
                  e.printStackTrace(err);
                  throw e;
                }
              }
              blockRemaining--;
              lastRecordWasBad = false;
            } catch (Exception e) {
              long pos = blockCount - blockRemaining;
              if (badRecordsInBlock == 0) {
                // first corrupt record
                numCorruptBlocks++;
                err.println("Corrupt block: " + numBlocks + " Records in block: " + blockCount
                    + " uncompressed block size: " + blockSize);
                err.println("Corrupt record at position: " + (pos));
              } else {
                // second bad record in block, if consecutive skip block.
                err.println("Corrupt record at position: " + (pos));
                if (lastRecordWasBad) {
                  // consecutive bad record
                  err.println(
                      "Second consecutive bad record in block: " + numBlocks + ". Skipping remainder of block. ");
                  numCorruptRecords += blockRemaining;
                  badRecordsInBlock += blockRemaining;
                  try {
                    fileReader.sync(position);
                  } catch (Exception e2) {
                    err.println("failed to sync to sync marker, aborting");
                    e2.printStackTrace(err);
                    return 1;
                  }
                  break;
                }
              }
              blockRemaining--;
              lastRecordWasBad = true;
              numCorruptRecords++;
              badRecordsInBlock++;
            }
          }
          if (badRecordsInBlock != 0) {
            err.println("** Number of unrecoverable records in block: " + (badRecordsInBlock));
          }
          position = fileReader.previousSync();
        } catch (Exception e) {
          err.println("Failed to read block " + numBlocks + ". Unknown record " + "count in block.  Skipping. Reason: "
              + e.getMessage());
          numCorruptBlocks++;
          try {
            fileReader.sync(position);
          } catch (Exception e2) {
            err.println("failed to sync to sync marker, aborting");
            e2.printStackTrace(err);
            return 1;
          }
        }
      }
    } finally {
      if (fileWritten) {
        try {
          fileWriter.close();
        } catch (Exception e) {
          e.printStackTrace(err);
          return 1;
        }
      }
    }
  }

  private int reportOnly(String input, PrintStream out, PrintStream err) throws IOException {
    return recover(input, null, out, err, false, false);
  }

  private int recoverAfter(String input, String output, PrintStream out, PrintStream err) throws IOException {
    return recover(input, output, out, err, false, true);
  }

  private int recoverPrior(String input, String output, PrintStream out, PrintStream err) throws IOException {
    return recover(input, output, out, err, true, false);
  }

  private int recoverAll(String input, String output, PrintStream out, PrintStream err) throws IOException {
    return recover(input, output, out, err, true, true);
  }
}
