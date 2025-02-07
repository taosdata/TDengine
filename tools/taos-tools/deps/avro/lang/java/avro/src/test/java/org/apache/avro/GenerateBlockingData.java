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
package org.apache.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.RandomData;

/**
 * Generates file with objects of a specific schema(that doesn't contain nesting
 * of arrays and maps) with random data. This is only for testing. Generated
 * file contains the count of objects of the specified schema followed by
 * objects serialized using BlockingBinaryEncoder. No other metadata is written
 * to the file. See interoptests.py for more details(interoptests.py reads the
 * file generated here and validates the contents).
 */
public class GenerateBlockingData {
  private static final int SYNC_INTERVAL = 1000;
  private static ByteArrayOutputStream buffer = new ByteArrayOutputStream(2 * SYNC_INTERVAL);

  private static EncoderFactory factory = EncoderFactory.get();
  private static Encoder bufOut = EncoderFactory.get().blockingBinaryEncoder(buffer, null);
  private static int blockCount;

  private static void writeBlock(Encoder vout, FileOutputStream out) throws IOException {
    vout.writeLong(blockCount);
    bufOut.flush();
    buffer.writeTo(out);
    buffer.reset();
    blockCount = 0;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("Usage: GenerateBlockingData <schemafile> <outputfile> <count>");
      System.exit(-1);
    }

    Schema sch = new Schema.Parser().parse(new File(args[0]));
    File outputFile = new File(args[1]);
    int numObjects = Integer.parseInt(args[2]);

    FileOutputStream out = new FileOutputStream(outputFile, false);
    DatumWriter<Object> dout = new GenericDatumWriter<>();
    dout.setSchema(sch);
    Encoder vout = factory.directBinaryEncoder(out, null);
    vout.writeLong(numObjects); // metadata:the count of objects in the file

    for (Object datum : new RandomData(sch, numObjects)) {
      dout.write(datum, bufOut);
      blockCount++;
      if (buffer.size() >= SYNC_INTERVAL) {
        writeBlock(vout, out);
      }
    }
    if (blockCount > 0) {
      writeBlock(vout, out);
    }
    out.flush();
    out.close();
  }
}
