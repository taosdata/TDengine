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
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.util.RandomData;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

/** Tool to create randomly populated Trevni file based on an Avro schema */
public class TrevniCreateRandomTool implements Tool {

  @Override
  public String getName() {
    return "trevni_random";
  }

  @Override
  public String getShortDescription() {
    return "Create a Trevni file filled with random instances of a schema.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.size() != 3) {
      err.println("Usage: schemaFile count outputFile");
      return 1;
    }

    File schemaFile = new File(args.get(0));
    int count = Integer.parseInt(args.get(1));
    File outputFile = new File(args.get(2));

    Schema schema = new Schema.Parser().parse(schemaFile);

    AvroColumnWriter<Object> writer = new AvroColumnWriter<>(schema, new ColumnFileMetaData());

    for (Object datum : new RandomData(schema, count))
      writer.write(datum);

    writer.writeTo(outputFile);

    return 0;
  }
}
