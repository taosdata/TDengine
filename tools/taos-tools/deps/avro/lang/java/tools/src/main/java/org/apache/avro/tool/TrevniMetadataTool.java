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
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.trevni.Input;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.MetaData;
import org.apache.trevni.ColumnMetaData;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

/** Tool to print Trevni file metadata as JSON. */
public class TrevniMetadataTool implements Tool {
  static final JsonFactory FACTORY = new JsonFactory();

  private JsonGenerator generator;

  @Override
  public String getName() {
    return "trevni_meta";
  }

  @Override
  public String getShortDescription() {
    return "Dumps a Trevni file's metadata as JSON.";
  }

  @Override
  public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {
    String filename;
    boolean pretty = false;
    if (args.size() == 2 && "-pretty".equals(args.get(0))) {
      pretty = true;
      filename = args.get(1);
    } else if (args.size() == 1) {
      filename = args.get(0);
    } else {
      err.println("Usage: [-pretty] input");
      return 1;
    }

    dump(TrevniUtil.input(filename), out, pretty);

    return 0;
  }

  /** Read a Trevni file and print each row as a JSON object. */
  public void dump(Input input, PrintStream out, boolean pretty) throws IOException {
    this.generator = FACTORY.createGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      generator.useDefaultPrettyPrinter();
    } else { // ensure newline separation
      MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
      pp.setRootValueSeparator(System.getProperty("line.separator"));
      generator.setPrettyPrinter(pp);
    }

    ColumnFileReader reader = new ColumnFileReader(input);

    generator.writeStartObject();
    generator.writeNumberField("rowCount", reader.getRowCount());
    generator.writeNumberField("columnCount", reader.getColumnCount());

    generator.writeFieldName("metadata");
    dump(reader.getMetaData());

    generator.writeFieldName("columns");
    generator.writeStartArray();
    for (ColumnMetaData c : reader.getColumnMetaData())
      dump(c);
    generator.writeEndArray();

    generator.writeEndObject();

    generator.flush();
    out.println();
    reader.close();
  }

  private void dump(MetaData<?> meta) throws IOException {
    generator.writeStartObject();
    for (Map.Entry<String, byte[]> e : meta.entrySet())
      generator.writeStringField(e.getKey(), new String(e.getValue(), StandardCharsets.ISO_8859_1));
    generator.writeEndObject();
  }

}
