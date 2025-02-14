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
import java.util.List;

import org.apache.trevni.Input;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnValues;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

/**
 * Tool to read Trevni files and print them as JSON. This can read any Trevni
 * file. Nested structure is reconstructed from the columns rather than any
 * schema information.
 */
public class TrevniToJsonTool implements Tool {
  static final JsonFactory FACTORY = new JsonFactory();

  private JsonGenerator generator;
  private ColumnValues[] values;
  private String[] shortNames;

  @Override
  public String getName() {
    return "trevni_tojson";
  }

  @Override
  public String getShortDescription() {
    return "Dumps a Trevni file as JSON.";
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

    toJson(TrevniUtil.input(filename), out, pretty);

    return 0;
  }

  /** Read a Trevni file and print each row as a JSON object. */
  public void toJson(Input input, PrintStream out, boolean pretty) throws IOException {
    this.generator = FACTORY.createGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      generator.useDefaultPrettyPrinter();
    } else { // ensure newline separation
      MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
      pp.setRootValueSeparator(System.getProperty("line.separator"));
      generator.setPrettyPrinter(pp);
    }

    ColumnFileReader reader = new ColumnFileReader(input);

    int columnCount = (int) reader.getColumnCount();
    this.values = new ColumnValues[columnCount];
    this.shortNames = new String[columnCount];
    for (int i = 0; i < columnCount; i++) {
      values[i] = reader.getValues(i);
      shortNames[i] = shortName(reader.getColumnMetaData(i));
    }

    List<ColumnMetaData> roots = reader.getRoots();
    for (long row = 0; row < reader.getRowCount(); row++) {
      for (ColumnValues v : values)
        v.startRow();
      generator.writeStartObject();
      for (ColumnMetaData root : roots)
        valueToJson(root);
      generator.writeEndObject();
    }
    generator.flush();
    out.println();
    reader.close();
  }

  private void valueToJson(ColumnMetaData column) throws IOException {
    generator.writeFieldName(shortNames[column.getNumber()]);
    ColumnValues in = values[column.getNumber()];
    if (!column.isArray()) {
      primitiveToJson(column, in.nextValue());
    } else {
      generator.writeStartArray();
      int length = in.nextLength();
      for (int i = 0; i < length; i++) {
        Object value = in.nextValue();
        List<ColumnMetaData> children = column.getChildren();
        if (children.size() == 0) {
          primitiveToJson(column, value);
        } else {
          generator.writeStartObject();
          if (value != null) {
            generator.writeFieldName("value$");
            primitiveToJson(column, value);
          }
          for (ColumnMetaData child : children)
            valueToJson(child);
          generator.writeEndObject();
        }
      }
      generator.writeEndArray();
    }
  }

  private void primitiveToJson(ColumnMetaData column, Object value) throws IOException {
    switch (column.getType()) {
    case NULL:
      generator.writeNull();
      break;
    case BOOLEAN:
      generator.writeBoolean((Boolean) value);
      break;
    case INT:
      generator.writeNumber((Integer) value);
      break;
    case LONG:
      generator.writeNumber((Long) value);
      break;
    case FIXED32:
      generator.writeNumber((Integer) value);
      break;
    case FIXED64:
      generator.writeNumber((Long) value);
      break;
    case FLOAT:
      generator.writeNumber((Float) value);
      break;
    case DOUBLE:
      generator.writeNumber((Double) value);
      break;
    case STRING:
      generator.writeString((String) value);
      break;
    case BYTES:
      generator.writeBinary((byte[]) value);
      break;
    default:
      throw new RuntimeException("Unknown value type: " + column.getType());
    }
  }

  // trim off portion of name shared with parent
  private String shortName(ColumnMetaData column) {
    String name = column.getName();
    ColumnMetaData parent = column.getParent();
    if (parent != null && name.startsWith(parent.getName()))
      name = name.substring(parent.getName().length());
    if (!Character.isLetterOrDigit(name.charAt(0)))
      name = name.substring(1);
    return name;
  }

}
