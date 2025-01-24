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

package org.apache.trevni.avro;

import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnValues;
import org.apache.trevni.Input;
import org.apache.trevni.InputFile;
import org.apache.trevni.TrevniRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;

import static org.apache.trevni.avro.AvroColumnator.isSimple;

/**
 * Read files written with {@link AvroColumnWriter}. A subset of the schema used
 * for writing may be specified when reading. In this case only columns of the
 * subset schema are read.
 */
public class AvroColumnReader<D> implements Iterator<D>, Iterable<D>, Closeable {

  private ColumnFileReader reader;
  private GenericData model;
  private Schema fileSchema;
  private Schema readSchema;

  private ColumnValues[] values;
  private int[] arrayWidths;
  private int column; // current index in values

  private Map<String, Map<String, Object>> defaults = new HashMap<>();

  /** Parameters for reading an Avro column file. */
  public static class Params {
    Input input;
    Schema schema;
    GenericData model = GenericData.get();

    /** Construct reading from a file. */
    public Params(File file) throws IOException {
      this(new InputFile(file));
    }

    /** Construct reading from input. */
    public Params(Input input) {
      this.input = input;
    }

    /** Set subset schema to project data down to. */
    public Params setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /** Set data representation. */
    public Params setModel(GenericData model) {
      this.model = model;
      return this;
    }
  }

  /** Construct a reader for a file. */
  public AvroColumnReader(Params params) throws IOException {
    this.reader = new ColumnFileReader(params.input);
    this.model = params.model;
    this.fileSchema = new Schema.Parser().parse(reader.getMetaData().getString(AvroColumnWriter.SCHEMA_KEY));
    this.readSchema = params.schema == null ? fileSchema : params.schema;
    initialize();
  }

  /** Return the schema for data in this file. */
  public Schema getFileSchema() {
    return fileSchema;
  }

  void initialize() throws IOException {
    // compute a mapping from column name to number for file
    Map<String, Integer> fileColumnNumbers = new HashMap<>();
    int i = 0;
    for (ColumnMetaData c : new AvroColumnator(fileSchema).getColumns())
      fileColumnNumbers.put(c.getName(), i++);

    // create iterator for each column in readSchema
    AvroColumnator readColumnator = new AvroColumnator(readSchema);
    this.arrayWidths = readColumnator.getArrayWidths();
    ColumnMetaData[] readColumns = readColumnator.getColumns();
    this.values = new ColumnValues[readColumns.length];
    int j = 0;
    for (ColumnMetaData c : readColumns) {
      Integer n = fileColumnNumbers.get(c.getName());
      if (n != null)
        values[j++] = reader.getValues(n);
    }
    findDefaults(readSchema, fileSchema);
  }

  // get defaults for fields in read that are not in write
  private void findDefaults(Schema read, Schema write) {
    switch (read.getType()) {
    case NULL:
    case BOOLEAN:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case BYTES:
    case STRING:
    case ENUM:
    case FIXED:
      if (read.getType() != write.getType())
        throw new TrevniRuntimeException("Type mismatch: " + read + " & " + write);
      break;
    case MAP:
      findDefaults(read.getValueType(), write.getValueType());
      break;
    case ARRAY:
      findDefaults(read.getElementType(), write.getElementType());
      break;
    case UNION:
      for (Schema s : read.getTypes()) {
        Integer index = write.getIndexNamed(s.getFullName());
        if (index == null)
          throw new TrevniRuntimeException("No matching branch: " + s);
        findDefaults(s, write.getTypes().get(index));
      }
      break;
    case RECORD:
      for (Field f : read.getFields()) {
        Field g = write.getField(f.name());
        if (g == null)
          setDefault(read, f);
        else
          findDefaults(f.schema(), g.schema());
      }
      break;
    default:
      throw new TrevniRuntimeException("Unknown schema: " + read);
    }
  }

  private void setDefault(Schema record, Field f) {
    String recordName = record.getFullName();
    Map<String, Object> recordDefaults = defaults.computeIfAbsent(recordName, k -> new HashMap<>());
    recordDefaults.put(f.name(), model.getDefaultValue(f));
  }

  @Override
  public Iterator<D> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return values[0].hasNext();
  }

  /** Return the number of rows in this file. */
  public long getRowCount() {
    return reader.getRowCount();
  }

  @Override
  public D next() {
    try {
      for (ColumnValues value : values)
        if (value != null)
          value.startRow();
      this.column = 0;
      return (D) read(readSchema);
    } catch (IOException e) {
      throw new TrevniRuntimeException(e);
    }
  }

  private Object read(Schema s) throws IOException {
    if (isSimple(s))
      return nextValue(s, column++);

    final int startColumn = column;

    switch (s.getType()) {
    case MAP:
      int size = values[column].nextLength();
      Map map = new HashMap(size);
      for (int i = 0; i < size; i++) {
        this.column = startColumn;
        values[column++].nextValue(); // null in parent
        String key = (String) values[column++].nextValue(); // key
        map.put(key, read(s.getValueType())); // value
      }
      column = startColumn + arrayWidths[startColumn];
      return map;
    case RECORD:
      Object record = model.newRecord(null, s);
      Map<String, Object> rDefaults = defaults.get(s.getFullName());
      for (Field f : s.getFields()) {
        Object value = ((rDefaults != null) && rDefaults.containsKey(f.name()))
            ? model.deepCopy(f.schema(), rDefaults.get(f.name()))
            : read(f.schema());
        model.setField(record, f.name(), f.pos(), value);
      }
      return record;
    case ARRAY:
      int length = values[column].nextLength();
      List elements = new GenericData.Array(length, s);
      for (int i = 0; i < length; i++) {
        this.column = startColumn;
        Object value = nextValue(s, column++);
        if (!isSimple(s.getElementType()))
          value = read(s.getElementType());
        elements.add(value);
      }
      column = startColumn + arrayWidths[startColumn];
      return elements;
    case UNION:
      Object value = null;
      for (Schema branch : s.getTypes()) {
        if (branch.getType() == Schema.Type.NULL)
          continue;
        if (values[column].nextLength() == 1) {
          value = nextValue(branch, column);
          column++;
          if (!isSimple(branch))
            value = read(branch);
        } else {
          column += arrayWidths[column];
        }
      }
      return value;
    default:
      throw new TrevniRuntimeException("Unknown schema: " + s);
    }
  }

  private Object nextValue(Schema s, int column) throws IOException {
    Object v = values[column].nextValue();

    switch (s.getType()) {
    case ENUM:
      return model.createEnum(s.getEnumSymbols().get((Integer) v), s);
    case FIXED:
      return model.createFixed(null, ((ByteBuffer) v).array(), s);
    }

    return v;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
