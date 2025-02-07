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
package org.apache.trevni;

import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

/** Reads data from a column file. */
public class ColumnFileReader implements Closeable {
  private Input file;

  private long rowCount;
  private int columnCount;
  private ColumnFileMetaData metaData;
  private ColumnDescriptor[] columns;
  private Map<String, ColumnDescriptor> columnsByName;

  /** Construct reading from the named file. */
  public ColumnFileReader(File file) throws IOException {
    this(new InputFile(file));
  }

  /** Construct reading from the provided input. */
  public ColumnFileReader(Input file) throws IOException {
    this.file = file;
    readHeader();
  }

  /** Return the number of rows in this file. */
  public long getRowCount() {
    return rowCount;
  }

  /** Return the number of columns in this file. */
  public long getColumnCount() {
    return columnCount;
  }

  /** Return this file's metadata. */
  public ColumnFileMetaData getMetaData() {
    return metaData;
  }

  /** Return all columns' metadata. */
  public ColumnMetaData[] getColumnMetaData() {
    ColumnMetaData[] result = new ColumnMetaData[columnCount];
    for (int i = 0; i < columnCount; i++)
      result[i] = columns[i].metaData;
    return result;
  }

  /** Return root columns' metadata. Roots are columns that have no parent. */
  public List<ColumnMetaData> getRoots() {
    List<ColumnMetaData> result = new ArrayList<>();
    for (int i = 0; i < columnCount; i++)
      if (columns[i].metaData.getParent() == null)
        result.add(columns[i].metaData);
    return result;
  }

  /** Return a column's metadata. */
  public ColumnMetaData getColumnMetaData(int number) {
    return columns[number].metaData;
  }

  /** Return a column's metadata. */
  public ColumnMetaData getColumnMetaData(String name) {
    return getColumn(name).metaData;
  }

  private <T extends Comparable> ColumnDescriptor<T> getColumn(String name) {
    ColumnDescriptor column = columnsByName.get(name);
    if (column == null)
      throw new TrevniRuntimeException("No column named: " + name);
    return (ColumnDescriptor<T>) column;
  }

  private void readHeader() throws IOException {
    InputBuffer in = new InputBuffer(file, 0);
    readMagic(in);
    this.rowCount = in.readFixed64();
    this.columnCount = in.readFixed32();
    this.metaData = ColumnFileMetaData.read(in);
    this.columnsByName = new HashMap<>(columnCount);

    columns = new ColumnDescriptor[columnCount];
    readColumnMetaData(in);
    readColumnStarts(in);
  }

  private void readMagic(InputBuffer in) throws IOException {
    byte[] magic = new byte[ColumnFileWriter.MAGIC.length];
    try {
      in.readFully(magic);
    } catch (IOException e) {
      throw new IOException("Not a data file.");
    }
    if (!(Arrays.equals(ColumnFileWriter.MAGIC, magic) || !Arrays.equals(ColumnFileWriter.MAGIC_1, magic)
        || !Arrays.equals(ColumnFileWriter.MAGIC_0, magic)))
      throw new IOException("Not a data file.");
  }

  private void readColumnMetaData(InputBuffer in) throws IOException {
    for (int i = 0; i < columnCount; i++) {
      ColumnMetaData meta = ColumnMetaData.read(in, this);
      meta.setDefaults(this.metaData);
      ColumnDescriptor column = new ColumnDescriptor(file, meta);
      columns[i] = column;
      meta.setNumber(i);
      columnsByName.put(meta.getName(), column);
    }
  }

  private void readColumnStarts(InputBuffer in) throws IOException {
    for (int i = 0; i < columnCount; i++)
      columns[i].start = in.readFixed64();
  }

  /** Return an iterator over values in the named column. */
  public <T extends Comparable> ColumnValues<T> getValues(String columnName) throws IOException {
    return new ColumnValues<>(getColumn(columnName));
  }

  /** Return an iterator over values in a column. */
  public <T extends Comparable> ColumnValues<T> getValues(int column) throws IOException {
    return new ColumnValues<>(columns[column]);
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

}
