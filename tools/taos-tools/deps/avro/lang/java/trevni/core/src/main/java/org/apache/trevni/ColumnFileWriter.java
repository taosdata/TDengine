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
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.HashSet;

/**
 * Writes data to a column file. All data is buffered until
 * {@link #writeTo(File)} is called.
 */
public class ColumnFileWriter {

  static final byte[] MAGIC_0 = new byte[] { 'T', 'r', 'v', 0 };
  static final byte[] MAGIC_1 = new byte[] { 'T', 'r', 'v', 1 };
  static final byte[] MAGIC = new byte[] { 'T', 'r', 'v', 2 };

  private ColumnFileMetaData metaData;
  private ColumnOutputBuffer[] columns;

  private long rowCount;
  private int columnCount;
  private long size;

  /** Construct given metadata for each column in the file. */
  public ColumnFileWriter(ColumnFileMetaData fileMeta, ColumnMetaData... columnMeta) throws IOException {
    checkColumns(columnMeta);
    this.metaData = fileMeta;
    this.columnCount = columnMeta.length;
    this.columns = new ColumnOutputBuffer[columnCount];
    for (int i = 0; i < columnCount; i++) {
      ColumnMetaData c = columnMeta[i];
      c.setDefaults(metaData);
      columns[i] = c.isArray() ? new ArrayColumnOutputBuffer(this, c) : new ColumnOutputBuffer(this, c);
      size += OutputBuffer.BLOCK_SIZE; // over-estimate
    }
  }

  private void checkColumns(ColumnMetaData[] columnMeta) {
    Set<String> seen = new HashSet<>();
    for (ColumnMetaData c : columnMeta) {
      String name = c.getName();
      if (seen.contains(name))
        throw new TrevniRuntimeException("Duplicate column name: " + name);
      ColumnMetaData parent = c.getParent();
      if (parent != null && !seen.contains(parent.getName()))
        throw new TrevniRuntimeException("Parent must precede child: " + name);
      seen.add(name);
    }
  }

  void incrementSize(int n) {
    size += n;
  }

  /**
   * Return the approximate size of the file that will be written. Tries to
   * slightly over-estimate. Indicates both the size in memory of the buffered
   * data as well as the size of the file that will be written by
   * {@link #writeTo(OutputStream)}.
   */
  public long sizeEstimate() {
    return size;
  }

  /** Return this file's metadata. */
  public ColumnFileMetaData getMetaData() {
    return metaData;
  }

  /** Return the number of columns in the file. */
  public int getColumnCount() {
    return columnCount;
  }

  /** Add a row to the file. */
  public void writeRow(Object... row) throws IOException {
    startRow();
    for (int column = 0; column < columnCount; column++)
      writeValue(row[column], column);
    endRow();
  }

  /** Expert: Called before any values are written to a row. */
  public void startRow() throws IOException {
    for (int column = 0; column < columnCount; column++)
      columns[column].startRow();
  }

  /**
   * Expert: Declare a count of items to be written to an array column or a column
   * whose parent is an array.
   */
  public void writeLength(int length, int column) throws IOException {
    columns[column].writeLength(length);
  }

  /**
   * Expert: Add a value to a row. For values in array columns or whose parents
   * are array columns, this must be preceded by a call to
   * {@link #writeLength(int, int)} and must be called that many times. For normal
   * columns this is called once for each row in the column.
   */
  public void writeValue(Object value, int column) throws IOException {
    columns[column].writeValue(value);
  }

  /** Expert: Called after all values are written to a row. */
  public void endRow() throws IOException {
    for (int column = 0; column < columnCount; column++)
      columns[column].endRow();
    rowCount++;
  }

  /** Write all rows added to the named file. */
  public void writeTo(File file) throws IOException {
    try (OutputStream out = new FileOutputStream(file)) {
      writeTo(out);
    }
  }

  /** Write all rows added to the named output stream. */
  public void writeTo(OutputStream out) throws IOException {
    writeHeader(out);

    for (int column = 0; column < columnCount; column++)
      columns[column].writeTo(out);
  }

  private void writeHeader(OutputStream out) throws IOException {
    OutputBuffer header = new OutputBuffer();

    header.write(MAGIC); // magic

    header.writeFixed64(rowCount); // row count

    header.writeFixed32(columnCount); // column count

    metaData.write(header); // file metadata

    for (ColumnOutputBuffer column : columns)
      column.getMeta().write(header); // column metadata

    for (long start : computeStarts(header.size()))
      header.writeFixed64(start); // column starts

    header.writeTo(out);

  }

  private long[] computeStarts(long start) throws IOException {
    long[] result = new long[columnCount];
    start += columnCount * 8; // room for starts
    for (int column = 0; column < columnCount; column++) {
      result[column] = start;
      start += columns[column].size();
    }
    return result;
  }

}
