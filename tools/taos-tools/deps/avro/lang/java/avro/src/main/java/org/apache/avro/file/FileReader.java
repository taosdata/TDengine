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
package org.apache.avro.file;

import java.io.IOException;
import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;

/** Interface for reading data from a file. */
public interface FileReader<D> extends Iterator<D>, Iterable<D>, Closeable {
  /** Return the schema for data in this file. */
  Schema getSchema();

  /**
   * Read the next datum from the file.
   * 
   * @param reuse an instance to reuse.
   * @throws NoSuchElementException if no more remain in the file.
   */
  D next(D reuse) throws IOException;

  /**
   * Move to the next synchronization point after a position. To process a range
   * of file entires, call this with the starting position, then check
   * {@link #pastSync(long)} with the end point before each call to
   * {@link #next()}.
   */
  void sync(long position) throws IOException;

  /** Return true if past the next synchronization point after a position. */
  boolean pastSync(long position) throws IOException;

  /** Return the current position in the input. */
  long tell() throws IOException;

}
