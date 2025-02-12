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

/** An InputStream that supports seek and tell. */
public interface SeekableInput extends Closeable {

  /**
   * Set the position for the next {@link java.io.InputStream#read(byte[],int,int)
   * read()}.
   */
  void seek(long p) throws IOException;

  /**
   * Return the position of the next
   * {@link java.io.InputStream#read(byte[],int,int) read()}.
   */
  long tell() throws IOException;

  /** Return the length of the file. */
  long length() throws IOException;

  /** Equivalent to {@link java.io.InputStream#read(byte[],int,int)}. */
  int read(byte[] b, int off, int len) throws IOException;
}
