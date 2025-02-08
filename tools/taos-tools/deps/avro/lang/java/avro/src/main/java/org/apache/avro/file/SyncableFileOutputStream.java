/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.avro.file;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * An implementation of {@linkplain Syncable} which writes to a file. An
 * instance of this class can be used with {@linkplain DataFileWriter} to
 * guarantee that Avro Container Files are persisted to disk on supported
 * platforms using the {@linkplain org.apache.avro.file.DataFileWriter#fSync()}
 * method.
 *
 * @see FileOutputStream
 */
public class SyncableFileOutputStream extends FileOutputStream implements Syncable {

  /**
   * Creates an instance of {@linkplain SyncableFileOutputStream} with the given
   * name.
   *
   * @param name - the full file name.
   * @throws FileNotFoundException - if the file cannot be created or opened.
   */
  public SyncableFileOutputStream(String name) throws FileNotFoundException {
    super(name);
  }

  /**
   * Creates an instance of {@linkplain SyncableFileOutputStream} using the given
   * {@linkplain File} instance.
   *
   * @param file - The file to use to create the output stream.
   *
   * @throws FileNotFoundException - if the file cannot be created or opened.
   */
  public SyncableFileOutputStream(File file) throws FileNotFoundException {
    super(file);
  }

  /**
   * Creates an instance of {@linkplain SyncableFileOutputStream} with the given
   * name and optionally append to the file if it already exists.
   *
   * @param name   - the full file name.
   * @param append - true if the file is to be appended to
   *
   * @throws FileNotFoundException - if the file cannot be created or opened.
   */
  public SyncableFileOutputStream(String name, boolean append) throws FileNotFoundException {
    super(name, append);
  }

  /**
   * Creates an instance of {@linkplain SyncableFileOutputStream} that writes to
   * the file represented by the given {@linkplain File} instance and optionally
   * append to the file if it already exists.
   *
   * @param file   - the file instance to use to create the stream.
   * @param append - true if the file is to be appended to
   *
   * @throws FileNotFoundException - if the file cannot be created or opened.
   */
  public SyncableFileOutputStream(File file, boolean append) throws FileNotFoundException {
    super(file, append);
  }

  /**
   * Creates an instance of {@linkplain SyncableFileOutputStream} using the given
   * {@linkplain FileDescriptor} instance.
   */
  public SyncableFileOutputStream(FileDescriptor fdObj) {
    super(fdObj);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void sync() throws IOException {
    getFD().sync();
  }
}
