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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileDescriptor;
import java.io.IOException;

/** A {@link FileInputStream} that implements {@link SeekableInput}. */
public class SeekableFileInput extends FileInputStream implements SeekableInput {

  public SeekableFileInput(File file) throws IOException {
    super(file);
  }

  public SeekableFileInput(FileDescriptor fd) throws IOException {
    super(fd);
  }

  @Override
  public void seek(long p) throws IOException {
    getChannel().position(p);
  }

  @Override
  public long tell() throws IOException {
    return getChannel().position();
  }

  @Override
  public long length() throws IOException {
    return getChannel().size();
  }
}
