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

package org.apache.avro.mapred;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.avro.file.SeekableInput;

/** Adapt an {@link FSDataInputStream} to {@link SeekableInput}. */
public class FsInput implements Closeable, SeekableInput {
  private final FSDataInputStream stream;
  private final long len;

  /** Construct given a path and a configuration. */
  public FsInput(Path path, Configuration conf) throws IOException {
    this(path, path.getFileSystem(conf));
  }

  /** Construct given a path and a {@code FileSystem}. */
  public FsInput(Path path, FileSystem fileSystem) throws IOException {
    this.len = fileSystem.getFileStatus(path).getLen();
    this.stream = fileSystem.open(path);
  }

  @Override
  public long length() {
    return len;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void seek(long p) throws IOException {
    stream.seek(p);
  }

  @Override
  public long tell() throws IOException {
    return stream.getPos();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
