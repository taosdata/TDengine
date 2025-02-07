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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.trevni.Input;

/** Adapt a Hadoop {@link FSDataInputStream} to Trevni's {@link Input}. */
public class HadoopInput implements Input {
  private final FSDataInputStream stream;
  private final long len;

  /** Construct given a path and a configuration. */
  public HadoopInput(Path path, Configuration conf) throws IOException {
    this.stream = path.getFileSystem(conf).open(path);
    this.len = path.getFileSystem(conf).getFileStatus(path).getLen();
  }

  @Override
  public long length() {
    return len;
  }

  @Override
  public int read(long p, byte[] b, int s, int l) throws IOException {
    return stream.read(p, b, s, l);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
