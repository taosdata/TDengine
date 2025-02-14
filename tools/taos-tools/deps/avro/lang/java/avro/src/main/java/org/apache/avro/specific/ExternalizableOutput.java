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

package org.apache.avro.specific;

import java.io.OutputStream;
import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Helper to permit Externalizable implementations that write to an
 * OutputStream.
 */
class ExternalizableOutput extends OutputStream {
  private final ObjectOutput out;

  public ExternalizableOutput(ObjectOutput out) {
    this.out = out;
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public void write(int c) throws IOException {
    out.write(c);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }

  @Override
  public void write(byte[] b, int offset, int len) throws IOException {
    out.write(b, offset, len);
  }
}
