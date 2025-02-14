/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** An {@link Input} backed with data in a byte array. */
public class InputBytes extends ByteArrayInputStream implements Input {

  /** Construct for the given bytes. */
  public InputBytes(byte[] data) {
    super(data);
  }

  /** Construct for the given bytes. */
  public InputBytes(ByteBuffer data) {
    super(data.array(), data.position(), data.limit());
  }

  @Override
  public long length() throws IOException {
    return this.count;
  }

  @Override
  public synchronized int read(long pos, byte[] b, int start, int len) throws IOException {
    this.pos = (int) pos;
    return read(b, start, len);
  }

  byte[] getBuffer() {
    return buf;
  }
}
