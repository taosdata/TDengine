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

/** A column output buffer for array columns. */
class ArrayColumnOutputBuffer extends ColumnOutputBuffer {
  private int length; // remaining in current array

  private static final int NONE = -1;

  private int runLength; // length of current run
  private int runValue = NONE; // what kind of run

  public ArrayColumnOutputBuffer(ColumnFileWriter writer, ColumnMetaData meta) throws IOException {
    super(writer, meta);
    assert getMeta().isArray() || getMeta().getParent() != null;
    assert !getMeta().hasIndexValues();
  }

  @Override
  public void writeLength(int l) throws IOException {
    assert this.length == 0;
    assert l >= 0;
    this.length = l;
    if (l == runValue) {
      runLength++; // continue a run
      return;
    }
    flushRun(); // end a run
    if (l == 1 || l == 0) {
      runLength = 1; // start a run
      runValue = l;
    } else {
      getBuffer().writeLength(l); // not a run
    }
  }

  @Override
  public void writeValue(Object value) throws IOException {
    assert length > 0;
    if (getMeta().getType() != ValueType.NULL) {
      flushRun();
      getBuffer().writeValue(value, getMeta().getType());
    }
    length -= 1;
  }

  @Override
  void flushBuffer() throws IOException {
    flushRun();
    super.flushBuffer();
  }

  private void flushRun() throws IOException {
    if (runLength == 0) // not in run
      return;
    else if (runLength == 1) // single value
      getBuffer().writeLength(runValue);
    else // a run
      getBuffer().writeLength((3 - runValue) - (runLength << 1));

    runLength = 0; // reset
    runValue = NONE;
  }

}
