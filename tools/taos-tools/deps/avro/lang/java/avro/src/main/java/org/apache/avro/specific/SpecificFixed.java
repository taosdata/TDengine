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

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.BinaryData;

/** Base class for generated fixed-sized data classes. */
public abstract class SpecificFixed implements GenericFixed, Comparable<SpecificFixed>, Externalizable {

  private byte[] bytes;

  public SpecificFixed() {
    bytes(new byte[getSchema().getFixedSize()]);
  }

  public SpecificFixed(byte[] bytes) {
    bytes(bytes);
  }

  public void bytes(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public byte[] bytes() {
    return bytes;
  }

  @Override
  public abstract Schema getSchema();

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    return o instanceof GenericFixed && Arrays.equals(bytes, ((GenericFixed) o).bytes());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString() {
    return Arrays.toString(bytes);
  }

  @Override
  public int compareTo(SpecificFixed that) {
    return BinaryData.compareBytes(this.bytes, 0, this.bytes.length, that.bytes, 0, that.bytes.length);
  }

  @Override
  public abstract void writeExternal(ObjectOutput out) throws IOException;

  @Override
  public abstract void readExternal(ObjectInput in) throws IOException;

}
