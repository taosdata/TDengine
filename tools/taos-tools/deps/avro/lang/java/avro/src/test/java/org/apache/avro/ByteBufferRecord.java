/**
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
package org.apache.avro;

import java.nio.ByteBuffer;

public class ByteBufferRecord {

  private ByteBuffer payload;
  private TypeEnum tp;

  public ByteBufferRecord() {
  }

  public ByteBuffer getPayload() {
    return payload;
  }

  public void setPayload(ByteBuffer payload) {
    this.payload = payload;
  }

  public TypeEnum getTp() {
    return tp;
  }

  public void setTp(TypeEnum tp) {
    this.tp = tp;
  }

  @Override
  public boolean equals(Object ob) {
    if (this == ob)
      return true;
    if (!(ob instanceof ByteBufferRecord))
      return false;
    ByteBufferRecord that = (ByteBufferRecord) ob;
    if (this.getPayload() == null)
      return that.getPayload() == null;
    if (!this.getPayload().equals(that.getPayload()))
      return false;
    if (this.getTp() == null)
      return that.getTp() == null;
    return this.getTp().equals(that.getTp());
  }

  @Override
  public int hashCode() {
    return this.payload.hashCode();
  }
}
