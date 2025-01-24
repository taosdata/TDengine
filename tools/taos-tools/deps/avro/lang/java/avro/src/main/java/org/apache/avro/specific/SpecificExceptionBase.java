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

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Schema;

/** Base class for specific exceptions. */
public abstract class SpecificExceptionBase extends AvroRemoteException implements SpecificRecord, Externalizable {

  public SpecificExceptionBase() {
    super();
  }

  public SpecificExceptionBase(Throwable value) {
    super(value);
  }

  public SpecificExceptionBase(Object value) {
    super(value);
  }

  public SpecificExceptionBase(Object value, Throwable cause) {
    super(value, cause);
  }

  @Override
  public abstract Schema getSchema();

  @Override
  public abstract Object get(int field);

  @Override
  public abstract void put(int field, Object value);

  @Override
  public boolean equals(Object that) {
    if (that == this)
      return true; // identical object
    if (!(that instanceof SpecificExceptionBase))
      return false; // not a record
    if (this.getClass() != that.getClass())
      return false; // not same schema
    return SpecificData.get().compare(this, that, this.getSchema()) == 0;
  }

  @Override
  public int hashCode() {
    return SpecificData.get().hashCode(this, this.getSchema());
  }

  @Override
  public abstract void writeExternal(ObjectOutput out) throws IOException;

  @Override
  public abstract void readExternal(ObjectInput in) throws IOException;

}
