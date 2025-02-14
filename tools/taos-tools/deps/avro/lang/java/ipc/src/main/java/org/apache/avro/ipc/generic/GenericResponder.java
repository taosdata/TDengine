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

package org.apache.avro.ipc.generic;

import java.io.IOException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.ipc.Responder;

/** {@link Responder} implementation for generic Java data. */
public abstract class GenericResponder extends Responder {
  private GenericData data;

  public GenericResponder(Protocol local) {
    this(local, GenericData.get());

  }

  public GenericResponder(Protocol local, GenericData data) {
    super(local);
    this.data = data;
  }

  public GenericData getGenericData() {
    return data;
  }

  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new GenericDatumWriter<>(schema, data);
  }

  protected DatumReader<Object> getDatumReader(Schema actual, Schema expected) {
    return new GenericDatumReader<>(actual, expected, data);
  }

  @Override
  public Object readRequest(Schema actual, Schema expected, Decoder in) throws IOException {
    return getDatumReader(actual, expected).read(null, in);
  }

  @Override
  public void writeResponse(Schema schema, Object response, Encoder out) throws IOException {
    getDatumWriter(schema).write(response, out);
  }

  @Override
  public void writeError(Schema schema, Object error, Encoder out) throws IOException {
    if (error instanceof AvroRemoteException)
      error = ((AvroRemoteException) error).getValue();
    getDatumWriter(schema).write(error, out);
  }

}
