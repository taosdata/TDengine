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

package org.apache.avro.ipc.specific;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.ipc.generic.GenericResponder;

/** {@link org.apache.avro.ipc.Responder Responder} for generated interfaces. */
public class SpecificResponder extends GenericResponder {
  private Object impl;

  public SpecificResponder(Class iface, Object impl) {
    this(iface, impl, new SpecificData(impl.getClass().getClassLoader()));
  }

  public SpecificResponder(Protocol protocol, Object impl) {
    this(protocol, impl, new SpecificData(impl.getClass().getClassLoader()));
  }

  public SpecificResponder(Class iface, Object impl, SpecificData data) {
    this(data.getProtocol(iface), impl, data);
  }

  public SpecificResponder(Protocol protocol, Object impl, SpecificData data) {
    super(protocol, data);
    this.impl = impl;
  }

  public SpecificData getSpecificData() {
    return (SpecificData) getGenericData();
  }

  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter<>(schema, getSpecificData());
  }

  @Override
  protected DatumReader<Object> getDatumReader(Schema actual, Schema expected) {
    return new SpecificDatumReader<>(actual, expected, getSpecificData());
  }

  @Override
  public void writeError(Schema schema, Object error, Encoder out) throws IOException {
    getDatumWriter(schema).write(error, out);
  }

  @Override
  public Object respond(Message message, Object request) throws Exception {
    int numParams = message.getRequest().getFields().size();
    Object[] params = new Object[numParams];
    Class[] paramTypes = new Class[numParams];
    int i = 0;
    try {
      for (Schema.Field param : message.getRequest().getFields()) {
        params[i] = ((GenericRecord) request).get(param.name());
        paramTypes[i] = getSpecificData().getClass(param.schema());
        i++;
      }
      Method method = impl.getClass().getMethod(message.getName(), paramTypes);
      method.setAccessible(true);
      return method.invoke(impl, params);
    } catch (InvocationTargetException e) {
      Throwable error = e.getTargetException();
      if (error instanceof Exception) {
        throw (Exception) error;
      } else {
        throw new AvroRuntimeException(error);
      }
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

}
