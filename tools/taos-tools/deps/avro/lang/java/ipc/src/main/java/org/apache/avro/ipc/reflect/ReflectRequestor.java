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

package org.apache.avro.ipc.reflect;

import java.io.IOException;
import java.lang.reflect.Proxy;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

/** A {@link org.apache.avro.ipc.Requestor} for existing interfaces. */
public class ReflectRequestor extends SpecificRequestor {

  public ReflectRequestor(Class<?> iface, Transceiver transceiver) throws IOException {
    this(iface, transceiver, new ReflectData(iface.getClassLoader()));
  }

  protected ReflectRequestor(Protocol protocol, Transceiver transceiver) throws IOException {
    this(protocol, transceiver, ReflectData.get());
  }

  public ReflectRequestor(Class<?> iface, Transceiver transceiver, ReflectData data) throws IOException {
    this(data.getProtocol(iface), transceiver, data);
  }

  public ReflectRequestor(Protocol protocol, Transceiver transceiver, ReflectData data) throws IOException {
    super(protocol, transceiver, data);
  }

  public ReflectData getReflectData() {
    return (ReflectData) getSpecificData();
  }

  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new ReflectDatumWriter<>(schema, getReflectData());
  }

  @Override
  protected DatumReader<Object> getDatumReader(Schema writer, Schema reader) {
    return new ReflectDatumReader<>(writer, reader, getReflectData());
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  public static <T> T getClient(Class<T> iface, Transceiver transceiver) throws IOException {
    return getClient(iface, transceiver, new ReflectData(iface.getClassLoader()));
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  @SuppressWarnings("unchecked")
  public static <T> T getClient(Class<T> iface, Transceiver transceiver, ReflectData reflectData) throws IOException {
    Protocol protocol = reflectData.getProtocol(iface);
    return (T) Proxy.newProxyInstance(reflectData.getClassLoader(), new Class[] { iface },
        new ReflectRequestor(protocol, transceiver, reflectData));
  }

  /** Create a proxy instance whose methods invoke RPCs. */
  @SuppressWarnings("unchecked")
  public static <T> T getClient(Class<T> iface, ReflectRequestor rreq) throws IOException {
    return (T) Proxy.newProxyInstance(rreq.getReflectData().getClassLoader(), new Class[] { iface }, rreq);
  }
}
