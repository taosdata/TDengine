/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.TestProtocolGeneric;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.ipc.reflect.ReflectResponder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

public class TestSaslAnonymous extends TestProtocolGeneric {

  private static final Logger LOG = LoggerFactory.getLogger(TestSaslAnonymous.class);

  @Before
  public void testStartServer() throws Exception {
    if (server != null)
      return;
    server = new SaslSocketServer(new TestResponder(), new InetSocketAddress(0));
    server.start();
    client = new SaslSocketTransceiver(new InetSocketAddress(server.getPort()));
    requestor = new GenericRequestor(PROTOCOL, client);
  }

  @Override
  public void testHandshake() throws IOException {
  }

  @Override
  public void testResponseChange() throws IOException {
  }

  public interface ProtoInterface {
    byte[] test(byte[] b);
  }

  // test big enough to fill socket output buffer
  @Test
  public void test64kRequest() throws Exception {
    SaslSocketServer s = new SaslSocketServer(new ReflectResponder(ProtoInterface.class, (ProtoInterface) b -> b),
        new InetSocketAddress(0));
    s.start();
    SaslSocketTransceiver client = new SaslSocketTransceiver(new InetSocketAddress(s.getPort()));
    ProtoInterface proxy = ReflectRequestor.getClient(ProtoInterface.class, client);

    proxy.test(new byte[64 * 1024]);

    client.close();
    s.close();
  }

}
