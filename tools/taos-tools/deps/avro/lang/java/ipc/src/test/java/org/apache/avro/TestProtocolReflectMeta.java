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
package org.apache.avro;

import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.reflect.ReflectRequestor;
import org.apache.avro.ipc.reflect.ReflectResponder;
import org.junit.Before;

import java.net.InetSocketAddress;

public class TestProtocolReflectMeta extends TestProtocolReflect {

  @Before
  @Override
  public void testStartServer() throws Exception {
    if (server != null)
      return;
    ReflectResponder rresp = new ReflectResponder(Simple.class, new TestImpl());
    rresp.addRPCPlugin(new RPCMetaTestPlugin("key1"));
    rresp.addRPCPlugin(new RPCMetaTestPlugin("key2"));
    server = new SocketServer(rresp, new InetSocketAddress(0));
    server.start();

    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    ReflectRequestor requestor = new ReflectRequestor(Simple.class, client);
    requestor.addRPCPlugin(new RPCMetaTestPlugin("key1"));
    requestor.addRPCPlugin(new RPCMetaTestPlugin("key2"));
    proxy = ReflectRequestor.getClient(Simple.class, requestor);
  }

}
