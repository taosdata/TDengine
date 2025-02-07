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
package org.apache.avro.ipc.jetty;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.TestProtocolSpecific;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.generic.GenericData;
import org.apache.avro.test.Simple;

import org.junit.Test;

import java.net.URL;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class TestProtocolHttp extends TestProtocolSpecific {

  @Override
  public Server createServer(Responder testResponder) throws Exception {
    return new HttpServer(testResponder, 0);
  }

  @Override
  public Transceiver createTransceiver() throws Exception {
    return new HttpTransceiver(new URL("http://127.0.0.1:" + server.getPort() + "/"));
  }

  protected int getExpectedHandshakeCount() {
    return REPEATING;
  }

  @Test
  public void testTimeout() throws Throwable {
    ServerSocket s = new ServerSocket(0);
    HttpTransceiver client = new HttpTransceiver(new URL("http://127.0.0.1:" + s.getLocalPort() + "/"));
    client.setTimeout(100);
    Simple proxy = SpecificRequestor.getClient(Simple.class, client);
    try {
      proxy.hello("foo");
      fail("Should have failed with an exception");
    } catch (AvroRuntimeException e) {
      assertTrue("Got unwanted exception: " + e.getCause(), e.getCause() instanceof SocketTimeoutException);
    } finally {
      s.close();
    }
  }

  /** Test that Responder ignores one-way with stateless transport. */
  @Test
  public void testStatelessOneway() throws Exception {
    // a version of the Simple protocol that doesn't declare "ack" one-way
    Protocol protocol = new Protocol("Simple", "org.apache.avro.test");
    Protocol.Message message = protocol.createMessage("ack", null, new LinkedHashMap<String, String>(),
        Schema.createRecord(new ArrayList<>()), Schema.create(Schema.Type.NULL), Schema.createUnion(new ArrayList<>()));
    protocol.getMessages().put("ack", message);

    // call a server over a stateless protocol that has a one-way "ack"
    GenericRequestor requestor = new GenericRequestor(protocol, createTransceiver());
    requestor.request("ack", new GenericData.Record(message.getRequest()));

    // make the request again, to better test handshakes w/ differing protocols
    requestor.request("ack", new GenericData.Record(message.getRequest()));
  }

}
