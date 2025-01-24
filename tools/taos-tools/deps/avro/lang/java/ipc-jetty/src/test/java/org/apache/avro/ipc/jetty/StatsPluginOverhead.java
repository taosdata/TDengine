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

import java.io.IOException;
import java.net.URL;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.stats.StatsPlugin;

/**
 * Naively measures overhead of using the stats plugin.
 *
 * The API used is the generic one. The protocol is the "null" protocol: null is
 * sent and returned.
 */
public class StatsPluginOverhead {
  /** Number of RPCs per iteration. */
  private static final int COUNT = 100000;
  private static final Protocol NULL_PROTOCOL = Protocol.parse("{\"protocol\": \"null\", "
      + "\"messages\": { \"null\": {" + "   \"request\": [], " + "   \"response\": \"null\"} } }");

  private static class IdentityResponder extends GenericResponder {
    public IdentityResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request) throws AvroRemoteException {
      return request;
    }
  }

  public static void main(String[] args) throws Exception {
    double with = sendRpcs(true) / 1000000000.0;
    double without = sendRpcs(false) / 1000000000.0;

    System.out.println(String.format("Overhead: %f%%.  RPC/s: %f (with) vs %f (without).  " + "RPC time (ms): %f vs %f",
        100 * (with - without) / (without), COUNT / with, COUNT / without, 1000 * with / COUNT,
        1000 * without / COUNT));
  }

  /** Sends RPCs and returns nanos elapsed. */
  private static long sendRpcs(boolean withPlugin) throws Exception {
    HttpServer server = createServer(withPlugin);
    Transceiver t = new HttpTransceiver(new URL("http://127.0.0.1:" + server.getPort() + "/"));
    GenericRequestor requestor = new GenericRequestor(NULL_PROTOCOL, t);

    long now = System.nanoTime();
    for (int i = 0; i < COUNT; ++i) {
      requestor.request("null", null);
    }
    long elapsed = System.nanoTime() - now;
    t.close();
    server.close();
    return elapsed;
  }

  /** Starts an Avro server. */
  private static HttpServer createServer(boolean withPlugin) throws IOException {
    Responder r = new IdentityResponder(NULL_PROTOCOL);
    if (withPlugin) {
      r.addRPCPlugin(new StatsPlugin());
    }
    // Start Avro server
    HttpServer server = new HttpServer(r, 0);
    server.start();
    return server;
  }

}
