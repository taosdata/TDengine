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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Random;

import javax.servlet.UnavailableException;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.ipc.stats.StatsPlugin;
import org.apache.avro.ipc.stats.StatsServlet;
import org.junit.Test;

public class TestStatsPluginAndServlet {
  Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", " + "\"messages\": { \"m\": {"
      + "   \"request\": [{\"name\": \"x\", \"type\": \"int\"}], " + "   \"response\": \"int\"} } }");
  Message message = protocol.getMessages().get("m");

  private static final long MS = 1000 * 1000L;

  /** Returns an HTML string. */
  private String generateServletResponse(StatsPlugin statsPlugin) throws IOException {
    StatsServlet servlet;
    try {
      servlet = new StatsServlet(statsPlugin);
    } catch (UnavailableException e1) {
      throw new IOException();
    }
    StringWriter w = new StringWriter();
    try {
      servlet.writeStats(w);
    } catch (Exception e) {
      e.printStackTrace();
    }
    String o = w.toString();
    return o;
  }

  /** Expects 0 and returns 1. */
  static class TestResponder extends GenericResponder {
    public TestResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request) throws AvroRemoteException {
      assertEquals(0, ((GenericRecord) request).get("x"));
      return 1;
    }

  }

  private void makeRequest(Transceiver t) throws Exception {
    GenericRecord params = new GenericData.Record(protocol.getMessages().get("m").getRequest());
    params.put("x", 0);
    GenericRequestor r = new GenericRequestor(protocol, t);
    assertEquals(1, r.request("m", params));
  }

  @Test
  public void testFullServerPath() throws Exception {
    Responder r = new TestResponder(protocol);
    StatsPlugin statsPlugin = new StatsPlugin();
    r.addRPCPlugin(statsPlugin);
    Transceiver t = new LocalTransceiver(r);

    for (int i = 0; i < 10; ++i) {
      makeRequest(t);
    }

    String o = generateServletResponse(statsPlugin);
    assertTrue(o.contains("10 calls"));
  }

  @Test
  public void testMultipleRPCs() throws IOException {
    org.apache.avro.ipc.stats.FakeTicks t = new org.apache.avro.ipc.stats.FakeTicks();
    StatsPlugin statsPlugin = new StatsPlugin(t, StatsPlugin.LATENCY_SEGMENTER, StatsPlugin.PAYLOAD_SEGMENTER);
    RPCContext context1 = makeContext();
    RPCContext context2 = makeContext();
    statsPlugin.serverReceiveRequest(context1);
    t.passTime(100 * MS); // first takes 100ms
    statsPlugin.serverReceiveRequest(context2);
    String r = generateServletResponse(statsPlugin);
    // Check in progress RPCs
    assertTrue(r.contains("m: 0ms"));
    assertTrue(r.contains("m: 100ms"));
    statsPlugin.serverSendResponse(context1);
    t.passTime(900 * MS); // second takes 900ms
    statsPlugin.serverSendResponse(context2);
    r = generateServletResponse(statsPlugin);
    assertTrue(r.contains("Average: 500.0ms"));
  }

  @Test
  public void testPayloadSize() throws Exception {
    Responder r = new TestResponder(protocol);
    StatsPlugin statsPlugin = new StatsPlugin();
    r.addRPCPlugin(statsPlugin);
    Transceiver t = new LocalTransceiver(r);
    makeRequest(t);

    String resp = generateServletResponse(statsPlugin);
    assertTrue(resp.contains("Average: 2.0"));

  }

  private RPCContext makeContext() {
    RPCContext context = new RPCContext();
    context.setMessage(message);
    return context;
  }

  /** Sleeps as requested. */
  private static class SleepyResponder extends GenericResponder {
    public SleepyResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request) throws AvroRemoteException {
      try {
        Thread.sleep((Long) ((GenericRecord) request).get("millis"));
      } catch (InterruptedException e) {
        throw new AvroRemoteException(e);
      }
      return null;
    }
  }

  /**
   * Demo program for using RPC stats. This automatically generates client RPC
   * requests. Alternatively a can be used (as below) to trigger RPCs.
   * 
   * <pre>
   * java -jar build/avro-tools-*.jar rpcsend '{"protocol":"sleepy","namespace":null,"types":[],"messages":{"sleep":{"request":[{"name":"millis","type":"long"}],"response":"null"}}}' sleep localhost 7002 '{"millis": 20000}'
   * </pre>
   * 
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      args = new String[] { "7002", "7003" };
    }
    Protocol protocol = Protocol.parse("{\"protocol\": \"sleepy\", " + "\"messages\": { \"sleep\": {"
        + "   \"request\": [{\"name\": \"millis\", \"type\": \"long\"},"
        + "{\"name\": \"data\", \"type\": \"bytes\"}], " + "   \"response\": \"null\"} } }");
    Responder r = new SleepyResponder(protocol);
    StatsPlugin p = new StatsPlugin();
    r.addRPCPlugin(p);

    // Start Avro server
    HttpServer avroServer = new HttpServer(r, Integer.parseInt(args[0]));
    avroServer.start();

    StatsServer ss = new StatsServer(p, 8080);

    HttpTransceiver trans = new HttpTransceiver(new URL("http://localhost:" + Integer.parseInt(args[0])));
    GenericRequestor req = new GenericRequestor(protocol, trans);

    while (true) {
      Thread.sleep(1000);
      GenericRecord params = new GenericData.Record(protocol.getMessages().get("sleep").getRequest());
      Random rand = new Random();
      params.put("millis", Math.abs(rand.nextLong()) % 1000);
      int payloadSize = Math.abs(rand.nextInt()) % 10000;
      byte[] payload = new byte[payloadSize];
      rand.nextBytes(payload);
      params.put("data", ByteBuffer.wrap(payload));
      req.request("sleep", params);
    }
  }
}
