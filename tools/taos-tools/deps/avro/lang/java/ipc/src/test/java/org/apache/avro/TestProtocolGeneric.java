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

import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.SocketServer;
import org.apache.avro.ipc.SocketTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.util.Utf8;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

public class TestProtocolGeneric {
  private static final Logger LOG = LoggerFactory.getLogger(TestProtocolGeneric.class);

  protected static final File FILE = new File("../../../share/test/schemas/simple.avpr");
  protected static final Protocol PROTOCOL;
  static {
    try {
      PROTOCOL = Protocol.parse(FILE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean throwUndeclaredError;

  protected static class TestResponder extends GenericResponder {
    public TestResponder() {
      super(PROTOCOL);
    }

    public Object respond(Message message, Object request) throws AvroRemoteException {
      GenericRecord params = (GenericRecord) request;

      if ("hello".equals(message.getName())) {
        LOG.info("hello: " + params.get("greeting"));
        return new Utf8("goodbye");
      }

      if ("echo".equals(message.getName())) {
        Object record = params.get("record");
        LOG.info("echo: " + record);
        return record;
      }

      if ("echoBytes".equals(message.getName())) {
        Object data = params.get("data");
        LOG.info("echoBytes: " + data);
        return data;
      }

      if ("error".equals(message.getName())) {
        if (throwUndeclaredError)
          throw new RuntimeException("foo");
        GenericRecord error = new GenericData.Record(PROTOCOL.getType("TestError"));
        error.put("message", new Utf8("an error"));
        throw new AvroRemoteException(error);
      }

      throw new AvroRuntimeException("unexpected message: " + message.getName());
    }

  }

  protected static SocketServer server;
  protected static Transceiver client;
  protected static GenericRequestor requestor;

  @Before
  public void testStartServer() throws Exception {
    if (server != null)
      return;
    server = new SocketServer(new TestResponder(), new InetSocketAddress(0));
    server.start();
    client = new SocketTransceiver(new InetSocketAddress(server.getPort()));
    requestor = new GenericRequestor(PROTOCOL, client);
  }

  @Test
  public void testHello() throws Exception {
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("hello").getRequest());
    params.put("greeting", new Utf8("bob"));
    Utf8 response = (Utf8) requestor.request("hello", params);
    assertEquals(new Utf8("goodbye"), response);
  }

  @Test
  public void testEcho() throws Exception {
    GenericRecord record = new GenericData.Record(PROTOCOL.getType("TestRecord"));
    record.put("name", new Utf8("foo"));
    record.put("kind", new GenericData.EnumSymbol(PROTOCOL.getType("Kind"), "BAR"));
    record.put("hash",
        new GenericData.Fixed(PROTOCOL.getType("MD5"), new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("echo").getRequest());
    params.put("record", record);
    Object echoed = requestor.request("echo", params);
    assertEquals(record, echoed);
  }

  @Test
  public void testEchoBytes() throws Exception {
    Random random = new Random();
    int length = random.nextInt(1024 * 16);
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("echoBytes").getRequest());
    ByteBuffer data = ByteBuffer.allocate(length);
    random.nextBytes(data.array());
    data.flip();
    params.put("data", data);
    Object echoed = requestor.request("echoBytes", params);
    assertEquals(data, echoed);
  }

  @Test
  public void testError() throws Exception {
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("error").getRequest());
    AvroRemoteException error = null;
    try {
      requestor.request("error", params);
    } catch (AvroRemoteException e) {
      error = e;
    }
    assertNotNull(error);
    assertEquals("an error", ((GenericRecord) error.getValue()).get("message").toString());
  }

  @Test
  public void testUndeclaredError() throws Exception {
    this.throwUndeclaredError = true;
    RuntimeException error = null;
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("error").getRequest());
    try {
      requestor.request("error", params);
    } catch (RuntimeException e) {
      error = e;
    } finally {
      this.throwUndeclaredError = false;
    }
    assertNotNull(error);
    assertTrue(error.toString().contains("foo"));
  }

  @Test
  /**
   * Construct and use a different protocol whose "hello" method has an extra
   * argument to check that schema is sent to parse request.
   */
  public void testHandshake() throws Exception {
    Protocol protocol = new Protocol("Simple", "org.apache.avro.test");
    List<Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("extra", Schema.create(Schema.Type.BOOLEAN), null, null));
    fields.add(new Schema.Field("greeting", Schema.create(Schema.Type.STRING), null, null));
    Protocol.Message message = protocol.createMessage("hello", null /* doc */, new LinkedHashMap<String, String>(),
        Schema.createRecord(fields), Schema.create(Schema.Type.STRING), Schema.createUnion(new ArrayList<>()));
    protocol.getMessages().put("hello", message);
    try (Transceiver t = new SocketTransceiver(new InetSocketAddress(server.getPort()))) {
      GenericRequestor r = new GenericRequestor(protocol, t);
      GenericRecord params = new GenericData.Record(message.getRequest());
      params.put("extra", Boolean.TRUE);
      params.put("greeting", new Utf8("bob"));
      Utf8 response = (Utf8) r.request("hello", params);
      assertEquals(new Utf8("goodbye"), response);
    }
  }

  @Test
  /**
   * Construct and use a different protocol whose "echo" response has an extra
   * field to check that correct schema is used to parse response.
   */
  public void testResponseChange() throws Exception {

    List<Field> fields = new ArrayList<>();
    for (Field f : PROTOCOL.getType("TestRecord").getFields())
      fields.add(new Field(f.name(), f.schema(), null, null));
    fields.add(new Field("extra", Schema.create(Schema.Type.BOOLEAN), null, true));
    Schema record = Schema.createRecord("TestRecord", null, "org.apache.avro.test", false);
    record.setFields(fields);

    Protocol protocol = new Protocol("Simple", "org.apache.avro.test");
    List<Field> params = new ArrayList<>();
    params.add(new Field("record", record, null, null));

    Protocol.Message message = protocol.createMessage("echo", null, new LinkedHashMap<String, String>(),
        Schema.createRecord(params), record, Schema.createUnion(new ArrayList<>()));
    protocol.getMessages().put("echo", message);
    try (Transceiver t = new SocketTransceiver(new InetSocketAddress(server.getPort()))) {
      GenericRequestor r = new GenericRequestor(protocol, t);
      GenericRecord args = new GenericData.Record(message.getRequest());
      GenericRecord rec = new GenericData.Record(record);
      rec.put("name", new Utf8("foo"));
      rec.put("kind", new GenericData.EnumSymbol(PROTOCOL.getType("Kind"), "BAR"));
      rec.put("hash", new GenericData.Fixed(PROTOCOL.getType("MD5"),
          new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 }));
      rec.put("extra", Boolean.TRUE);
      args.put("record", rec);
      GenericRecord response = (GenericRecord) r.request("echo", args);
      assertEquals(rec, response);
    }
  }

  @AfterClass
  public static void testStopServer() throws Exception {
    client.close();
    server.close();
  }
}
