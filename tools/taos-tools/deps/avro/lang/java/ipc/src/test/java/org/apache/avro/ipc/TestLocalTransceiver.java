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
package org.apache.avro.ipc;

import static org.junit.Assert.assertEquals;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestLocalTransceiver {

  Protocol protocol = Protocol.parse("" + "{\"protocol\": \"Minimal\", " + "\"messages\": { \"m\": {"
      + "   \"request\": [{\"name\": \"x\", \"type\": \"string\"}], " + "   \"response\": \"string\"} } }");

  static class TestResponder extends GenericResponder {
    public TestResponder(Protocol local) {
      super(local);
    }

    @Override
    public Object respond(Message message, Object request) throws AvroRemoteException {
      assertEquals(new Utf8("hello"), ((GenericRecord) request).get("x"));
      return new Utf8("there");
    }

  }

  @Test
  public void testSingleRpc() throws Exception {
    Transceiver t = new LocalTransceiver(new TestResponder(protocol));
    GenericRecord params = new GenericData.Record(protocol.getMessages().get("m").getRequest());
    params.put("x", new Utf8("hello"));
    GenericRequestor r = new GenericRequestor(protocol, t);

    for (int x = 0; x < 5; x++)
      assertEquals(new Utf8("there"), r.request("m", params));
  }

}
