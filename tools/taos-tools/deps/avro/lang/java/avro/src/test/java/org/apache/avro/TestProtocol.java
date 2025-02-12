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

import static org.junit.Assert.*;

import org.junit.Test;

public class TestProtocol {

  @Test
  public void testPropEquals() {
    Protocol p1 = new Protocol("P", null, "foo");
    p1.addProp("a", "1");
    Protocol p2 = new Protocol("P", null, "foo");
    p2.addProp("a", "2");
    assertFalse(p1.equals(p2));
  }

  @Test
  public void testSplitProtocolBuild() {
    Protocol p = new Protocol("P", null, "foo");
    p.addProp("property", "some value");

    String protocolString = p.toString();
    final int mid = protocolString.length() / 2;

    Protocol parsedStringProtocol = org.apache.avro.Protocol.parse(protocolString);
    Protocol parsedArrayOfStringProtocol = org.apache.avro.Protocol.parse(protocolString.substring(0, mid),
        protocolString.substring(mid));

    assertNotNull(parsedStringProtocol);
    assertNotNull(parsedArrayOfStringProtocol);
    assertEquals(parsedStringProtocol.toString(), parsedArrayOfStringProtocol.toString());
  }
}
