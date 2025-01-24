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
import java.util.HashMap;
import java.util.Map;
import java.net.InetSocketAddress;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.ipc.generic.GenericRequestor;
import org.apache.avro.TestProtocolGeneric;
import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestSaslDigestMd5 extends TestProtocolGeneric {

  private static final Logger LOG = LoggerFactory.getLogger(TestSaslDigestMd5.class);

  private static final String HOST = "localhost";
  private static final String SERVICE = "avro-test";
  private static final String PRINCIPAL = "avro-test-principal";
  private static final String PASSWORD = "super secret password";
  private static final String REALM = "avro-test-realm";

  private static final String DIGEST_MD5_MECHANISM = "DIGEST-MD5";
  private static final Map<String, String> DIGEST_MD5_PROPS = new HashMap<>();

  static {
    DIGEST_MD5_PROPS.put(Sasl.QOP, "auth-int");
    if (System.getProperty("java.vendor").contains("IBM")) {
      DIGEST_MD5_PROPS.put("com.ibm.security.sasl.digest.realm", REALM);
    } else {
      DIGEST_MD5_PROPS.put("com.sun.security.sasl.digest.realm", REALM);
    }
  }

  private static class TestSaslCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback c : callbacks) {
        if (c instanceof NameCallback) {
          ((NameCallback) c).setName(PRINCIPAL);
        } else if (c instanceof PasswordCallback) {
          ((PasswordCallback) c).setPassword(PASSWORD.toCharArray());
        } else if (c instanceof AuthorizeCallback) {
          ((AuthorizeCallback) c).setAuthorized(true);
        } else if (c instanceof RealmCallback) {
          ((RealmCallback) c).setText(REALM);
        } else {
          throw new UnsupportedCallbackException(c);
        }
      }
    }
  }

  @Before
  public void testStartServer() throws Exception {
    if (server != null)
      return;
    server = new SaslSocketServer(new TestResponder(), new InetSocketAddress(0), DIGEST_MD5_MECHANISM, SERVICE, HOST,
        DIGEST_MD5_PROPS, new TestSaslCallbackHandler());
    server.start();
    SaslClient saslClient = Sasl.createSaslClient(new String[] { DIGEST_MD5_MECHANISM }, PRINCIPAL, SERVICE, HOST,
        DIGEST_MD5_PROPS, new TestSaslCallbackHandler());
    client = new SaslSocketTransceiver(new InetSocketAddress(server.getPort()), saslClient);
    requestor = new GenericRequestor(PROTOCOL, client);
  }

  @Test(expected = SaslException.class)
  public void testAnonymousClient() throws Exception {
    Server s = new SaslSocketServer(new TestResponder(), new InetSocketAddress(0), DIGEST_MD5_MECHANISM, SERVICE, HOST,
        DIGEST_MD5_PROPS, new TestSaslCallbackHandler());
    s.start();
    Transceiver c = new SaslSocketTransceiver(new InetSocketAddress(s.getPort()));
    GenericRequestor requestor = new GenericRequestor(PROTOCOL, c);
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("hello").getRequest());
    params.put("greeting", "bob");
    Utf8 response = (Utf8) requestor.request("hello", params);
    assertEquals(new Utf8("goodbye"), response);
    s.close();
    c.close();
  }

  private static class WrongPasswordCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback c : callbacks) {
        if (c instanceof NameCallback) {
          ((NameCallback) c).setName(PRINCIPAL);
        } else if (c instanceof PasswordCallback) {
          ((PasswordCallback) c).setPassword("wrong".toCharArray());
        } else if (c instanceof AuthorizeCallback) {
          ((AuthorizeCallback) c).setAuthorized(true);
        } else if (c instanceof RealmCallback) {
          ((RealmCallback) c).setText(REALM);
        } else {
          throw new UnsupportedCallbackException(c);
        }
      }
    }
  }

  @Test(expected = SaslException.class)
  public void testWrongPassword() throws Exception {
    Server s = new SaslSocketServer(new TestResponder(), new InetSocketAddress(0), DIGEST_MD5_MECHANISM, SERVICE, HOST,
        DIGEST_MD5_PROPS, new TestSaslCallbackHandler());
    s.start();
    SaslClient saslClient = Sasl.createSaslClient(new String[] { DIGEST_MD5_MECHANISM }, PRINCIPAL, SERVICE, HOST,
        DIGEST_MD5_PROPS, new WrongPasswordCallbackHandler());
    Transceiver c = new SaslSocketTransceiver(new InetSocketAddress(server.getPort()), saslClient);
    GenericRequestor requestor = new GenericRequestor(PROTOCOL, c);
    GenericRecord params = new GenericData.Record(PROTOCOL.getMessages().get("hello").getRequest());
    params.put("greeting", "bob");
    Utf8 response = (Utf8) requestor.request("hello", params);
    assertEquals(new Utf8("goodbye"), response);
    s.close();
    c.close();
  }

  @Override
  public void testHandshake() throws IOException {
  }

  @Override
  public void testResponseChange() throws IOException {
  }

}
