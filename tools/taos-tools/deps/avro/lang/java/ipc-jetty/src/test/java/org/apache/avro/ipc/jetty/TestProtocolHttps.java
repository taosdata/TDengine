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

import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.TestProtocolSpecific;
import org.apache.avro.ipc.HttpTransceiver;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.net.URL;

public class TestProtocolHttps extends TestProtocolSpecific {

  @Override
  public Server createServer(Responder testResponder) throws Exception {
    System.setProperty("javax.net.ssl.keyStore", "src/test/keystore");
    System.setProperty("javax.net.ssl.keyStorePassword", "avrotest");
    System.setProperty("javax.net.ssl.password", "avrotest");
    System.setProperty("javax.net.ssl.trustStore", "src/test/truststore");
    System.setProperty("javax.net.ssl.trustStorePassword", "avrotest");
    SslConnectionFactory connectionFactory = new SslConnectionFactory("HTTP/1.1");
    SslContextFactory sslContextFactory = connectionFactory.getSslContextFactory();

    sslContextFactory.setKeyStorePath(System.getProperty("javax.net.ssl.keyStore"));
    sslContextFactory.setKeyManagerPassword(System.getProperty("javax.net.ssl.password"));
    sslContextFactory.setKeyStorePassword(System.getProperty("javax.net.ssl.keyStorePassword"));
    sslContextFactory.setNeedClientAuth(false);
    return new HttpServer(testResponder, connectionFactory, "localhost", 18443);
  }

  @Override
  public Transceiver createTransceiver() throws Exception {
    return new HttpTransceiver(new URL("https://localhost:" + server.getPort() + "/"));
  }

  protected int getExpectedHandshakeCount() {
    return REPEATING;
  }

}
