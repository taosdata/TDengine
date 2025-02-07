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

import java.io.IOException;
import java.util.Map;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslException;
import javax.security.auth.callback.CallbackHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Server} that uses {@link javax.security.sasl} for authentication and
 * encryption.
 */
public class SaslSocketServer extends SocketServer {
  private static final Logger LOG = LoggerFactory.getLogger(SaslServer.class);

  private static abstract class SaslServerFactory {
    protected abstract SaslServer getServer() throws SaslException;
  }

  private SaslServerFactory factory;

  /**
   * Create using SASL's anonymous
   * (<a href="https://www.ietf.org/rfc/rfc2245.txt">RFC 2245) mechanism.
   */
  public SaslSocketServer(Responder responder, SocketAddress addr) throws IOException {
    this(responder, addr, new SaslServerFactory() {
      @Override
      public SaslServer getServer() {
        return new AnonymousServer();
      }
    });
  }

  /** Create using the specified {@link SaslServer} parameters. */
  public SaslSocketServer(Responder responder, SocketAddress addr, final String mechanism, final String protocol,
      final String serverName, final Map<String, ?> props, final CallbackHandler cbh) throws IOException {
    this(responder, addr, new SaslServerFactory() {
      @Override
      public SaslServer getServer() throws SaslException {
        return Sasl.createSaslServer(mechanism, protocol, serverName, props, cbh);
      }
    });
  }

  private SaslSocketServer(Responder responder, SocketAddress addr, SaslServerFactory factory) throws IOException {
    super(responder, addr);
    this.factory = factory;
  }

  @Override
  protected Transceiver getTransceiver(SocketChannel channel) throws IOException {
    return new SaslSocketTransceiver(channel, factory.getServer());
  }

  private static class AnonymousServer implements SaslServer {
    private String user;

    @Override
    public String getMechanismName() {
      return "ANONYMOUS";
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
      this.user = new String(response, StandardCharsets.UTF_8);
      return null;
    }

    @Override
    public boolean isComplete() {
      return user != null;
    }

    @Override
    public String getAuthorizationID() {
      return user;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
      return null;
    }

    @Override
    public void dispose() {
    }
  }

}
