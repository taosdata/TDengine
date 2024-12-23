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
import java.net.InetSocketAddress;
import java.util.Arrays;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.ResponderServlet;
import org.apache.avro.ipc.Server;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/** An HTTP-based RPC {@link Server}. */
public class HttpServer implements Server {
  private org.eclipse.jetty.server.Server server;

  /** Constructs a server to run on the named port. */
  public HttpServer(Responder responder, int port) throws IOException {
    this(new ResponderServlet(responder), null, port);
  }

  /** Constructs a server to run on the named port. */
  public HttpServer(ResponderServlet servlet, int port) throws IOException {
    this(servlet, null, port);
  }

  /** Constructs a server to run on the named port on the specified address. */
  public HttpServer(Responder responder, InetSocketAddress addr) throws IOException {
    this(new ResponderServlet(responder), addr.getHostString(), addr.getPort());
  }

  /** Constructs a server to run on the named port on the specified address. */
  public HttpServer(Responder responder, String bindAddress, int port) throws IOException {
    this(new ResponderServlet(responder), bindAddress, port);
  }

  /** Constructs a server to run on the named port on the specified address. */
  public HttpServer(ResponderServlet servlet, String bindAddress, int port) throws IOException {
    this.server = new org.eclipse.jetty.server.Server();
    ServerConnector connector = new ServerConnector(this.server);
    connector.setAcceptQueueSize(128);
    connector.setIdleTimeout(10000);
    if (bindAddress != null) {
      connector.setHost(bindAddress);
    }
    connector.setPort(port);
    server.addConnector(connector);

    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(new ServletHolder(servlet), "/*");
    ServletContextHandler sch = new ServletContextHandler();
    sch.setServletHandler(handler);
    server.setHandler(sch);
  }

  /**
   * Constructs a server to run with the given ConnectionFactory on the given
   * address/port.
   */
  public HttpServer(Responder responder, ConnectionFactory connectionFactory, String bindAddress, int port)
      throws IOException {
    this(new ResponderServlet(responder), connectionFactory, bindAddress, port);
  }

  /**
   * Constructs a server to run with the given ConnectionFactory on the given
   * address/port.
   */
  public HttpServer(ResponderServlet servlet, ConnectionFactory connectionFactory, String bindAddress, int port)
      throws IOException {
    this.server = new org.eclipse.jetty.server.Server();
    HttpConfiguration httpConfig = new HttpConfiguration();
    HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfig);
    ServerConnector connector = new ServerConnector(this.server, connectionFactory, httpFactory);
    if (bindAddress != null) {
      connector.setHost(bindAddress);
    }
    connector.setPort(port);

    server.addConnector(connector);
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);
    handler.addServletWithMapping(new ServletHolder(servlet), "/*");
  }

  /**
   * Constructs a server to run with the given connector.
   *
   * @deprecated - use the Constructors that take a ConnectionFactory
   */
  @Deprecated
  public HttpServer(ResponderServlet servlet, Connector connector) throws IOException {
    this.server = connector.getServer();
    if (server.getConnectors().length == 0 || Arrays.asList(server.getConnectors()).contains(connector)) {
      server.addConnector(connector);
    }
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);
    handler.addServletWithMapping(new ServletHolder(servlet), "/*");
  }

  /**
   * Constructs a server to run with the given connector.
   *
   * @deprecated - use the Constructors that take a ConnectionFactory
   */
  @Deprecated
  public HttpServer(Responder responder, Connector connector) throws IOException {
    this(new ResponderServlet(responder), connector);
  }

  public void addConnector(Connector connector) {
    server.addConnector(connector);
  }

  @Override
  public int getPort() {
    return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
  }

  @Override
  public void close() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  /**
   * Start the server.
   * 
   * @throws AvroRuntimeException if the underlying Jetty server throws any
   *                              exception while starting.
   */
  @Override
  public void start() {
    try {
      server.start();
    } catch (Exception e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public void join() throws InterruptedException {
    server.join();
  }
}
