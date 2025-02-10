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
import java.net.InetSocketAddress;
import java.net.URI;

import org.slf4j.LoggerFactory;

/** IPC utilities, including client and server factories. */
public class Ipc {
  private Ipc() {
  } // no public ctor

  static boolean warned = false;

  /** Create a client {@link Transceiver} connecting to the provided URI. */
  public static Transceiver createTransceiver(URI uri) throws IOException {
    if ("http".equals(uri.getScheme()))
      return new HttpTransceiver(uri.toURL());
    else if ("avro".equals(uri.getScheme()))
      return new SaslSocketTransceiver(new InetSocketAddress(uri.getHost(), uri.getPort()));
    else
      throw new IOException("unknown uri scheme: " + uri);
  }

  /**
   * Create a {@link Server} listening at the named URI using the provided
   * responder.
   */
  public static Server createServer(Responder responder, URI uri) throws IOException {
    if ("avro".equals(uri.getScheme())) {
      return new SaslSocketServer(responder, new InetSocketAddress(uri.getHost(), uri.getPort()));
    } else if ("http".equals(uri.getScheme())) {
      if (!warned) {
        LoggerFactory.getLogger(Ipc.class)
            .error("Using Ipc.createServer to create http instances is deprecated.  Create "
                + " an instance of org.apache.avro.ipc.jetty.HttpServer directly.");
        warned = true;
      }
      try {
        Class<?> cls = Class.forName("org.apache.avro.ipc.jetty.HttpServer");
        return (Server) cls.getConstructor(Responder.class, Integer.TYPE).newInstance(responder, uri.getPort());
      } catch (Throwable t) {
        // ignore, exception will be thrown
      }
    }
    throw new IOException("unknown uri scheme: " + uri);
  }

}
