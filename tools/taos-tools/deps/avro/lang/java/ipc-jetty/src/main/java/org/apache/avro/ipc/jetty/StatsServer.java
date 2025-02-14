package org.apache.avro.ipc.jetty;

import org.apache.avro.ipc.stats.StatsPlugin;
import org.apache.avro.ipc.stats.StatsServlet;
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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/* This is a server that displays live information from a StatsPlugin.
 *
 *  Typical usage is as follows:
 *    StatsPlugin plugin = new StatsPlugin();
 *    requestor.addPlugin(plugin);
 *    StatsServer server = new StatsServer(plugin, 8080);
 *
 *  */
public class StatsServer {
  Server httpServer;
  StatsPlugin plugin;

  /*
   * Start a stats server on the given port, responsible for the given plugin.
   */
  public StatsServer(StatsPlugin plugin, int port) throws Exception {
    this.httpServer = new Server(port);
    this.plugin = plugin;

    ServletHandler handler = new ServletHandler();
    httpServer.setHandler(handler);
    handler.addServletWithMapping(new ServletHolder(new StaticServlet()), "/");

    handler.addServletWithMapping(new ServletHolder(new StatsServlet(plugin)), "/");

    httpServer.start();
  }

  /* Stops this server. */
  public void stop() throws Exception {
    this.httpServer.stop();
  }
}
