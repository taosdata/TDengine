#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package}.transport;

import java.net.InetSocketAddress;

import ${package}.service.SimpleOrderService;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.netty.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ${package}.service.OrderProcessingService;

/**
 * {@code SimpleOrderProcessingServer} provides a very basic example Netty endpoint for the
 * {@link SimpleOrderService} implementation
 */
public class SimpleOrderServiceEndpoint {

  private static final Logger log = LoggerFactory.getLogger(SimpleOrderServiceEndpoint.class);

  private InetSocketAddress endpointAddress;

  private Server service;

  public SimpleOrderServiceEndpoint(InetSocketAddress endpointAddress) {
    this.endpointAddress = endpointAddress;
  }

  public synchronized void start() throws Exception {
    if (log.isInfoEnabled()) {
      log.info("Starting Simple Ordering Netty Server on '{}'", endpointAddress);
    }

    SpecificResponder responder = new SpecificResponder(OrderProcessingService.class, new SimpleOrderService());
    service = new NettyServer(responder, endpointAddress);
    service.start();
  }

  public synchronized void stop() throws Exception {
    if (log.isInfoEnabled()) {
      log.info("Stopping Simple Ordering Server on '{}'", endpointAddress);
    }
    service.start();
  }
}
