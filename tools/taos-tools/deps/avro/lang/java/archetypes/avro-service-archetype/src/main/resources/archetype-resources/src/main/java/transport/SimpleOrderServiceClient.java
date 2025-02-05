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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.netty.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ${package}.service.Confirmation;
import ${package}.service.Order;
import ${package}.service.OrderFailure;
import ${package}.service.OrderProcessingService;

/**
 * {@code SimpleOrderServiceClient} is a basic client for the Netty backed {@link OrderProcessingService}
 * implementation.
 */
public class SimpleOrderServiceClient implements OrderProcessingService {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleOrderServiceEndpoint.class);

  private InetSocketAddress endpointAddress;

  private Transceiver transceiver;

  private OrderProcessingService service;

  public SimpleOrderServiceClient(InetSocketAddress endpointAddress) {
    this.endpointAddress = endpointAddress;
  }

  public synchronized void start() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Starting Simple Ordering Netty client on '{}'", endpointAddress);
    }
    transceiver = new NettyTransceiver(endpointAddress);
    service = SpecificRequestor.getClient(OrderProcessingService.class, transceiver);
  }

  public void stop() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Stopping Simple Ordering Netty client on '{}'", endpointAddress);
    }
    if (transceiver != null && transceiver.isConnected()) {
      transceiver.close();
    }
  }

  @Override
  public Confirmation submitOrder(Order order) throws OrderFailure {
    return service.submitOrder(order);
  }

}
