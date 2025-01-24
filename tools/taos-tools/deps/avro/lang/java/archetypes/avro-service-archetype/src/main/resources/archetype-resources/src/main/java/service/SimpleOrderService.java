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

package ${package}.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code SimpleOrderService} is a simple example implementation of an Avro service generated from the
 * order-service.avpr protocol definition.
 */
public class SimpleOrderService implements OrderProcessingService {

  private Logger log = LoggerFactory.getLogger(SimpleOrderService.class);

  @Override
  public Confirmation submitOrder(Order order) throws OrderFailure {
    log.info("Received order for '{}' items from customer with id '{}'",
      new Object[] {order.getOrderItems().size(), order.getCustomerId()});

    long estimatedCompletion = System.currentTimeMillis() + (5 * 60 * 60);
    return Confirmation.newBuilder().setCustomerId(order.getCustomerId()).setEstimatedCompletion(estimatedCompletion)
      .setOrderId(order.getOrderId()).build();
  }
}
