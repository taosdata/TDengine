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

package org.apache.avro.grpc;

import org.apache.avro.Protocol;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

/**
 * Provides components to set up a gRPC Server for Avro's IDL and serialization.
 */
public abstract class AvroGrpcServer {

  private AvroGrpcServer() {
  }

  /**
   * Creates a {@link ServerServiceDefinition} for Avro Interface and its
   * implementation that can be passed a gRPC Server.
   *
   * @param iface Avro generated RPC service interface for which service defintion
   *              is created.
   * @param impl  Implementation of the service interface to be invoked for
   *              requests.
   * @return a new server service definition.
   */
  public static ServerServiceDefinition createServiceDefinition(Class iface, Object impl) {
    Protocol protocol = AvroGrpcUtils.getProtocol(iface);
    ServiceDescriptor serviceDescriptor = ServiceDescriptor.create(iface);
    ServerServiceDefinition.Builder serviceDefinitionBuilder = ServerServiceDefinition
        .builder(serviceDescriptor.getServiceName());
    Map<String, Protocol.Message> messages = protocol.getMessages();
    for (Method method : iface.getMethods()) {
      Protocol.Message msg = messages.get(method.getName());
      // setup a method handler only if corresponding message exists in avro protocol.
      if (msg != null) {
        UnaryMethodHandler methodHandler = msg.isOneWay() ? new OneWayUnaryMethodHandler(impl, method)
            : new UnaryMethodHandler(impl, method);
        serviceDefinitionBuilder.addMethod(
            serviceDescriptor.getMethod(method.getName(), MethodDescriptor.MethodType.UNARY),
            ServerCalls.asyncUnaryCall(methodHandler));
      }
    }
    return serviceDefinitionBuilder.build();
  }

  private static class UnaryMethodHandler implements ServerCalls.UnaryMethod<Object[], Object> {
    private final Object serviceImpl;
    private final Method method;

    UnaryMethodHandler(Object serviceImpl, Method method) {
      this.serviceImpl = serviceImpl;
      this.method = method;
    }

    @Override
    public void invoke(Object[] request, StreamObserver<Object> responseObserver) {
      Object methodResponse = null;
      try {
        methodResponse = method.invoke(getServiceImpl(), request);
      } catch (InvocationTargetException e) {
        methodResponse = e.getTargetException();
      } catch (Exception e) {
        methodResponse = e;
      }
      responseObserver.onNext(methodResponse);
      responseObserver.onCompleted();
    }

    public Method getMethod() {
      return method;
    }

    public Object getServiceImpl() {
      return serviceImpl;
    }
  }

  private static class OneWayUnaryMethodHandler extends UnaryMethodHandler {
    private static final Logger LOG = Logger.getLogger(OneWayUnaryMethodHandler.class.getName());

    OneWayUnaryMethodHandler(Object serviceImpl, Method method) {
      super(serviceImpl, method);
    }

    @Override
    public void invoke(Object[] request, StreamObserver<Object> responseObserver) {
      // first respond back with a fixed void response in order for call to be
      // complete
      responseObserver.onNext(null);
      responseObserver.onCompleted();
      // process the rpc request
      try {
        getMethod().invoke(getServiceImpl(), request);
      } catch (Exception e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause != cause.getCause()) {
          cause = cause.getCause();
        }
        LOG.log(Level.WARNING, "Error processing one-way rpc", cause);
      }
    }
  }
}
