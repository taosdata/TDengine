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

import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.Arrays;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

/** Component that sets up a gRPC client for Avro's IDL and Serialization. */
public abstract class AvroGrpcClient {

  private AvroGrpcClient() {
  }

  /**
   * Creates a gRPC client for Avro's interface with default {@link CallOptions}.
   *
   * @param channel the channel used for gRPC {@link ClientCalls}.
   * @param iface   Avro interface for which client is built.
   * @param <T>     type of Avro Interface.
   * @return a new client proxy.
   */
  public static <T> T create(Channel channel, Class<T> iface) {
    return create(channel, iface, CallOptions.DEFAULT);
  }

  /**
   * Creates a gRPC client for Avro's interface with provided {@link CallOptions}.
   *
   * @param channel     the channel used for gRPC {@link ClientCalls}.
   * @param iface       Avro interface for which client is built.
   * @param callOptions client call options for gRPC.
   * @param <T>         type of Avro Interface.
   * @return a new client proxy.
   */
  public static <T> T create(Channel channel, Class<T> iface, CallOptions callOptions) {
    Protocol protocol = AvroGrpcUtils.getProtocol(iface);
    ServiceDescriptor serviceDescriptor = ServiceDescriptor.create(iface);
    ServiceInvocationHandler proxyHandler = new ServiceInvocationHandler(channel, callOptions, protocol,
        serviceDescriptor);
    return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class[] { iface }, proxyHandler);
  }

  private static class ServiceInvocationHandler implements InvocationHandler {
    private final Channel channel;
    private final CallOptions callOptions;
    private final ServiceDescriptor serviceDescriptor;

    ServiceInvocationHandler(Channel channel, CallOptions callOptions, Protocol protocol,
        ServiceDescriptor serviceDescriptor) {
      this.channel = channel;
      this.callOptions = callOptions;
      this.serviceDescriptor = serviceDescriptor;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return invokeUnaryMethod(method, args);
      } catch (RuntimeException re) {
        // rethrow any runtime exception
        throw re;
      } catch (Exception e) {
        // throw any of the declared exceptions
        for (Class<?> exceptionClass : method.getExceptionTypes()) {
          if (exceptionClass.isInstance(e)) {
            throw e;
          }
        }
        // wrap all other exceptions
        throw new AvroRemoteException(e);
      }
    }

    private Object invokeUnaryMethod(Method method, Object[] args) throws Exception {
      Type[] parameterTypes = method.getParameterTypes();
      if ((parameterTypes.length > 0) && (parameterTypes[parameterTypes.length - 1] instanceof Class)
          && Callback.class.isAssignableFrom(((Class<?>) parameterTypes[parameterTypes.length - 1]))) {
        // get the callback argument from the end
        Object[] finalArgs = Arrays.copyOf(args, args.length - 1);
        Callback<?> callback = (Callback<?>) args[args.length - 1];
        unaryRequest(method.getName(), finalArgs, callback);
        return null;
      } else {
        return unaryRequest(method.getName(), args);
      }
    }

    private Object unaryRequest(String methodName, Object[] args) throws Exception {
      CallFuture<Object> callFuture = new CallFuture<>();
      unaryRequest(methodName, args, callFuture);
      try {
        return callFuture.get();
      } catch (Exception e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        }
        throw new AvroRemoteException(e.getCause());
      }
    }

    private <RespT> void unaryRequest(String methodName, Object[] args, Callback<RespT> callback) throws Exception {
      StreamObserver<Object> observerAdpater = new CallbackToResponseStreamObserverAdpater<>(callback);
      ClientCalls.asyncUnaryCall(
          channel.newCall(serviceDescriptor.getMethod(methodName, MethodDescriptor.MethodType.UNARY), callOptions),
          args, observerAdpater);
    }

    private static class CallbackToResponseStreamObserverAdpater<T> implements StreamObserver<Object> {
      private final Callback<T> callback;

      CallbackToResponseStreamObserverAdpater(Callback<T> callback) {
        this.callback = callback;
      }

      @Override
      public void onNext(Object value) {
        if (value instanceof Throwable) {
          callback.handleError((Throwable) value);
        } else {
          callback.handleResult((T) value);
        }
      }

      @Override
      public void onError(Throwable t) {
        callback.handleError(new AvroRuntimeException(t));
      }

      @Override
      public void onCompleted() {
        // do nothing as there is no equivalent in Callback.
      }
    }
  }
}
