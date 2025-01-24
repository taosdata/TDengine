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

/**
 * An instrumentation API for RPC metadata. Each of these methods is invoked at
 * key points during the RPC exchange. Additionally, path-based
 * <em>metadata</em> that is passed along with the RPC call and can be set or
 * queried by subsequent instrumentation points.
 */
public class RPCPlugin {

  /**
   * Called on the client before the initial RPC handshake to setup any handshake
   * metadata for this plugin
   * 
   * @param context the handshake rpc context
   */
  public void clientStartConnect(RPCContext context) {
  }

  /**
   * Called on the server during the RPC handshake
   * 
   * @param context the handshake rpc context
   */
  public void serverConnecting(RPCContext context) {
  }

  /**
   * Called on the client after the initial RPC handshake
   * 
   * @param context the handshake rpc context
   */
  public void clientFinishConnect(RPCContext context) {
  }

  /**
   * This method is invoked at the client before it issues the RPC call.
   * 
   * @param context the per-call rpc context (in/out parameter)
   */
  public void clientSendRequest(RPCContext context) {
  }

  /**
   * This method is invoked at the RPC server when the request is received, but
   * before the call itself is executed
   * 
   * @param context the per-call rpc context (in/out parameter)
   */
  public void serverReceiveRequest(RPCContext context) {
  }

  /**
   * This method is invoked at the server before the response is executed, but
   * before the response has been formulated
   * 
   * @param context the per-call rpc context (in/out parameter)
   */
  public void serverSendResponse(RPCContext context) {
  }

  /**
   * This method is invoked at the client after the call is executed, and after
   * the client receives the response
   * 
   * @param context the per-call rpc context
   */
  public void clientReceiveResponse(RPCContext context) {
  }

}
