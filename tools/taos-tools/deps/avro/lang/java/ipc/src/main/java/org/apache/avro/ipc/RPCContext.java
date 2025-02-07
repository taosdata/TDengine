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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Protocol.Message;

/**
 * This class represents the context of an RPC call or RPC handshake. Designed
 * to provide information to RPC plugin writers, this class encapsulates
 * information about the rpc exchange, including handshake and call metadata.
 * Note: this data includes full copies of the RPC payload, so plugins which
 * store RPCContexts beyond the life of each call should be conscious of memory
 * use.
 *
 */
public class RPCContext {

  private HandshakeRequest handshakeRequest;
  private HandshakeResponse handshakeResponse;

  protected Map<String, ByteBuffer> requestCallMeta, responseCallMeta;

  protected Object response;
  protected Exception error;
  private Message message;
  List<ByteBuffer> requestPayload;
  List<ByteBuffer> responsePayload;

  /** Set the handshake request of this RPC. */
  public void setHandshakeRequest(HandshakeRequest handshakeRequest) {
    this.handshakeRequest = handshakeRequest;
  }

  /** Get the handshake request of this RPC. */
  public HandshakeRequest getHandshakeRequest() {
    return this.handshakeRequest;
  }

  /** Set the handshake response of this RPC. */
  public void setHandshakeResponse(HandshakeResponse handshakeResponse) {
    this.handshakeResponse = handshakeResponse;
  }

  /** Get the handshake response of this RPC. */
  public HandshakeResponse getHandshakeResponse() {
    return this.handshakeResponse;
  }

  /**
   * This is an access method for the handshake state provided by the client to
   * the server.
   * 
   * @return a map representing handshake state from the client to the server
   */
  public Map<String, ByteBuffer> requestHandshakeMeta() {
    if (handshakeRequest.getMeta() == null)
      handshakeRequest.setMeta(new HashMap<>());
    return handshakeRequest.getMeta();
  }

  void setRequestHandshakeMeta(Map<String, ByteBuffer> newmeta) {
    handshakeRequest.setMeta(newmeta);
  }

  /**
   * This is an access method for the handshake state provided by the server back
   * to the client
   * 
   * @return a map representing handshake state from the server to the client
   */
  public Map<String, ByteBuffer> responseHandshakeMeta() {
    if (handshakeResponse.getMeta() == null)
      handshakeResponse.setMeta(new HashMap<>());
    return handshakeResponse.getMeta();
  }

  void setResponseHandshakeMeta(Map<String, ByteBuffer> newmeta) {
    handshakeResponse.setMeta(newmeta);
  }

  /**
   * This is an access method for the per-call state provided by the client to the
   * server.
   * 
   * @return a map representing per-call state from the client to the server
   */
  public Map<String, ByteBuffer> requestCallMeta() {
    if (requestCallMeta == null) {
      requestCallMeta = new HashMap<>();
    }
    return requestCallMeta;
  }

  void setRequestCallMeta(Map<String, ByteBuffer> newmeta) {
    requestCallMeta = newmeta;
  }

  /**
   * This is an access method for the per-call state provided by the server back
   * to the client.
   * 
   * @return a map representing per-call state from the server to the client
   */
  public Map<String, ByteBuffer> responseCallMeta() {
    if (responseCallMeta == null) {
      responseCallMeta = new HashMap<>();
    }
    return responseCallMeta;
  }

  void setResponseCallMeta(Map<String, ByteBuffer> newmeta) {
    responseCallMeta = newmeta;
  }

  void setResponse(Object response) {
    this.response = response;
    this.error = null;
  }

  /**
   * The response object generated at the server, if it exists. If an exception
   * was generated, this will be null.
   * 
   * @return the response created by this RPC, no null if an exception was
   *         generated
   */
  public Object response() {
    return response;
  }

  void setError(Exception error) {
    this.response = null;
    this.error = error;
  }

  /**
   * The exception generated at the server, or null if no such exception has
   * occurred
   * 
   * @return the exception generated at the server, or null if no such exception
   */
  public Exception error() {
    return error;
  }

  /**
   * Indicates whether an exception was generated at the server
   * 
   * @return true is an exception was generated at the server, or false if not
   */
  public boolean isError() {
    return error != null;
  }

  /** Sets the {@link Message} corresponding to this RPC */
  public void setMessage(Message message) {
    this.message = message;
  }

  /**
   * Returns the {@link Message} corresponding to this RPC
   * 
   * @return this RPC's {@link Message}
   */
  public Message getMessage() {
    return message;
  }

  /**
   * Sets the serialized payload of the request in this RPC. Will not include
   * handshake or meta-data.
   */
  public void setRequestPayload(List<ByteBuffer> payload) {
    this.requestPayload = payload;
  }

  /**
   * Returns the serialized payload of the request in this RPC. Will only be
   * generated from a Requestor and will not include handshake or meta-data. If
   * the request payload has not been set yet, returns null.
   *
   * @return this RPC's request payload.
   */
  public List<ByteBuffer> getRequestPayload() {
    return this.requestPayload;
  }

  /**
   * Returns the serialized payload of the response in this RPC. Will only be
   * generated from a Responder and will not include handshake or meta-data. If
   * the response payload has not been set yet, returns null.
   *
   * @return this RPC's response payload.
   */
  public List<ByteBuffer> getResponsePayload() {
    return this.responsePayload;
  }

  /**
   * Sets the serialized payload of the response in this RPC. Will not include
   * handshake or meta-data.
   */
  public void setResponsePayload(List<ByteBuffer> payload) {
    this.responsePayload = payload;
  }
}
