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
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for the client side of a protocol interaction. */
public abstract class Requestor {
  private static final Logger LOG = LoggerFactory.getLogger(Requestor.class);

  private static final Schema META = Schema.createMap(Schema.create(Schema.Type.BYTES));
  private static final GenericDatumReader<Map<String, ByteBuffer>> META_READER = new GenericDatumReader<>(META);
  private static final GenericDatumWriter<Map<String, ByteBuffer>> META_WRITER = new GenericDatumWriter<>(META);

  private final Protocol local;
  private volatile Protocol remote;
  private volatile boolean sendLocalText;
  private final Transceiver transceiver;
  private final ReentrantLock handshakeLock = new ReentrantLock();

  protected final List<RPCPlugin> rpcMetaPlugins;

  public Protocol getLocal() {
    return local;
  }

  public Transceiver getTransceiver() {
    return transceiver;
  }

  protected Requestor(Protocol local, Transceiver transceiver) throws IOException {
    this.local = local;
    this.transceiver = transceiver;
    this.rpcMetaPlugins = new CopyOnWriteArrayList<>();
  }

  /**
   * Adds a new plugin to manipulate RPC metadata. Plugins are executed in the
   * order that they are added.
   *
   * @param plugin a plugin that will manipulate RPC metadata
   */
  public void addRPCPlugin(RPCPlugin plugin) {
    rpcMetaPlugins.add(plugin);
  }

  private static final EncoderFactory ENCODER_FACTORY = new EncoderFactory();

  /** Writes a request message and reads a response or error message. */
  public Object request(String messageName, Object request) throws Exception {
    // Initialize request
    Request rpcRequest = new Request(messageName, request, new RPCContext());
    CallFuture<Object> future = /* only need a Future for two-way messages */
        rpcRequest.getMessage().isOneWay() ? null : new CallFuture<>();

    // Send request
    request(rpcRequest, future);

    if (future == null) // the message is one-way, so return immediately
      return null;
    try { // the message is two-way, wait for the result
      return future.get();
    } catch (ExecutionException e) {
      Throwable error = e.getCause();
      if (error instanceof Exception) {
        throw (Exception) error;
      } else {
        throw new AvroRuntimeException(error);
      }
    }
  }

  /**
   * Writes a request message and returns the result through a Callback. Clients
   * can also use a Future interface by creating a new CallFuture<T>, passing it
   * in as the Callback parameter, and then waiting on that Future.
   *
   * @param <T>         the return type of the message.
   * @param messageName the name of the message to invoke.
   * @param request     the request data to send.
   * @param callback    the callback which will be invoked when the response is
   *                    returned or an error occurs.
   * @throws AvroRemoteException  if an exception is thrown to client by server.
   * @throws IOException          if an I/O error occurs while sending the
   *                              message.
   * @throws AvroRuntimeException for another undeclared error while sending the
   *                              message.
   */
  public <T> void request(String messageName, Object request, Callback<T> callback)
      throws AvroRemoteException, IOException {
    request(new Request(messageName, request, new RPCContext()), callback);
  }

  /** Writes a request message and returns the result through a Callback. */
  <T> void request(Request request, Callback<T> callback) throws AvroRemoteException, IOException {
    Transceiver t = getTransceiver();
    if (!t.isConnected()) {
      // Acquire handshake lock so that only one thread is performing the
      // handshake and other threads block until the handshake is completed
      handshakeLock.lock();
      try {
        if (t.isConnected()) {
          // Another thread already completed the handshake; no need to hold
          // the write lock
          handshakeLock.unlock();
        } else {
          CallFuture<T> callFuture = new CallFuture<>(callback);
          t.transceive(request.getBytes(), new TransceiverCallback<>(request, callFuture));
          try {
            // Block until handshake complete
            callFuture.await();
          } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
          }
          if (request.getMessage().isOneWay()) {
            Throwable error = callFuture.getError();
            if (error != null) {
              if (error instanceof AvroRemoteException) {
                throw (AvroRemoteException) error;
              } else if (error instanceof AvroRuntimeException) {
                throw (AvroRuntimeException) error;
              } else if (error instanceof IOException) {
                throw (IOException) error;
              } else {
                throw new AvroRuntimeException(error);
              }
            }
          }
          return;
        }
      } finally {
        if (handshakeLock.isHeldByCurrentThread()) {
          handshakeLock.unlock();
        }
      }
    }

    if (request.getMessage().isOneWay()) {
      t.lockChannel();
      try {
        t.writeBuffers(request.getBytes());
        if (callback != null) {
          callback.handleResult(null);
        }
      } finally {
        t.unlockChannel();
      }
    } else {
      t.transceive(request.getBytes(), new TransceiverCallback<>(request, callback));
    }

  }

  private static final ConcurrentMap<String, MD5> REMOTE_HASHES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<MD5, Protocol> REMOTE_PROTOCOLS = new ConcurrentHashMap<>();

  private static final SpecificDatumWriter<HandshakeRequest> HANDSHAKE_WRITER = new SpecificDatumWriter<>(
      HandshakeRequest.class);

  private static final SpecificDatumReader<HandshakeResponse> HANDSHAKE_READER = new SpecificDatumReader<>(
      HandshakeResponse.class);

  private void writeHandshake(Encoder out) throws IOException {
    if (getTransceiver().isConnected())
      return;
    MD5 localHash = new MD5();
    localHash.bytes(local.getMD5());
    String remoteName = transceiver.getRemoteName();
    MD5 remoteHash = REMOTE_HASHES.get(remoteName);
    if (remoteHash == null) { // guess remote is local
      remoteHash = localHash;
      remote = local;
    } else {
      remote = REMOTE_PROTOCOLS.get(remoteHash);
    }
    HandshakeRequest handshake = new HandshakeRequest();
    handshake.setClientHash(localHash);
    handshake.setServerHash(remoteHash);
    if (sendLocalText)
      handshake.setClientProtocol(local.toString());

    RPCContext context = new RPCContext();
    context.setHandshakeRequest(handshake);
    for (RPCPlugin plugin : rpcMetaPlugins) {
      plugin.clientStartConnect(context);
    }
    handshake.setMeta(context.requestHandshakeMeta());

    HANDSHAKE_WRITER.write(handshake, out);
  }

  private boolean readHandshake(Decoder in) throws IOException {
    if (getTransceiver().isConnected())
      return true;
    boolean established = false;
    HandshakeResponse handshake = HANDSHAKE_READER.read(null, in);
    switch (handshake.getMatch()) {
    case BOTH:
      established = true;
      sendLocalText = false;
      break;
    case CLIENT:
      LOG.debug("Handshake match = CLIENT");
      setRemote(handshake);
      established = true;
      sendLocalText = false;
      break;
    case NONE:
      LOG.debug("Handshake match = NONE");
      setRemote(handshake);
      sendLocalText = true;
      break;
    default:
      throw new AvroRuntimeException("Unexpected match: " + handshake.getMatch());
    }

    RPCContext context = new RPCContext();
    context.setHandshakeResponse(handshake);
    for (RPCPlugin plugin : rpcMetaPlugins) {
      plugin.clientFinishConnect(context);
    }
    if (established)
      getTransceiver().setRemote(remote);
    return established;
  }

  private void setRemote(HandshakeResponse handshake) throws IOException {
    remote = Protocol.parse(handshake.getServerProtocol().toString());
    MD5 remoteHash = handshake.getServerHash();
    REMOTE_HASHES.put(transceiver.getRemoteName(), remoteHash);
    REMOTE_PROTOCOLS.putIfAbsent(remoteHash, remote);
  }

  /** Return the remote protocol. Force a handshake if required. */
  public Protocol getRemote() throws IOException {
    if (remote != null)
      return remote; // already have it
    MD5 remoteHash = REMOTE_HASHES.get(transceiver.getRemoteName());
    if (remoteHash != null) {
      remote = REMOTE_PROTOCOLS.get(remoteHash);
      if (remote != null)
        return remote; // already cached
    }
    handshakeLock.lock();
    try {
      // force handshake
      ByteBufferOutputStream bbo = new ByteBufferOutputStream();
      // direct because the payload is tiny.
      Encoder out = ENCODER_FACTORY.directBinaryEncoder(bbo, null);
      writeHandshake(out);
      out.writeInt(0); // empty metadata
      out.writeString(""); // bogus message name
      List<ByteBuffer> response = getTransceiver().transceive(bbo.getBufferList());
      ByteBufferInputStream bbi = new ByteBufferInputStream(response);
      BinaryDecoder in = DecoderFactory.get().binaryDecoder(bbi, null);
      readHandshake(in);
      return this.remote;
    } finally {
      handshakeLock.unlock();
    }
  }

  /** Writes a request message. */
  public abstract void writeRequest(Schema schema, Object request, Encoder out) throws IOException;

  @Deprecated // for compatibility in 1.5
  public Object readResponse(Schema schema, Decoder in) throws IOException {
    return readResponse(schema, schema, in);
  }

  /** Reads a response message. */
  public abstract Object readResponse(Schema writer, Schema reader, Decoder in) throws IOException;

  @Deprecated // for compatibility in 1.5
  public Object readError(Schema schema, Decoder in) throws IOException {
    return readError(schema, schema, in);
  }

  /** Reads an error message. */
  public abstract Exception readError(Schema writer, Schema reader, Decoder in) throws IOException;

  /**
   * Handles callbacks from transceiver invocations.
   */
  protected class TransceiverCallback<T> implements Callback<List<ByteBuffer>> {
    private final Request request;
    private final Callback<T> callback;

    /**
     * Creates a TransceiverCallback.
     *
     * @param request  the request to set.
     * @param callback the callback to set.
     */
    public TransceiverCallback(Request request, Callback<T> callback) {
      this.request = request;
      this.callback = callback;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleResult(List<ByteBuffer> responseBytes) {
      ByteBufferInputStream bbi = new ByteBufferInputStream(responseBytes);
      BinaryDecoder in = DecoderFactory.get().binaryDecoder(bbi, null);
      try {
        if (!readHandshake(in)) {
          // Resend the handshake and return
          Request handshake = new Request(request);
          getTransceiver().transceive(handshake.getBytes(), new TransceiverCallback<>(handshake, callback));
          return;
        }
      } catch (Exception e) {
        LOG.error("Error handling transceiver callback: " + e, e);
      }

      // Read response; invoke callback
      Response response = new Response(request, in);
      Object responseObject;
      try {
        try {
          responseObject = response.getResponse();
        } catch (Exception e) {
          if (callback != null) {
            callback.handleError(e);
          }
          return;
        }
        if (callback != null) {
          callback.handleResult((T) responseObject);
        }
      } catch (Throwable t) {
        LOG.error("Error in callback handler: " + t, t);
      }
    }

    @Override
    public void handleError(Throwable error) {
      callback.handleError(error);
    }
  }

  /**
   * Encapsulates/generates a request.
   */
  class Request {
    private final String messageName;
    private final Object request;
    private final RPCContext context;
    private final BinaryEncoder encoder;
    private Message message;
    private List<ByteBuffer> requestBytes;

    /**
     * Creates a Request.
     *
     * @param messageName the name of the message to invoke.
     * @param request     the request data to send.
     * @param context     the RPC context to use.
     */
    public Request(String messageName, Object request, RPCContext context) {
      this(messageName, request, context, null);
    }

    /**
     * Creates a Request.
     *
     * @param messageName the name of the message to invoke.
     * @param request     the request data to send.
     * @param context     the RPC context to use.
     * @param encoder     the BinaryEncoder to use to serialize the request.
     */
    public Request(String messageName, Object request, RPCContext context, BinaryEncoder encoder) {
      this.messageName = messageName;
      this.request = request;
      this.context = context;
      this.encoder = ENCODER_FACTORY.binaryEncoder(new ByteBufferOutputStream(), encoder);
    }

    /**
     * Copy constructor.
     *
     * @param other Request from which to copy fields.
     */
    public Request(Request other) {
      this.messageName = other.messageName;
      this.request = other.request;
      this.context = other.context;
      this.encoder = other.encoder;
    }

    /**
     * Gets the message name.
     *
     * @return the message name.
     */
    public String getMessageName() {
      return messageName;
    }

    /**
     * Gets the RPC context.
     *
     * @return the RPC context.
     */
    public RPCContext getContext() {
      return context;
    }

    /**
     * Gets the Message associated with this request.
     *
     * @return this request's message.
     */
    public Message getMessage() {
      if (message == null) {
        message = getLocal().getMessages().get(messageName);
        if (message == null) {
          throw new AvroRuntimeException("Not a local message: " + messageName);
        }
      }
      return message;
    }

    /**
     * Gets the request data, generating it first if necessary.
     *
     * @return the request data.
     * @throws IOException if an error occurs generating the request data.
     */
    public List<ByteBuffer> getBytes() throws IOException {
      if (requestBytes == null) {
        ByteBufferOutputStream bbo = new ByteBufferOutputStream();
        BinaryEncoder out = ENCODER_FACTORY.binaryEncoder(bbo, encoder);

        // use local protocol to write request
        Message m = getMessage();
        context.setMessage(m);

        writeRequest(m.getRequest(), request, out); // write request payload

        out.flush();
        List<ByteBuffer> payload = bbo.getBufferList();

        writeHandshake(out); // prepend handshake if needed

        context.setRequestPayload(payload);
        for (RPCPlugin plugin : rpcMetaPlugins) {
          plugin.clientSendRequest(context); // get meta-data from plugins
        }
        META_WRITER.write(context.requestCallMeta(), out);

        out.writeString(m.getName()); // write message name

        out.flush();
        bbo.append(payload);

        requestBytes = bbo.getBufferList();
      }
      return requestBytes;
    }
  }

  /**
   * Encapsulates/parses a response.
   */
  class Response {
    private final Request request;
    private final BinaryDecoder in;

    /**
     * Creates a Response.
     *
     * @param request the Request associated with this response.
     */
    public Response(Request request) {
      this(request, null);
    }

    /**
     * Creates a Creates a Response.
     *
     * @param request the Request associated with this response.
     * @param in      the BinaryDecoder to use to deserialize the response.
     */
    public Response(Request request, BinaryDecoder in) {
      this.request = request;
      this.in = in;
    }

    /**
     * Gets the RPC response, reading/deserializing it first if necessary.
     *
     * @return the RPC response.
     * @throws Exception if an error occurs reading/deserializing the response.
     */
    public Object getResponse() throws Exception {
      Message lm = request.getMessage();
      Message rm = remote.getMessages().get(request.getMessageName());
      if (rm == null)
        throw new AvroRuntimeException("Not a remote message: " + request.getMessageName());

      Transceiver t = getTransceiver();
      if ((lm.isOneWay() != rm.isOneWay()) && t.isConnected())
        throw new AvroRuntimeException("Not both one-way messages: " + request.getMessageName());

      if (lm.isOneWay() && t.isConnected())
        return null; // one-way w/ handshake

      RPCContext context = request.getContext();
      context.setResponseCallMeta(META_READER.read(null, in));

      if (!in.readBoolean()) { // no error
        Object response = readResponse(rm.getResponse(), lm.getResponse(), in);
        context.setResponse(response);
        for (RPCPlugin plugin : rpcMetaPlugins) {
          plugin.clientReceiveResponse(context);
        }
        return response;

      } else {
        Exception error = readError(rm.getErrors(), lm.getErrors(), in);
        context.setError(error);
        for (RPCPlugin plugin : rpcMetaPlugins) {
          plugin.clientReceiveResponse(context);
        }
        throw error;
      }
    }
  }
}
