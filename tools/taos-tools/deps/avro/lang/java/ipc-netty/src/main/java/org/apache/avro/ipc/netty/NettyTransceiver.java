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

package org.apache.avro.ipc.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyDataPack;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyFrameDecoder;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyFrameEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty-based {@link Transceiver} implementation.
 */
public class NettyTransceiver extends Transceiver {
  /** If not specified, the default connection timeout will be used (60 sec). */
  public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 60 * 1000;
  public static final String NETTY_CONNECT_TIMEOUT_OPTION = "connectTimeoutMillis";
  public static final String NETTY_TCP_NODELAY_OPTION = "tcpNoDelay";
  public static final String NETTY_KEEPALIVE_OPTION = "keepAlive";
  public static final boolean DEFAULT_TCP_NODELAY_VALUE = true;

  private static final Logger LOG = LoggerFactory.getLogger(NettyTransceiver.class.getName());

  private final AtomicInteger serialGenerator = new AtomicInteger(0);
  private final Map<Integer, Callback<List<ByteBuffer>>> requests = new ConcurrentHashMap<>();

  private final Integer connectTimeoutMillis;
  private final Bootstrap bootstrap;
  private final InetSocketAddress remoteAddr;
  private final EventLoopGroup workerGroup;

  volatile ChannelFuture channelFuture;
  volatile boolean stopping;
  private final Object channelFutureLock = new Object();

  /**
   * Read lock must be acquired whenever using non-final state. Write lock must be
   * acquired whenever modifying state.
   */
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private Channel channel; // Synchronized on stateLock
  private Protocol remote; // Synchronized on stateLock

  NettyTransceiver() {
    connectTimeoutMillis = 0;
    bootstrap = null;
    remoteAddr = null;
    channelFuture = null;
    workerGroup = null;
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection
   * timeout.
   * 
   * @param addr the address to connect to.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr) throws IOException {
    this(addr, DEFAULT_CONNECTION_TIMEOUT_MILLIS);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr                 the address to connect to.
   * @param connectTimeoutMillis maximum amount of time to wait for connection
   *                             establishment in milliseconds, or null to use
   *                             {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, Integer connectTimeoutMillis) throws IOException {
    this(addr, connectTimeoutMillis, null, null);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr        the address to connect to.
   * @param initializer Consumer function to apply initial setup to the
   *                    SocketChannel. Useablet to set things like SSL
   *                    requirements, compression, etc...
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, final Consumer<SocketChannel> initializer) throws IOException {
    this(addr, DEFAULT_CONNECTION_TIMEOUT_MILLIS, initializer, null);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr                 the address to connect to.
   * @param connectTimeoutMillis maximum amount of time to wait for connection
   *                             establishment in milliseconds, or null to use
   *                             {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @param initializer          Consumer function to apply initial setup to the
   *                             SocketChannel. Usable to set things like SSL
   *                             requirements, compression, etc...
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, Integer connectTimeoutMillis,
      final Consumer<SocketChannel> initializer) throws IOException {
    this(addr, connectTimeoutMillis, initializer, null);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr                 the address to connect to.
   * @param connectTimeoutMillis maximum amount of time to wait for connection
   *                             establishment in milliseconds, or null to use
   *                             {@link #DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @param initializer          Consumer function to apply initial setup to the
   *                             SocketChannel. Usable to set things like SSL
   *                             requirements, compression, etc...
   * @param bootStrapInitialzier Consumer function to apply initial setup to the
   *                             Bootstrap. Usable to set things like tcp
   *                             connection properties, nagle algorithm, etc...
   * @throws IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, Integer connectTimeoutMillis,
      final Consumer<SocketChannel> initializer, final Consumer<Bootstrap> bootStrapInitialzier) throws IOException {
    // Set up.
    if (connectTimeoutMillis == null) {
      connectTimeoutMillis = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    }
    this.connectTimeoutMillis = connectTimeoutMillis;
    workerGroup = new NioEventLoopGroup(new NettyTransceiverThreadFactory("avro"));
    bootstrap = new Bootstrap().group(workerGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
        .option(ChannelOption.TCP_NODELAY, DEFAULT_TCP_NODELAY_VALUE).handler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            if (initializer != null) {
              initializer.accept(ch);
            }
            ch.pipeline().addLast("frameDecoder", new NettyFrameDecoder())
                .addLast("frameEncoder", new NettyFrameEncoder()).addLast("handler", createNettyClientAvroHandler());
          }
        });
    if (bootStrapInitialzier != null) {
      bootStrapInitialzier.accept(bootstrap);
    }

    remoteAddr = addr;

    // Make a new connection.
    stateLock.readLock().lock();
    try {
      getChannel();
    } catch (Throwable e) {
      // must attempt to clean up any allocated channel future
      if (channelFuture != null) {
        channelFuture.channel().close();
      }

      workerGroup.shutdownGracefully();

      if (e instanceof IOException)
        throw (IOException) e;
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      // all that's left is Error
      throw (Error) e;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Creates a Netty ChannelUpstreamHandler for handling events on the Netty
   * client channel.
   * 
   * @return the ChannelUpstreamHandler to use.
   */
  protected ChannelInboundHandler createNettyClientAvroHandler() {
    return new NettyClientAvroHandler();
  }

  /**
   * Tests whether the given channel is ready for writing.
   * 
   * @return true if the channel is open and ready; false otherwise.
   */
  private static boolean isChannelReady(Channel channel) {
    return (channel != null) && channel.isOpen() && channel.isActive();
  }

  /**
   * Gets the Netty channel. If the channel is not connected, first attempts to
   * connect. NOTE: The stateLock read lock *must* be acquired before calling this
   * method.
   * 
   * @return the Netty channel
   * @throws IOException if an error occurs connecting the channel.
   */
  private Channel getChannel() throws IOException {
    if (!isChannelReady(channel)) {
      // Need to reconnect
      // Upgrade to write lock
      stateLock.readLock().unlock();
      stateLock.writeLock().lock();
      try {
        if (!isChannelReady(channel)) {
          synchronized (channelFutureLock) {
            if (!stopping) {
              LOG.debug("Connecting to {}", remoteAddr);
              channelFuture = bootstrap.connect(remoteAddr);
            }
          }
          if (channelFuture != null) {
            try {
              channelFuture.await(connectTimeoutMillis);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt(); // Reset interrupt flag
              throw new IOException("Interrupted while connecting to " + remoteAddr);
            }

            synchronized (channelFutureLock) {
              if (!channelFuture.isSuccess()) {
                remote = null;
                throw new IOException("Error connecting to " + remoteAddr, channelFuture.cause());
              }
              channel = channelFuture.channel();
              channelFuture = null;
            }
          }
        }
      } finally {
        // Downgrade to read lock:
        stateLock.readLock().lock();
        stateLock.writeLock().unlock();
      }
    }
    return channel;
  }

  /**
   * Closes the connection to the remote peer if connected.
   * 
   * @param awaitCompletion       if true, will block until the close has
   *                              completed.
   * @param cancelPendingRequests if true, will drain the requests map and send an
   *                              IOException to all Callbacks.
   * @param cause                 if non-null and cancelPendingRequests is true,
   *                              this Throwable will be passed to all Callbacks.
   */
  private void disconnect(boolean awaitCompletion, boolean cancelPendingRequests, Throwable cause) {
    Channel channelToClose = null;
    Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;
    boolean stateReadLockHeld = stateLock.getReadHoldCount() != 0;

    ChannelFuture channelFutureToCancel = null;
    synchronized (channelFutureLock) {
      if (stopping && channelFuture != null) {
        channelFutureToCancel = channelFuture;
        channelFuture = null;
      }
    }
    if (channelFutureToCancel != null) {
      channelFutureToCancel.cancel(true);
    }

    if (stateReadLockHeld) {
      stateLock.readLock().unlock();
    }
    stateLock.writeLock().lock();
    try {
      if (channel != null) {
        if (cause != null) {
          LOG.debug("Disconnecting from {}", remoteAddr, cause);
        } else {
          LOG.debug("Disconnecting from {}", remoteAddr);
        }
        channelToClose = channel;
        channel = null;
        remote = null;
        if (cancelPendingRequests) {
          // Remove all pending requests (will be canceled after relinquishing
          // write lock).
          requestsToCancel = new ConcurrentHashMap<>(requests);
          requests.clear();
        }
      }
    } finally {
      if (stateReadLockHeld) {
        stateLock.readLock().lock();
      }
      stateLock.writeLock().unlock();
    }

    // Cancel any pending requests by sending errors to the callbacks:
    if ((requestsToCancel != null) && !requestsToCancel.isEmpty()) {
      LOG.debug("Removing {} pending request(s)", requestsToCancel.size());
      for (Callback<List<ByteBuffer>> request : requestsToCancel.values()) {
        request.handleError(cause != null ? cause : new IOException(getClass().getSimpleName() + " closed"));
      }
    }

    // Close the channel:
    if (channelToClose != null) {
      ChannelFuture closeFuture = channelToClose.close();
      if (awaitCompletion && (closeFuture != null)) {
        try {
          closeFuture.await(connectTimeoutMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Reset interrupt flag
          LOG.warn("Interrupted while disconnecting", e);
        }
      }
    }
  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This
   * method is a no-op.
   */
  @Override
  public void lockChannel() {

  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This
   * method is a no-op.
   */
  @Override
  public void unlockChannel() {

  }

  /**
   * Closes this transceiver and disconnects from the remote peer. Cancels all
   * pending RPCs, sends an IOException to all pending callbacks, and blocks until
   * the close has completed.
   */
  @Override
  public void close() {
    close(true);
  }

  /**
   * Closes this transceiver and disconnects from the remote peer. Cancels all
   * pending RPCs and sends an IOException to all pending callbacks.
   * 
   * @param awaitCompletion if true, will block until the close has completed.
   */
  public void close(boolean awaitCompletion) {
    try {
      // Close the connection:
      stopping = true;
      disconnect(awaitCompletion, true, null);
    } finally {
      // Shut down all thread pools to exit.
      if (workerGroup != null) {
        workerGroup.shutdownGracefully();
      }
    }
  }

  @Override
  public String getRemoteName() throws IOException {
    stateLock.readLock().lock();
    try {
      return getChannel().remoteAddress().toString();
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Override as non-synchronized method because the method is thread safe.
   */
  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request) throws IOException {
    try {
      CallFuture<List<ByteBuffer>> transceiverFuture = new CallFuture<>();
      transceive(request, transceiverFuture);
      return transceiverFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.debug("failed to get the response", e);
      return null;
    }
  }

  @Override
  public void transceive(List<ByteBuffer> request, Callback<List<ByteBuffer>> callback) throws IOException {
    stateLock.readLock().lock();
    try {
      int serial = serialGenerator.incrementAndGet();
      NettyDataPack dataPack = new NettyDataPack(serial, request);
      requests.put(serial, callback);
      writeDataPack(dataPack);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    ChannelFuture writeFuture;
    stateLock.readLock().lock();
    try {
      writeFuture = writeDataPack(new NettyDataPack(serialGenerator.incrementAndGet(), buffers));
    } finally {
      stateLock.readLock().unlock();
    }

    if (!writeFuture.isDone()) {
      try {
        writeFuture.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Reset interrupt flag
        throw new IOException("Interrupted while writing Netty data pack", e);
      }
    }
    if (!writeFuture.isSuccess()) {
      throw new IOException("Error writing buffers", writeFuture.cause());
    }
  }

  /**
   * Writes a NettyDataPack, reconnecting to the remote peer if necessary. NOTE:
   * The stateLock read lock *must* be acquired before calling this method.
   * 
   * @param dataPack the data pack to write.
   * @return the Netty ChannelFuture for the write operation.
   * @throws IOException if an error occurs connecting to the remote peer.
   */
  private ChannelFuture writeDataPack(NettyDataPack dataPack) throws IOException {
    return getChannel().writeAndFlush(dataPack);
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Protocol getRemote() {
    stateLock.readLock().lock();
    try {
      return remote;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public boolean isConnected() {
    stateLock.readLock().lock();
    try {
      return remote != null;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public void setRemote(Protocol protocol) {
    stateLock.writeLock().lock();
    try {
      this.remote = protocol;
    } finally {
      stateLock.writeLock().unlock();
    }
  }

  /**
   * A ChannelFutureListener for channel write operations that notifies a
   * {@link Callback} if an error occurs while writing to the channel.
   */
  protected static class WriteFutureListener implements ChannelFutureListener {
    protected final Callback<List<ByteBuffer>> callback;

    /**
     * Creates a WriteFutureListener that notifies the given callback if an error
     * occurs writing data to the channel.
     * 
     * @param callback the callback to notify, or null to skip notification.
     */
    public WriteFutureListener(Callback<List<ByteBuffer>> callback) {
      this.callback = callback;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!future.isSuccess() && (callback != null)) {
        callback.handleError(new IOException("Error writing buffers", future.cause()));
      }
    }
  }

  /**
   * Avro client handler for the Netty transport
   */
  protected class NettyClientAvroHandler extends SimpleChannelInboundHandler<NettyDataPack> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NettyDataPack dataPack) throws Exception {
      Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
      if (callback == null) {
        throw new RuntimeException("Missing previous call info");
      }
      try {
        callback.handleResult(dataPack.getDatas());
      } finally {
        requests.remove(dataPack.getSerial());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      disconnect(false, true, e);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (!ctx.channel().isOpen()) {
        LOG.info("Connection to {} disconnected.", ctx.channel().remoteAddress());
        disconnect(false, true, null);
      }
      super.channelInactive(ctx);
    }

  }

  /**
   * Creates threads with unique names based on a specified name prefix.
   */
  protected static class NettyTransceiverThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String prefix;

    /**
     * Creates a NettyTransceiverThreadFactory that creates threads with the
     * specified name.
     * 
     * @param prefix the name prefix to use for all threads created by this
     *               ThreadFactory. A unique ID will be appended to this prefix to
     *               form the final thread name.
     */
    public NettyTransceiverThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(prefix + " " + threadId.incrementAndGet());
      return thread;
    }
  }
}
