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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyDataPack;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyFrameDecoder;
import org.apache.avro.ipc.netty.NettyTransportCodec.NettyFrameEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty-based RPC {@link Server} implementation.
 */
public class NettyServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class.getName());

  private final Responder responder;

  private final Channel serverChannel;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup callerGroup;
  private final CountDownLatch closed = new CountDownLatch(1);
  private final AtomicInteger activeCount = new AtomicInteger(0);

  public NettyServer(Responder responder, InetSocketAddress addr) throws InterruptedException {
    this(responder, addr, null);
  }

  public NettyServer(Responder responder, InetSocketAddress addr, final Consumer<SocketChannel> initializer)
      throws InterruptedException {
    this(responder, addr, initializer, null, null, null, null);
  }

  public NettyServer(Responder responder, InetSocketAddress addr, final Consumer<SocketChannel> initializer,
      final Consumer<ServerBootstrap> bootStrapInitialzier) throws InterruptedException {
    this(responder, addr, initializer, bootStrapInitialzier, null, null, null);
  }

  public NettyServer(Responder responder, InetSocketAddress addr, final Consumer<SocketChannel> initializer,
      final Consumer<ServerBootstrap> bootStrapInitialzier, EventLoopGroup bossGroup, EventLoopGroup workerGroup,
      EventLoopGroup callerGroup) throws InterruptedException {
    this.bossGroup = bossGroup == null ? new NioEventLoopGroup(1) : bossGroup;
    this.workerGroup = workerGroup == null ? new NioEventLoopGroup(10) : workerGroup;
    this.callerGroup = callerGroup == null ? new DefaultEventLoopGroup(16) : callerGroup;
    this.responder = responder;
    ServerBootstrap bootstrap = new ServerBootstrap().group(this.bossGroup, this.workerGroup)
        .channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            if (initializer != null) {
              initializer.accept(ch);
            }
            ch.pipeline().addLast("frameDecoder", new NettyFrameDecoder())
                .addLast("frameEncoder", new NettyFrameEncoder()).addLast("handler", new NettyServerAvroHandler());
          }
        }).option(ChannelOption.SO_BACKLOG, 1024).childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true);

    if (bootStrapInitialzier != null) {
      bootStrapInitialzier.accept(bootstrap);
    }
    serverChannel = bootstrap.bind(addr).sync().channel();
  }

  @Override
  public void start() {
    // No-op.
  }

  @Override
  public void close() {
    workerGroup.shutdownGracefully().syncUninterruptibly();
    bossGroup.shutdownGracefully().syncUninterruptibly();
    try {
      serverChannel.closeFuture().sync();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    closed.countDown();
  }

  @Override
  public int getPort() {
    return ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  /**
   *
   * @return The number of clients currently connected to this server.
   */
  public int getNumActiveConnections() {
    return activeCount.get();
  }

  /**
   * Avro server handler for the Netty transport
   */
  class NettyServerAvroHandler extends SimpleChannelInboundHandler<NettyDataPack> {

    private NettyTransceiver connectionMetadata = new NettyTransceiver();

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      activeCount.incrementAndGet();
      super.channelActive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, final NettyDataPack dataPack) throws Exception {
      callerGroup.submit(new Runnable() {
        @Override
        public void run() {
          List<ByteBuffer> req = dataPack.getDatas();
          try {
            List<ByteBuffer> res = responder.respond(req, connectionMetadata);
            // response will be null for oneway messages.
            if (res != null) {
              dataPack.setDatas(res);
              ctx.channel().writeAndFlush(dataPack);
            }
          } catch (IOException e) {
            LOG.warn("unexpected error");
          }
        }
      });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
      LOG.warn("Unexpected exception from downstream.", e);
      ctx.close().syncUninterruptibly();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      LOG.info("Connection to {} disconnected.", ctx.channel().remoteAddress());
      activeCount.decrementAndGet();
      super.channelInactive(ctx);
    }

  }
}
