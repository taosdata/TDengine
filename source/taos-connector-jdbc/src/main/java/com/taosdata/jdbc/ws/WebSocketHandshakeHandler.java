package com.taosdata.jdbc.ws;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketHandshakeHandler extends ChannelInboundHandlerAdapter {
    private static final Logger log = LoggerFactory.getLogger(WebSocketHandshakeHandler.class);

    private final WebSocketClientHandshaker handshaker;
    private ChannelPromise handshakeFuture;
    public WebSocketHandshakeHandler(WebSocketClientHandshaker handshaker) {
        this.handshaker = handshaker;
        this.handshakeFuture = null;
    }

    public ChannelFuture handshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // initialize handshake
        handshaker.handshake(ctx.channel());
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!handshaker.isHandshakeComplete()) {
            try {
                handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                log.error("handshake failed: ", e);
                handshakeFuture.setFailure(e);
                ctx.close();
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }


}