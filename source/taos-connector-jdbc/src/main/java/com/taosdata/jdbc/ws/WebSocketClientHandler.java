package com.taosdata.jdbc.ws;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger log = LoggerFactory.getLogger(WebSocketClientHandler.class);
    private final Consumer<String> textMessageHandler;
    private final Consumer<ByteBuf> binaryMessageHandler;

    public static final AttributeKey<Boolean> LOCAL_INITIATED_CLOSE = AttributeKey.valueOf("localInitiatedClose");
    public static final AttributeKey<Integer> CLOSE_CODE_KEY = AttributeKey.valueOf("closeCodeKey");
    public static final AttributeKey<String> REASON_KEY = AttributeKey.valueOf("reasonKey");

    public WebSocketClientHandler(Consumer<String> textMessageHandler,
                                  Consumer<ByteBuf> binaryMessageHandler) {
        this.textMessageHandler = textMessageHandler;
        this.binaryMessageHandler = binaryMessageHandler;
    }
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (ctx.channel() == null) {
            log.warn("channelInactive: ctx.channel() is null");
            return;
        }

        Boolean isLocalInitiated = ctx.channel().attr(LOCAL_INITIATED_CLOSE).get();
        isLocalInitiated = isLocalInitiated != null && isLocalInitiated;

        Integer code = ctx.channel().attr(CLOSE_CODE_KEY).get();
        code = code != null ? code : 1000; // default close code

        String reason = ctx.channel().attr(REASON_KEY).get();
        reason = reason != null ? reason : "unknown";

        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        String remoteHost = remoteAddress != null ? remoteAddress.getHostString() : "unknown-host";
        int remotePort = remoteAddress != null ? remoteAddress.getPort() : -1;
        String uri = remoteHost + ":" + remotePort;

        if (isLocalInitiated) {
            log.debug("disconnect uri: {}, code: {}, reason: {}, remote: {}", uri, code, reason, false);
        } else {
            log.error("disconnect uri: {}, code: {}, reason: {}, remote: {}", uri, code, reason, true);
        }

        ctx.fireChannelInactive();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel ch = ctx.channel();
        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            if (response.status().code() != 101) { // 101 is the status code for switching protocols
                String content = response.content().toString(CharsetUtil.UTF_8);
                log.error("WebSocket handshake error，code: {}, msg: {}", response.status(), content);
                ctx.close();
            }
            return;
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof PingWebSocketFrame) {
            // Handle ping frames
            PongWebSocketFrame pongFrame = new PongWebSocketFrame(frame.content().retain());
            ctx.writeAndFlush(pongFrame);
        } else if (frame instanceof TextWebSocketFrame) {
            TextWebSocketFrame textFrame = (TextWebSocketFrame) frame;
            textMessageHandler.accept(textFrame.text());
        } else if (frame instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
            binaryMessageHandler.accept(binaryFrame.content());
        } else if (frame instanceof CloseWebSocketFrame) {
            int code = ((CloseWebSocketFrame) frame).statusCode();
            String reason = ((CloseWebSocketFrame) frame).reasonText();

            ctx.channel().attr(CLOSE_CODE_KEY).set(code);
            ctx.channel().attr(REASON_KEY).set(reason);
            ch.close();
        } else if (frame instanceof ContinuationWebSocketFrame) {
            ContinuationWebSocketFrame continuationFrame = (ContinuationWebSocketFrame) frame;
            log.error("received ContinuationWebSocketFrame, len: {}", continuationFrame.content().capacity());
        }
    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("exception caught", cause);
        ctx.close();
    }
}