package com.taosdata.jdbc.ws;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13;
import io.netty.handler.codec.http.websocketx.WebSocketFrameEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.net.URI;

public class CustomWebSocketClientHandshaker extends WebSocketClientHandshaker13 {

    public CustomWebSocketClientHandshaker(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength);
    }

    public CustomWebSocketClientHandshaker(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength, boolean performMasking, boolean allowMaskMismatch) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength, performMasking, allowMaskMismatch);
    }

    public CustomWebSocketClientHandshaker(URI webSocketURL, WebSocketVersion version, String subprotocol, boolean allowExtensions, HttpHeaders customHeaders, int maxFramePayloadLength, boolean performMasking, boolean allowMaskMismatch, long forceCloseTimeoutMillis) {
        super(webSocketURL, version, subprotocol, allowExtensions, customHeaders, maxFramePayloadLength, performMasking, allowMaskMismatch, forceCloseTimeoutMillis);
    }

    @Override
    protected WebSocketFrameEncoder newWebSocketEncoder() {
        return new CustomWebSocketFrameEncoder();
    }
}
