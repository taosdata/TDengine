package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.common.ConnectionParam;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandler;
import io.netty.handler.codec.http.websocketx.extensions.WebSocketClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.DeflateFrameClientExtensionHandshaker;
import io.netty.handler.codec.http.websocketx.extensions.compression.PerMessageDeflateClientExtensionHandshaker;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.net.URI;

public class WebSocketChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final ConnectionParam connectionParam;
    private final String host;
    private final int port;
    public final URI serverUri;

    public WebSocketChannelInitializer(ConnectionParam connectionParam, String host, int port, URI serverUri) {
        this.connectionParam = connectionParam;
        this.host = host;
        this.port = port;
        this.serverUri = serverUri;
    }
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();

        if (connectionParam.isUseSsl()) {
            if (connectionParam.isDisableSslCertValidation()){
                SslContext sslCtx = SslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build();
                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
            } else {
                SslContext sslCtx = SslContextBuilder.forClient().build();
                p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
            }
        }

        // for debug
        //p.addLast(new LoggingHandler(LogLevel.DEBUG)); // NOSONAR - kept for debugging

        p.addLast(new HttpClientCodec());
        p.addLast(new HttpObjectAggregator(8192));

        // use custom websocket client handshaker to avoid mask encode
        WebSocketClientHandshaker handshaker = new CustomWebSocketClientHandshaker(serverUri,
                WebSocketVersion.V13,
                null,
                true,
                new DefaultHttpHeaders(),
                100 * 1024 * 1024,
                true,
                false,
                -1L);
        p.addLast(new WebSocketHandshakeHandler(handshaker));

        if (connectionParam.isEnableCompression()) {
            WebSocketClientExtensionHandshaker deflateHandshaker = new PerMessageDeflateClientExtensionHandshaker(
                    6, false,
                    15,       // clientMaxWindowSize (2^15 = 32KB)
                    true,     // clientNoContextTakeover
                    true    // serverNoContextTakeover

            );

            WebSocketClientExtensionHandler extensionHandler = new WebSocketClientExtensionHandler(deflateHandshaker,  new DeflateFrameClientExtensionHandshaker(false),
                    new DeflateFrameClientExtensionHandshaker(true));
            p.addLast(extensionHandler);
        }

        p.addLast(new WebSocketFrameAggregator(100 * 1024 * 1024)); // max 100MB

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final WebSocketClientHandler handler =
                new WebSocketClientHandler(connectionParam.getTextMessageHandler(),
                        connectionParam.getBinaryMessageHandler());
        p.addLast(handler);
    }
}