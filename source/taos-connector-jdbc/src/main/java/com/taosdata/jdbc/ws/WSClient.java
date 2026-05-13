package com.taosdata.jdbc.ws;

import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.timeout.TimeoutException;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WSClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WSClient.class);


    public final URI serverUri;

    private final String host;
    private final int port;

    private Channel channel;
    private final ConnectionParam connectionParam;

    static {
        Utils.initEventLoopGroup();
    }
    /**
     * create websocket connection client
     *
     * @param serverUri connection url
     */
    public WSClient(URI serverUri, ConnectionParam connectionParam) {
       this.serverUri = serverUri;
        this.connectionParam = connectionParam;
        this.channel = null;

        String scheme = serverUri.getScheme() == null ? "ws" : serverUri.getScheme();
        host = serverUri.getHost() == null ? "127.0.0.1" : serverUri.getHost();
        if (serverUri.getPort() == -1) {
            if ("ws".equalsIgnoreCase(scheme)) {
                port = 80;
            } else {
                port = 443;
            }
        } else {
            port = serverUri.getPort();
        }
    }

    private  Bootstrap createBootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(Utils.getEventLoopGroup())
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionParam.getConnectTimeout())
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .handler(new WebSocketChannelInitializer(connectionParam, host, port, serverUri));
        return b;
    }

    // Asynchronous method to obtain a Channel (reuses core logic from the original code)
    public CompletableFuture<Channel> getChannelAsync() {
        // CompletableFuture to hold the asynchronous result (success returns Channel, failure returns SQLException)
        CompletableFuture<Channel> resultFuture = new CompletableFuture<>();
        Bootstrap b = createBootstrap();

        // Initiate asynchronous TCP connection, returns ChannelFuture to track connection status
        ChannelFuture connectFuture = b.connect(host, port);

        // Register listener for connection result (executes in Netty IO thread when connection completes)
        connectFuture.addListener(connect -> {
            if (!connect.isSuccess()) {
                // Handle connection failure: wrap underlying exception into business-defined SQLException and complete future exceptionally
                Throwable cause = connect.cause();
                SQLException ex = TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, cause.getMessage());
                resultFuture.completeExceptionally(ex);
                return;
            }

            // Connection successful: get the established Channel and WebSocket handshake handler from the pipeline
            Channel tmpChn = connectFuture.channel();
            WebSocketHandshakeHandler wsHandler = tmpChn.pipeline().get(WebSocketHandshakeHandler.class);
            ChannelFuture handshakeFuture = wsHandler.handshakeFuture();

            // Register listener for WebSocket handshake result (executes in IO thread when handshake completes)
            handshakeFuture.addListener(handshake -> {
                if (!handshake.isSuccess()) {
                    // Handle handshake failure: close the channel to release resources, wrap exception, and complete future exceptionally
                    tmpChn.close();
                    SQLException ex = TSDBError.createSQLException(
                            TSDBErrorNumbers.ERROR_UNKNOWN,
                            "Handshake failed: " + handshake.cause().getMessage()
                    );
                    resultFuture.completeExceptionally(ex);
                    return;
                }

                // Handshake successful: remove the handshake handler (no longer needed after handshake) and complete future with the valid Channel
                tmpChn.pipeline().remove(wsHandler);
                this.channel = tmpChn;
                resultFuture.complete(tmpChn);
            });

            // Asynchronous handshake timeout handling: schedule a task to check if handshake is completed within the timeout period
            tmpChn.eventLoop().schedule(() -> {
                if (!handshakeFuture.isDone()) {
                    tmpChn.close();
                    resultFuture.completeExceptionally(
                            TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, "Handshake timed out")
                    );
                }
            }, connectionParam.getConnectTimeout(), TimeUnit.MILLISECONDS);
        });

        // Asynchronous connection timeout handling: schedule a task to cancel the connection if it's not completed within the timeout period
        connectFuture.channel().eventLoop().schedule(() -> {
            if (!connectFuture.isDone()) {
                connectFuture.cancel(true); // Cancel the uncompleted connection
                resultFuture.completeExceptionally(
                        TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT)
                );
            }
        }, connectionParam.getConnectTimeout(), TimeUnit.MILLISECONDS);

        return resultFuture;
    }
    private Channel getChannel() throws SQLException {
        Bootstrap b = createBootstrap();
        ChannelFuture connectFuture = b.connect(host, port);

        Channel tmpChn = null;
        try {
            if (!connectFuture.awaitUninterruptibly(connectionParam.getConnectTimeout())) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT);
           }

            if (!connectFuture.isSuccess()) {
                Throwable cause = connectFuture.cause();
                if (cause instanceof ConnectTimeoutException) {
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT);
                }
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, cause.getMessage());
            }

            tmpChn = connectFuture.channel();
            Promise<WebSocketHandshakeHandler> wsHandlerPromise = tmpChn.eventLoop().newPromise();

            final Channel chn = tmpChn;
            tmpChn.eventLoop().execute(() -> {
                try {
                    WebSocketHandshakeHandler wsHandler = chn.pipeline().get(WebSocketHandshakeHandler.class);
                    if (wsHandler == null) {
                        wsHandlerPromise.setFailure(new IllegalStateException("WebSocketHandshakeHandler not initialized，initChannel execute failed"));
                    } else {
                        wsHandlerPromise.setSuccess(wsHandler);
                    }
                } catch (Exception e) {
                    wsHandlerPromise.setFailure(e);
                }
            });

            // wait for the Promise to complete with timeout
            wsHandlerPromise.awaitUninterruptibly(connectionParam.getConnectTimeout());
            if (!wsHandlerPromise.isSuccess()) {
                tmpChn.close().syncUninterruptibly();
                log.error("get wsHandler failed", wsHandlerPromise.cause());
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "get wsHandler failed: " + wsHandlerPromise.cause().getMessage());
            }

            // get result from the Promise
            WebSocketHandshakeHandler wsHandler = wsHandlerPromise.getNow();
            ChannelFuture handshakeFuture = wsHandler.handshakeFuture();

            if (!handshakeFuture.awaitUninterruptibly(connectionParam.getConnectTimeout())) {
                tmpChn.close().syncUninterruptibly();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, "Handshake timed out");
            }

            if (!handshakeFuture.isSuccess()) {
                tmpChn.close().syncUninterruptibly();
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "Handshake failed: " + handshakeFuture.cause().getMessage());
            }
            tmpChn.pipeline().remove(wsHandler);
            return tmpChn;
        } catch (TimeoutException e) {
            if (tmpChn != null) {
                tmpChn.close().syncUninterruptibly();
            }
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT, e.getMessage());
        }
    }

    public boolean isOpen() {
        if (channel == null) {
            return false;
        }
        return channel.isActive();
    }

    public boolean isClosed() {
        return !isOpen();
    }

    // Extracted repeated logic: Create close frame and mark local-initiated close
    private CloseWebSocketFrame createCloseFrameAndMark() {
        int statusCode = 1000;
        String reason = "Normal close";
        CloseWebSocketFrame closeFrame = new CloseWebSocketFrame(statusCode, reason);
        // Set flag for local-initiated close (reuse original logic)
        channel.attr(WebSocketClientHandler.LOCAL_INITIATED_CLOSE).set(true);
        return closeFrame;
    }

    // Extracted channel validity check (avoids duplicate judgments)
    private boolean isChannelValid() {
        return channel != null && channel.isOpen();
    }

    @Override
    public void close() {
        if (!isChannelValid()) {
            return; // Return directly if channel is invalid
        }

        // Call extracted method to create close frame
        CloseWebSocketFrame closeFrame = createCloseFrameAndMark();
        ChannelFuture writeFuture = channel.writeAndFlush(closeFrame);

        // Block synchronously until write completes (original sync logic retained)
        writeFuture.syncUninterruptibly();

        // Close channel synchronously (original sync logic retained)
        ChannelFuture closeFuture = channel.close();
        closeFuture.syncUninterruptibly();
        if (closeFuture.isSuccess()) {
            log.debug("WebSocket connection closed successfully");
        } else {
            log.error("WebSocket connection closed error", closeFuture.cause());
        }
    }

    /**
     * Async close WebSocket connection (non-blocking, returns CompletableFuture)
     * @return CompletableFuture for async result (no return on success; contains exception on failure)
     */
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        if (!isChannelValid()) {
            resultFuture.complete(null); // Complete directly if channel is invalid
            return resultFuture;
        }

        // Call extracted method to create close frame (reuse logic)
        CloseWebSocketFrame closeFrame = createCloseFrameAndMark();
        ChannelFuture writeFuture = channel.writeAndFlush(closeFrame);

        // Async listener for write result (original async logic retained)
        writeFuture.addListener(write -> {
            if (!write.isSuccess()) {
                log.error("Failed to write close frame", write.cause());
                resultFuture.completeExceptionally(write.cause());
                return;
            }

            // Async close channel and listen for result
            ChannelFuture closeFuture = channel.close();
            closeFuture.addListener(close -> {
                if (close.isSuccess()) {
                    log.debug("WebSocket connection closed successfully");
                    resultFuture.complete(null);
                } else {
                    log.error("WebSocket connection closed error", close.cause());
                    resultFuture.completeExceptionally(close.cause());
                }
            });
        });

        return resultFuture;
    }

    public boolean connectBlocking(){
        synchronized (this) {
            if (channel != null && channel.isActive()) {
                return true;
            }
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }

            try {
                channel = getChannel();
                return true;
            } catch (SQLException e) {
                log.error("WebSocket connect failed: ", e);
                return false;
            }
        }
    }

    public boolean reconnectBlocking(){
        return connectBlocking();
    }

    public void send(String strData) {
        if (!channel.isActive()) {
            throw new WebsocketNotConnectedException();
        }
        channel.eventLoop().execute(() -> {
            channel.writeAndFlush(new TextWebSocketFrame(strData));
        });
    }

    public void send(ByteBuf binData) {
        if (!channel.isActive()) {
            Utils.releaseByteBuf(binData);
            throw new WebsocketNotConnectedException();
        }

        channel.writeAndFlush(new BinaryWebSocketFrame(binData));
    }
    public void closeBlocking() {
        this.close();
    }
    public static WSClient getInstance(ConnectionParam params, int endpointIndex, WSFunction function) throws SQLException {
        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = "";
        if (params.getEndpoints().get(endpointIndex).getPort() != 0) {
            port = ":" + params.getEndpoints().get(endpointIndex).getPort();
        }

        String wsFunction = "/ws";
        if (function.equals(WSFunction.TMQ)){
            wsFunction = "/rest/tmq";
        }
        String loginUrl = protocol + "://" + params.getEndpoints().get(endpointIndex).getHost() + port + wsFunction;

        if (null != params.getCloudToken()) {
            loginUrl = loginUrl + "?token=" + params.getCloudToken();
        }

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, params);
    }
    public static WSClient getSlaveInstance(ConnectionParam params, WSFunction function) throws SQLException {
        if (StringUtils.isEmpty(params.getSlaveClusterHost()) || StringUtils.isEmpty(params.getSlaveClusterHost())){
            return null;
        }

        if (Strings.isNullOrEmpty(function.getFunction())) {
            throw new SQLException("websocket url error");
        }
        String protocol = "ws";
        if (params.isUseSsl()) {
            protocol = "wss";
        }
        String port = ":" + params.getSlaveClusterPort();

        String wsFunction = "/ws";

        if (!function.equals(WSFunction.WS)){
            throw new SQLException("slave cluster is not supported!");
        }

        String loginUrl = protocol + "://" + params.getSlaveClusterHost() + port + wsFunction;

        URI urlPath;
        try {
            urlPath = new URI(loginUrl);
        } catch (URISyntaxException e) {
            throw new SQLException("Slave cluster websocket url parse error: " + loginUrl, e);
        }
        return new WSClient(urlPath, params);
    }
}
