package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBConstants;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.Printable;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.CompletableFutureTimeout;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.entity.Request;
import com.taosdata.jdbc.ws.entity.Response;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Handles message sending and receiving over WebSocket connections to TDengine.
 * Provides synchronous and asynchronous message sending with timeout and retry support.
 * Delegates connection management to WSConnectionManager.
 */
public class Transport implements AutoCloseable {

    /** Error code indicating RPC network unavailability */
    public static final int TSDB_CODE_RPC_NETWORK_UNAVAIL = 0x0B;

    /** Error code indicating some nodes are not connected */
    public static final int TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED = 0x20;

    /** Empty byte array constant */
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /** Error message for closed connection */
    public static final String ERROR_MSG_CONNECTION_CLOSED = "Websocket Not Connected Exception for connection closed";

    private final WSConnectionManager connectionManager;
    private final InFlightRequest inFlightRequest;
    private final long defaultTimeout;
    private volatile boolean closed = false;

    /**
     * Protected constructor for internal use.
     */
    protected Transport() {
        this.connectionManager = null;
        this.inFlightRequest = null;
        this.defaultTimeout = TSDBConstants.DEFAULT_MESSAGE_WAIT_TIMEOUT;
    }

    /**
     * Constructs a new Transport instance.
     *
     * @param function the WebSocket function type
     * @param param the connection parameters
     * @param inFlightRequest the in-flight request manager
     * @throws SQLException if initialization fails
     */
    public Transport(WSFunction function,
                     ConnectionParam param,
                     InFlightRequest inFlightRequest) throws SQLException {
        this.connectionManager = new WSConnectionManager(function, param, inFlightRequest);
        this.inFlightRequest = inFlightRequest;
        this.defaultTimeout = param.getRequestTimeout();
    }

    /**
     * Sends a request with default timeout and retry enabled.
     *
     * @param request the request to send
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    @SuppressWarnings("all")
    public Response send(Request request) throws SQLException {
        return send(request, true, defaultTimeout);
    }

    /**
     * Sends a request with specified timeout and retry enabled.
     *
     * @param request the request to send
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response send(Request request, long timeout) throws SQLException {
        return send(request, true, timeout);
    }

    /**
     * Sends a request with specified timeout and retry configuration.
     *
     * @param request the request to send
     * @param reSend whether to retry on connection failure
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response send(Request request, boolean reSend, long timeout) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        Response response = null;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        String reqString = request.toString();

        inFlightRequest.put(new FutureResponse(request.getAction(), request.id(), completableFuture));

        try {
            connectionManager.getCurrentClient().send(reqString);
        } catch (WebsocketNotConnectedException e) {
            try {
                connectionManager.handleConnectionException(this);
                if (!reSend) {
                    inFlightRequest.remove(request.getAction(), request.id());
                    throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
                }
                connectionManager.getCurrentClient().send(reqString);
            } catch (SQLException ex) {
                inFlightRequest.remove(request.getAction(), request.id());
                throw ex;
            } catch (Exception ex) {
                inFlightRequest.remove(request.getAction(), request.id());
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION, e.getMessage());
            }
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, timeout, TimeUnit.MILLISECONDS, getPrintString(reqString, request));
        try {
            response = responseFuture.get();
            handleTaosdError(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        } catch (ExecutionException e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }

    /**
     * Sends a binary request with single data payload.
     *
     * @param action the action type
     * @param reqId the request ID
     * @param resultId the result ID
     * @param type the message type
     * @param rawData the binary data payload
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response send(String action, long reqId, long resultId, long type, byte[] rawData, long timeout) throws SQLException {
        return send(action, reqId, resultId, type, rawData, EMPTY_BYTE_ARRAY, timeout);
    }

    /**
     * Sends a binary request with dual data payloads.
     *
     * @param action the action type
     * @param reqId the request ID
     * @param resultId the result ID
     * @param type the message type
     * @param rawData the first binary data payload
     * @param rawData2 the second binary data payload
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response send(String action, long reqId, long resultId, long type, byte[] rawData, byte[] rawData2, long timeout) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        int totalLength = 24 + rawData.length + rawData2.length;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(resultId);
        buffer.writeLongLE(type);
        buffer.writeBytes(rawData);
        buffer.writeBytes(rawData2);

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        inFlightRequest.put(new FutureResponse(action, reqId, completableFuture));

        try {
            Utils.retainByteBuf(buffer);
            connectionManager.getCurrentClient().send(buffer);
        } catch (WebsocketNotConnectedException e) {
            try {
                connectionManager.handleConnectionException(this);
                Utils.retainByteBuf(buffer);
                connectionManager.getCurrentClient().send(buffer);
            } catch (Exception ex) {
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION, e.getMessage());
            }
        } finally {
            Utils.releaseByteBuf(buffer);
        }

        String reqString = "action:" + action + ", reqId:" + reqId + ", resultId:" + resultId + ", actionType" + type;
        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(completableFuture, timeout, TimeUnit.MILLISECONDS, reqString);
        try {
            response = responseFuture.get();
            handleTaosdError(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inFlightRequest.remove(action, reqId);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        } catch (ExecutionException e) {
            inFlightRequest.remove(action, reqId);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }

    /**
     * Sends asynchronous fetch block request.
     *
     * @param reqId the request ID
     * @param resultId the result ID
     * @throws SQLException if sending fails
     */
    public void sendFetchBlockAsync(long reqId, long resultId) throws SQLException {
        final byte[] version = {1, 0};

        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        int totalLength = 26;
        ByteBuf buffer = PooledByteBufAllocator.DEFAULT.directBuffer(totalLength);

        buffer.writeLongLE(reqId);
        buffer.writeLongLE(resultId);
        buffer.writeLongLE(7); // fetch block action type
        buffer.writeBytes(version);

        try {
            Utils.retainByteBuf(buffer);
            connectionManager.getCurrentClient().send(buffer);
        } catch (WebsocketNotConnectedException e) {
            connectionManager.handleConnectionException(this);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED,
                    "Websocket reconnected, but the result set is closed");
        } finally {
            Utils.releaseByteBuf(buffer);
        }
    }

    /**
     * Sends a request with pre-allocated ByteBuf buffer.
     *
     * @param action the action type
     * @param reqId the request ID
     * @param buffer the ByteBuf containing the request data
     * @param resend whether to retry on connection failure
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response send(String action, long reqId, ByteBuf buffer, boolean resend, long timeout) throws SQLException {
        if (isClosed()) {
            Utils.releaseByteBuf(buffer);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        inFlightRequest.put(new FutureResponse(action, reqId, completableFuture));

        try {
            Utils.retainByteBuf(buffer);
            connectionManager.getCurrentClient().send(buffer);
        } catch (WebsocketNotConnectedException e) {
            try {
                connectionManager.handleConnectionException(this);
            } catch (SQLException ex) {
                inFlightRequest.remove(action, reqId);
                throw ex;
            }

            if (!resend) {
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED);
            }

            try {
                Utils.retainByteBuf(buffer);
                connectionManager.getCurrentClient().send(buffer);
            } catch (Exception ex) {
                inFlightRequest.remove(action, reqId);
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION, ex.getMessage());
            } finally {
                Utils.releaseByteBuf(buffer);
            }
        } finally {
            Utils.releaseByteBuf(buffer);
        }

        String reqString = "action:" + action + ", reqId:" + reqId;
        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(completableFuture, timeout, TimeUnit.MILLISECONDS, reqString);
        try {
            response = responseFuture.get();
            handleTaosdError(response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            inFlightRequest.remove(action, reqId);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        } catch (ExecutionException e) {
           inFlightRequest.remove(action, reqId);
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        }
        return response;
    }

    /**
     * Handles TDengine-specific error codes that may require connection cleanup.
     *
     * @param response the response to check for errors
     */
    private void handleTaosdError(Response response) {
        if (connectionManager.getConnectionParam().getEndpoints().size() > 1 &&
                response instanceof com.taosdata.jdbc.ws.entity.CommonResp) {
            com.taosdata.jdbc.ws.entity.CommonResp commonResp = (com.taosdata.jdbc.ws.entity.CommonResp) response;
            if (TSDB_CODE_RPC_NETWORK_UNAVAIL == commonResp.getCode() ||
                    TSDB_CODE_RPC_SOMENODE_NOT_CONNECTED == commonResp.getCode()) {
                connectionManager.getCurrentClient().closeBlocking();
            }
        }
    }

    /**
     * Sends a request without automatic retry on connection failure.
     *
     * @param request the request to send
     * @param timeout the timeout in milliseconds
     * @return the response from server
     * @throws SQLException if sending fails or timeout occurs
     */
    public Response sendWithoutRetry(Request request, long timeout) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        Response response;
        CompletableFuture<Response> completableFuture = new CompletableFuture<>();
        String reqString = request.toString();

        inFlightRequest.put(new FutureResponse(request.getAction(), request.id(), completableFuture));

        try {
            connectionManager.getCurrentClient().send(reqString);
        } catch (Exception e) {
            inFlightRequest.remove(request.getAction(), request.id());
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION,
                    e.getMessage() == null ? "" : e.getMessage());
        }

        CompletableFuture<Response> responseFuture = CompletableFutureTimeout.orTimeout(
                completableFuture, timeout, TimeUnit.MILLISECONDS, getPrintString(reqString, request));
        try {
            response = responseFuture.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        } catch (ExecutionException e) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_QUERY_TIMEOUT, e.getMessage());
        } finally {
            inFlightRequest.remove(request.getAction(), request.id());
        }
        return response;
    }

    /**
     * Sends a request without waiting for response.
     *
     * @param request the request to send
     * @throws SQLException if sending fails
     */
    public void sendWithoutResponse(Request request) throws SQLException {
        if (isClosed()) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, ERROR_MSG_CONNECTION_CLOSED);
        }

        try {
            connectionManager.getCurrentClient().send(request.toString());
        } catch (WebsocketNotConnectedException e) {
            connectionManager.handleConnectionException(this);
            try {
                connectionManager.getCurrentClient().send(request.toString());
            } catch (Exception ex) {
                throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESTFUL_CLIENT_IOEXCEPTION, e.getMessage());
            }
        }
    }

    /**
     * Checks if this transport is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Checks if the current connection is lost.
     *
     * @return true if connection is lost, false otherwise
     */
    public boolean isConnectionLost() {
        return connectionManager.isConnectionLost();
    }

    /**
     * Closes this transport and releases all resources.
     */
    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }
        closed = true;
        inFlightRequest.close();

        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    /**
     * Checks and establishes connection to TDengine servers.
     *
     * @param connectTimeout the connection timeout in milliseconds
     * @throws SQLException if connection cannot be established within timeout
     */
    public void checkConnection(int connectTimeout) throws SQLException {
        connectionManager.checkConnection(connectTimeout);
    }

    /**
     * Performs TMQ-specific reconnection.
     *
     * @throws SQLException if reconnection fails
     */
    public void reconnectTmq() throws SQLException {
        connectionManager.performReconnect(true, this);
    }

    /**
     * Gets the number of reconnection attempts made.
     *
     * @return the reconnection count
     */
    public int getReconnectCount() {
        return connectionManager.getReconnectCount();
    }

    /**
     * Checks if the current connection is active.
     *
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connectionManager.isConnected();
    }

    /**
     * Gets the connection parameters.
     *
     * @return the connection parameters
     */
    public final ConnectionParam getConnectionParam() {
        return connectionManager.getConnectionParam();
    }

    /**
     * Gets the current active endpoint.
     *
     * @return the current endpoint
     */
    public final Endpoint getCurrentEndpoint() {
        return connectionManager.getCurrentEndpoint();
    }

    /**
     * Balances connections across available endpoints.
     *
     * @throws SQLException if balance operation fails
     */
    public void balanceConnection() {
        connectionManager.balanceConnection(this);
    }

    private String getPrintString(String reqString, Request request) {
        if (request.getArgs() instanceof Printable){
            return request.toPrintString();
        }
        return reqString;
    }
}