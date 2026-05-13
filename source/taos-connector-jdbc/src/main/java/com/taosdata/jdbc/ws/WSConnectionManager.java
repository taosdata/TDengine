package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.ws.entity.*;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.taosdata.jdbc.TSDBErrorNumbers.ERROR_CONNECTION_TIMEOUT;

/**
 * Manages WebSocket connections, reconnection, and load balancing for TDengine.
 * Handles connection lifecycle including initial connection, reconnection on failure,
 * and balancing connections across multiple endpoints.
 */
public class WSConnectionManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WSConnectionManager.class);

    private final AtomicInteger reconnectCount = new AtomicInteger(0);
    private boolean firstDisconnect = true;
    private final ArrayList<WSClient> clientArr = new ArrayList<>();
    private final ConnectionParam connectionParam;
    private final WSFunction wsFunction;
    private final InFlightRequest inFlightRequest;
    private final long defaultTimeout;

    private int currentNodeIndex;
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();
    private volatile boolean closed = false;

    /**
     * Constructs a new WSConnectionManager with the specified parameters.
     *
     * @param function the WebSocket function type
     * @param param the connection parameters
     * @param inFlightRequest the in-flight request manager
     * @throws SQLException if connection initialization fails
     */
    public WSConnectionManager(WSFunction function, ConnectionParam param, InFlightRequest inFlightRequest) throws SQLException {
        this.connectionParam = param;
        this.wsFunction = function;
        this.inFlightRequest = inFlightRequest;
        this.defaultTimeout = param.getRequestTimeout();
        initializeClients();
        rebalanceManager.newCluster(param.getEndpoints());
    }

    /**
     * Initializes WebSocket clients based on connection parameters.
     * Supports master-slave mode and multiple endpoints configuration.
     *
     * @throws SQLException if client initialization fails
     */
    private void initializeClients() throws SQLException {
        WSClient slave = WSClient.getSlaveInstance(connectionParam, wsFunction);
        if (slave != null) {
            WSClient master = WSClient.getInstance(connectionParam, 0, wsFunction);
            this.clientArr.add(master);
            this.clientArr.add(slave);
            currentNodeIndex = 0;
        } else {
            for (int i = 0; i < connectionParam.getEndpoints().size(); i++) {
                WSClient client = WSClient.getInstance(connectionParam, i, wsFunction);
                this.clientArr.add(client);
            }
            currentNodeIndex = 0;
        }
    }

    /**
     * Performs complete reconnection process including physical reconnection and authentication.
     *
     * @param isTmq whether this is a TMQ connection (TMQ has different reconnection behavior)
     * @param transport the transport instance for sending authentication requests
     * @throws SQLException if reconnection fails for all endpoints
     */
    public synchronized void performReconnect(boolean isTmq, Transport transport) throws SQLException {
        if (isConnected()) {
            return;
        }

        if (!connectionParam.isEnableAutoConnect()) {
            handleInitialDisconnect();
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED,
                    "Websocket Not Connected Exception for connection closed");
        }

        boolean reconnected = performCurrentNodeReconnect(isTmq, transport);
        if (reconnected) {
            handleSuccessfulReconnect();
            return;
        }

        handleOtherNodesReconnect(isTmq, transport);
    }

    private boolean performCurrentNodeReconnect(boolean isTmq, Transport transport) throws SQLException {
        boolean reconnected = reconnectCurrentNodePhysical();
        if (reconnected && !isTmq) {
            reconnected = authenticateAfterReconnect(transport);
        }
        return reconnected;
    }

    private void handleOtherNodesReconnect(boolean isTmq, Transport transport) throws SQLException {
        handleInitialDisconnect();

        List<Endpoint> endpoints = connectionParam.getEndpoints();
        int[] indexes = rebalanceManager.getConnectCountsAsc(endpoints);

        for (int currentIndex : indexes) {
            if (!rebalanceManager.getEndpointInfo(endpoints.get(currentIndex)).isOnline()) {
                continue;
            }

            currentNodeIndex = currentIndex;
            boolean reconnected = performCurrentNodeReconnect(isTmq, transport);

            if (reconnected) {
                handleSuccessfulReconnect();
                rebalanceManager.incrementConnectionCount(endpoints.get(currentNodeIndex));
                rebalanceManager.connected(endpoints, currentNodeIndex);
                return;
            }
        }

        close();
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED,
                "Websocket Not Connected Exception for all endpoints");
    }

    private boolean reconnectCurrentNodePhysical() {
        for (int retryTimes = 0; retryTimes < connectionParam.getReconnectRetryCount(); retryTimes++) {
            try {
                boolean reconnected = clientArr.get(currentNodeIndex).reconnectBlocking();
                if (reconnected) {
                    return true;
                }
                Thread.sleep(connectionParam.getReconnectIntervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Reconnect sleep interrupted", e);
                break;
            } catch (Exception e) {
                log.error("try connect remote server failed!", e);
            }
        }
        return false;
    }

    /**
     * Performs authentication after physical reconnection.
     *
     * @param transport the transport instance for sending authentication request
     * @return true if authentication successful, false otherwise
     * @throws SQLException if authentication request fails
     */
    public boolean authenticateAfterReconnect(Transport transport) throws SQLException {
        ConnectReq connectReq = new ConnectReq(connectionParam);
        ConnectResp auth;
        try {
            auth = (ConnectResp) transport.sendWithoutRetry(new Request(Action.CONN.getAction(), connectReq), defaultTimeout);

            if (Code.SUCCESS.getCode() == auth.getCode()) {
                return true;
            } else {
                clientArr.get(currentNodeIndex).closeBlocking();
                log.error("reconnect failed, code: {}, msg: {}", auth.getCode(), auth.getMessage());
                return false;
            }
        } catch (SQLException e) {
            clientArr.get(currentNodeIndex).closeBlocking();
            throw e;
        }
    }

    private void handleInitialDisconnect() {
        if (firstDisconnect) {
            rebalanceManager.disconnected(connectionParam, currentNodeIndex, inFlightRequest);
            firstDisconnect = false;
        }
    }

    private void handleSuccessfulReconnect() {
        reconnectCount.incrementAndGet();
        firstDisconnect = true;
        if (log.isDebugEnabled()) {
            log.debug("reconnect success to {}",
                    StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));
        }
    }

    /**
     * Checks and establishes connection to TDengine servers.
     * For first connection with slave cluster, tests all nodes and keeps only the current connection.
     * For subsequent connections, uses load balancing to select the best endpoint.
     *
     * @param connectTimeout the connection timeout in milliseconds
     * @throws SQLException if connection cannot be established within timeout
     */
    public void checkConnection(int connectTimeout) throws SQLException {
        if (WSConnection.g_FirstConnection.compareAndSet(true, false) &&
                !StringUtils.isEmpty(connectionParam.getSlaveClusterHost())) {
            checkConnectionWithSlaveCluster(connectTimeout);
        } else {
            checkConnectionWithLoadBalancing(connectTimeout);
        }
    }

    private void checkConnectionWithSlaveCluster(int connectTimeout) throws SQLException {
        for (int i = 0; i < clientArr.size(); i++) {
            if (!clientArr.get(i).connectBlocking()) {
                rebalanceManager.disconnected(connectionParam, i, inFlightRequest);
                close();
                throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                        "can't create connection with server " + clientArr.get(i).serverUri.toString() +
                                " within: " + connectTimeout + " milliseconds");
            }
            if (log.isDebugEnabled()) {
                log.debug("connect success to {}", StringUtils.getBasicUrl(clientArr.get(i).serverUri.toString()));
            }

            rebalanceManager.incrementConnectionCount(connectionParam.getEndpoints().get(i));
            rebalanceManager.connected(connectionParam.getEndpoints(), i);
        }

        for (int i = 0; i < clientArr.size(); i++) {
            if (i != currentNodeIndex) {
                clientArr.get(i).closeBlocking();
                if (log.isDebugEnabled()) {
                    log.debug("disconnect success to {}", StringUtils.getBasicUrl(clientArr.get(i).serverUri.toString()));
                }
            }
        }
    }

    private void checkConnectionWithLoadBalancing(int connectTimeout) throws SQLException {
        if (connectWithMinimumCount()) {
            return;
        }
        close();
        throw TSDBError.createSQLException(ERROR_CONNECTION_TIMEOUT,
                "can't create connection with any server within: " + connectTimeout + " milliseconds");
    }

    private boolean connectWithMinimumCount() {
        List<Endpoint> endpoints = connectionParam.getEndpoints();
        int[] indexes = rebalanceManager.getConnectCountsAsc(endpoints);
        for (int currentIndex : indexes) {
            if (connectionParam.isEnableAutoConnect() &&
                    !rebalanceManager.getEndpointInfo(endpoints.get(currentIndex)).isOnline()) {
                continue;
            }

            currentNodeIndex = currentIndex;
            rebalanceManager.incrementConnectionCount(endpoints.get(currentNodeIndex));
            if (clientArr.get(currentNodeIndex).connectBlocking()) {
                if (log.isDebugEnabled()) {
                    log.debug("connect success to {}",
                            StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));
                }
                rebalanceManager.connected(endpoints, currentNodeIndex);
                return true;
            } else {
                if (log.isErrorEnabled()) {
                    log.error("connect failed to {}",
                            StringUtils.getBasicUrl(clientArr.get(currentNodeIndex).serverUri.toString()));
                }
                rebalanceManager.disconnected(connectionParam, currentNodeIndex, inFlightRequest);
            }
        }
        return false;
    }

    /**
     * Balances connections across available endpoints by switching to the least loaded endpoint.
     *
     * @param transport the transport instance for sending authentication during switch
     */
    public void balanceConnection(Transport transport) {
        synchronized (this) {
            int backCurrentIndex = currentNodeIndex;

            int toIndex = rebalanceManager.getMinConnectionEndpointIndex(connectionParam);
            if (currentNodeIndex != toIndex) {
                performConnectionSwitch(backCurrentIndex, toIndex, transport);
            }
        }
    }

    private void performConnectionSwitch(int backCurrentIndex, int toIndex, Transport transport) {
        try {
            clientArr.get(toIndex).reconnectBlocking();
        } catch (Exception e) {
            log.error("try connect remote server failed!", e);
            return;
        }

        currentNodeIndex = toIndex;
        try {
            ConnectReq connectReq = new ConnectReq(connectionParam);
            ConnectResp auth = (ConnectResp) transport.sendWithoutRetry(
                    new Request(Action.CONN.getAction(), connectReq), defaultTimeout);

            if (Code.SUCCESS.getCode() != auth.getCode()) {
                clientArr.get(toIndex).closeBlocking();
                currentNodeIndex = backCurrentIndex;
                log.error("balance connection failed, code: {}, msg: {}", auth.getCode(), auth.getMessage());
                return;
            }
        } catch (SQLException e) {
            log.error("balance connection failed!", e);
            return;
        }

        clientArr.get(backCurrentIndex).closeBlocking();
        rebalanceManager.incrementConnectionCount(connectionParam.getEndpoints().get(currentNodeIndex));
        rebalanceManager.decrementConnectionCount(connectionParam.getEndpoints().get(backCurrentIndex));
    }

    /**
     * Gets the current active WebSocket client.
     *
     * @return the current WSClient instance
     */
    public WSClient getCurrentClient() {
        return clientArr.get(currentNodeIndex);
    }

    /**
     * Checks if the current connection is open and active.
     *
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return clientArr.get(currentNodeIndex).isOpen();
    }

    /**
     * Checks if the current connection is closed or lost.
     *
     * @return true if connection is lost, false otherwise
     */
    public boolean isConnectionLost() {
        return clientArr.get(currentNodeIndex).isClosed();
    }

    /**
     * Gets the number of reconnection attempts made.
     *
     * @return the reconnection count
     */
    public int getReconnectCount() {
        return reconnectCount.get();
    }

    /**
     * Gets the connection parameters.
     *
     * @return the connection parameters
     */
    public ConnectionParam getConnectionParam() {
        return connectionParam;
    }

    /**
     * Gets the current active endpoint.
     *
     * @return the current endpoint
     */
    public Endpoint getCurrentEndpoint() {
        return connectionParam.getEndpoints().get(currentNodeIndex);
    }

    /**
     * Checks if this connection manager is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Closes all connections and releases resources.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;

        if (isConnected()) {
            rebalanceManager.disconnectedBySelf(connectionParam, currentNodeIndex);
        }
        for (WSClient wsClient : clientArr) {
            wsClient.close();
        }
    }

    /**
     * Throws special connection closed exception for TMQ connections.
     * TMQ handles reconnection differently in its poll mechanism.
     *
     * @throws SQLException always throws TMQ-specific connection closed exception
     */
    public void tmqRethrowConnectionCloseException() throws SQLException {
        if (WSFunction.TMQ.equals(this.wsFunction)) {
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_CONNECTION_CLOSED,
                    "Websocket Not Connected Exception internal for TMQ");
        }
    }

    /**
     * Handles connection exceptions and initiates reconnection if appropriate.
     *
     * @param transport the transport instance for reconnection
     * @throws SQLException if reconnection fails or TMQ exception should be thrown
     */
    void handleConnectionException(Transport transport) throws SQLException {
        if (WSFunction.TMQ.equals(this.wsFunction)) {
            tmqRethrowConnectionCloseException();
        }
        performReconnect(false, transport);
    }
}