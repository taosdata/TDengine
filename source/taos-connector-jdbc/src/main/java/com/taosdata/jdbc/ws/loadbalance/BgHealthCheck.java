package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.WSClient;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;

// Health check node context: manages node state, health check parameters, and timer tasks
public class BgHealthCheck {
    static final Logger log = org.slf4j.LoggerFactory.getLogger(BgHealthCheck.class);
    private WSClient wsClient; // Reference to the WebSocket client for connection handling
    private final ConnectionParam param;
    private final int endpointIndex;
    private final Endpoint endpoint;
    private StepFlow flow;
    private volatile boolean cancelled = false;

    private final int sleepMinSeconds;
    private final int sleepMaxSeconds;
    private int currentInterval = -1;
    private int recoveryCmdCount = 0;
    private final int recoveryCmdMaxCount;
    private final int recoveryInterval;
    private final InFlightRequest inFlightRequest;

    public long getResultId() {
        return resultId;
    }

    public void setResultId(long resultId) {
        this.resultId = resultId;
    }

    private long resultId = -1;

    public BgHealthCheck(WSFunction wsFunction, ConnectionParam param, int index, InFlightRequest inFlightRequest) {
        endpointIndex = index;
        this.param = param;
        endpoint = param.getEndpoints().get(index);
        recoveryCmdMaxCount = param.getHealthCheckRecoveryCount();
        sleepMinSeconds = param.getHealthCheckInitInterval();
        sleepMaxSeconds = param.getHealthCheckMaxInterval();
        recoveryInterval = param.getHealthCheckRecoveryInterval();
        this.inFlightRequest = inFlightRequest;

        try {
            wsClient = WSClient.getInstance(param, index, wsFunction);
        } catch (Exception e) {
            log.error("Failed to initialize WSClient for health check: {}", e.getMessage());
            return;
        }

        List<Step> steps = Arrays.asList(
                new ConnectStep(),
                new ConCmdStep(),
                new QueryStep(),
                new FetchStep(),
                new Fetch2Step(),
                new FreeResultStep(),
                new CloseStep()
        );
        this.flow = new StepFlow(steps, this);
        RebalanceManager.getInstance().addBgHealthCheckInstance();
   }

    public void start() {
        this.flow.start();
    }

    public void cleanUp() {
        resultId = 0;
        recoveryCmdCount = 0;
    }

    public WSClient getWsClient() {
        return wsClient;
    }

    public ConnectionParam getParam() {
        return param;
    }

    public int getEndpointIndex() {
        return endpointIndex;
    }
    public Endpoint getEndpoint() {
        return endpoint;
    }
    public int getRecoveryInterval() {
        return recoveryInterval;
    }
    public InFlightRequest getInFlightRequest() {
        return inFlightRequest;
    }
    public int getNextInterval() {
        if (currentInterval < 0) {
            currentInterval = sleepMinSeconds;
            return currentInterval;
        }
        currentInterval = Math.min(currentInterval * 2, sleepMaxSeconds);
        return currentInterval;
    }

    public void addRecoveryCmdCount() {
        recoveryCmdCount++;
    }

    public boolean needMoreRetry() {
        return recoveryCmdCount < recoveryCmdMaxCount;
    }
    public boolean isCancelled() {
        return cancelled;
    }
    public void setCancelled() {
        this.cancelled = true;
    }
}