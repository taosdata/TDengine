package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.common.Cluster;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.EndpointInfo;
import com.taosdata.jdbc.enums.WSFunction;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.FutureResponse;
import com.taosdata.jdbc.ws.InFlightRequest;
import com.taosdata.jdbc.ws.entity.Action;
import com.taosdata.jdbc.ws.entity.FetchBlockHealthCheckResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rebalance Manager for TDengine cluster endpoints
 * Manages endpoint connection count, health status, and automatic load rebalancing logic
 * Implements singleton pattern with static inner class (lazy loading + thread-safe initialization)
 * This class is responsible for cluster registration, endpoint state maintenance, and rebalancing trigger
 *
 * @version 1.0
 */
public class RebalanceManager {
    private static final Logger log = LoggerFactory.getLogger(RebalanceManager.class);

    // Global rebalancing status flag (indicates if any cluster is rebalancing)
    private final AtomicBoolean globalRebalancing;
    // Cluster-specific rebalancing status: Cluster -> AtomicBoolean (rebalancing state)
    private final Map<Cluster, AtomicBoolean> clusterRebalanceMap;
    // Endpoint metadata storage: Endpoint -> EndpointInfo (connection count + health status)
    private final Map<Endpoint, EndpointInfo> endpointInfoMap;
    // Endpoint to cluster mapping: Endpoint -> Cluster (associate endpoint with its cluster)
    private final Map<Endpoint, Cluster> endpointClusterMap;
    // Reserved instance counter for internal statistics (unused in current logic)
    private final AtomicInteger bgHealthCheckInstanceCounter;
    private final Set<BgHealthCheck> bgHealthCheckSet;

    /**
     * Private constructor for singleton pattern
     * Initializes all thread-safe state containers (ConcurrentHashMap + Atomic variables)
     */
    private RebalanceManager() {
        this.globalRebalancing = new AtomicBoolean(false);
        this.clusterRebalanceMap = new ConcurrentHashMap<>();
        this.endpointInfoMap = new ConcurrentHashMap<>();
        this.endpointClusterMap = new ConcurrentHashMap<>();
        this.bgHealthCheckInstanceCounter = new AtomicInteger(0);
        this.bgHealthCheckSet = ConcurrentHashMap.newKeySet();
    }

    /**
     * Static inner class for lazy singleton initialization
     * JVM's class loading mechanism guarantees thread safety for instance creation
     */
    private static class SingletonHolder {
        private static final RebalanceManager INSTANCE = new RebalanceManager();
    }

    /**
     * Get the singleton instance of RebalanceManager
     * This is the only way to access the manager's functionality
     *
     * @return Singleton instance of RebalanceManager
     */
    public static RebalanceManager getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * Encapsulation class for endpoint connection count statistics
     * Contains minimum/maximum/total connection count and the index of the minimum endpoint
     * Reduces repeated traversal of endpoint lists by encapsulating all stats in one pass
     */
    private static class EndpointCountStats {
        private final int minCount;       // Minimum connection count among all endpoints
        private final int minIndex;       // Index of the endpoint with minimum connection count
        private final int maxCount;       // Maximum connection count among all endpoints
        private final int totalCount;     // Total connection count of all endpoints

        /**
         * Constructor for EndpointCountStats
         *
         * @param minCount  Minimum connection count
         * @param maxCount  Maximum connection count
         * @param totalCount Total connection count of all endpoints
         * @param minIndex  Index of the endpoint with the least connections
         */
        public EndpointCountStats(int minCount, int maxCount, int totalCount, int minIndex) {
            this.minCount = minCount;
            this.minIndex = minIndex;
            this.maxCount = maxCount;
            this.totalCount = totalCount;
        }

        public int getMinCount() { return minCount; }
        public int getMinIndex() { return minIndex; }
        public int getMaxCount() { return maxCount; }
        public int getTotalCount() { return totalCount; }
    }

    /**
     * Collect and calculate endpoint connection count statistics in one traversal
     * Core deduplication logic to avoid redundant list iteration
     *
     * @param endpoints List of endpoints to calculate statistics for
     * @return Encapsulated EndpointCountStats object with all statistics
     */
    private EndpointCountStats collectEndpointCountStats(List<Endpoint> endpoints) {
        int size = endpoints.size();
        int minCount = Integer.MAX_VALUE;
        int maxCount = Integer.MIN_VALUE;
        int minIndex = -1;
        int totalCount = 0;

        // Traverse endpoints once to compute all statistics
        for (int i = 0; i < size; i++) {
            Endpoint endpoint = endpoints.get(i);
            EndpointInfo info = this.endpointInfoMap.get(endpoint);
            int count = info.getConnectCount();

            totalCount += count;
            if (count < minCount) {
                minCount = count;
                minIndex = i;
            }
            if (count > maxCount) {
                maxCount = count;
            }
        }

        return new EndpointCountStats(minCount, maxCount, totalCount, minIndex);
    }

    /**
     * Check if the cluster of the specified endpoint is in rebalancing state
     *
     * @param endpoint Target endpoint to check
     * @return true if the cluster is rebalancing, false otherwise
     */
    public boolean isRebalancing(Endpoint endpoint) {
        Cluster cluster = this.endpointClusterMap.get(endpoint);
        return this.clusterRebalanceMap.get(cluster).get();
    }

    /**
     * Check if there is any cluster in global rebalancing state
     *
     * @return true if global rebalancing is ongoing, false otherwise
     */
    public boolean isRebalancing() {
        return this.globalRebalancing.get();
    }

    /**
     * Increment the connection count of the specified endpoint
     * Called when a new connection is established to the endpoint
     *
     * @param endpoint Target endpoint
     */
    public void incrementConnectionCount(Endpoint endpoint) {
        this.endpointInfoMap.get(endpoint).incrementConnectCount();
    }

    /**
     * Decrement the connection count of the specified endpoint
     * Called when a connection is closed to the endpoint
     *
     * @param endpoint Target endpoint
     */
    public void decrementConnectionCount(Endpoint endpoint) {
        this.endpointInfoMap.get(endpoint).decrementConnectCount();
    }

    /**
     * Mark the specified endpoint as online when connection is successful
     *
     * @param endpoints List of cluster endpoints
     * @param index     Index of the connected endpoint in the list
     */
    public void connected(List<Endpoint> endpoints, int index) {
        Endpoint endpoint = endpoints.get(index);
        this.endpointInfoMap.get(endpoint).setOnline();
    }

    /**
     * Handle endpoint disconnection event
     * 1. Decrement connection count via disconnectedBySelf
     * 2. Start background health check if auto-connect is enabled and endpoint is first marked as offline
     *
     * @param original      Original connection parameters
     * @param index         Index of the disconnected endpoint in the list
     * @param inFlightRequest In-flight request container for health check response handling
     */
    public void disconnected(ConnectionParam original, int index, InFlightRequest inFlightRequest) {
        this.disconnectedBySelf(original, index);

        if (!StringUtils.isEmpty(original.getCloudToken())) {
            return;
        }

        Endpoint disconnectedEndpoint = original.getEndpoints().get(index);
        if (this.endpointInfoMap.get(disconnectedEndpoint).setOffline()
                && original.isEnableAutoConnect()
                && StringUtils.isEmpty(System.getProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK"))
        ) {
            // Start health check only once when the endpoint is first marked as offline
            this.startBackgroundHealthCheck(original, index, inFlightRequest);
        }
    }

    /**
     * Handle self-initiated disconnection (only decrement connection count)
     *
     * @param original Original connection parameters
     * @param index    Index of the disconnected endpoint in the list
     */
    public void disconnectedBySelf(ConnectionParam original, int index) {
        List<Endpoint> endpoints = original.getEndpoints();
        Endpoint endpoint = endpoints.get(index);

        this.endpointInfoMap.get(endpoint).decrementConnectCount();
    }

    /**
     * Register a new cluster and initialize endpoint metadata
     * Idempotent operation: no effect if the cluster is already registered
     *
     * @param endpoints List of endpoints in the new cluster
     */
    public void newCluster(List<Endpoint> endpoints) {
        Cluster cluster = new Cluster(endpoints.toArray(new Endpoint[0]));
        if (null == this.clusterRebalanceMap.putIfAbsent(cluster, new AtomicBoolean(false))) {
            // Initialize endpoint info only for newly registered clusters
            for (Endpoint endpoint : endpoints) {
                this.endpointInfoMap.putIfAbsent(endpoint, new EndpointInfo());
                this.endpointClusterMap.putIfAbsent(endpoint, cluster);
            }
        }
    }

    /**
     * Mark endpoint as online and trigger rebalancing if conditions are met
     *
     * @param param    Connection parameters (contains rebalance thresholds)
     * @param endpoint The endpoint that recovered to online state
     */
    public void endpointUp(ConnectionParam param, Endpoint endpoint) {
        Cluster cluster = this.endpointClusterMap.get(endpoint);
        this.endpointInfoMap.get(endpoint).setOnline();

        if (this.needRebalancingInner(param)) {
            this.clusterRebalanceMap.get(cluster).set(true);
            this.globalRebalancing.set(true);
            log.info("Endpoint: {} is up, starting load rebalancing", endpoint);
        }
    }

    /**
     * Get the metadata (connection count + health status) of the specified endpoint
     *
     * @param endpoint Target endpoint
     * @return EndpointInfo object containing endpoint metadata
     */
    public EndpointInfo getEndpointInfo(Endpoint endpoint) {
        return this.endpointInfoMap.get(endpoint);
    }

    /**
     * Get endpoint indexes sorted by connection count in ascending order
     * Used for load balancing to select the least loaded endpoint
     *
     * @param endpoints List of cluster endpoints
     * @return Sorted array of endpoint indexes (ascending by connection count)
     */
    public int[] getConnectCountsAsc(List<Endpoint> endpoints) {
        int n = endpoints.size();
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = i;
        }

        // Insertion sort for endpoint indexes based on connection count (ascending)
        for (int i = 1; i < n; i++) {
            int current = indexes[i];
            int currentCount = this.endpointInfoMap.get(endpoints.get(current)).getConnectCount();
            int j = i - 1;

            while (j >= 0) {
                int jCount = this.endpointInfoMap.get(endpoints.get(indexes[j])).getConnectCount();
                if (jCount <= currentCount) {
                    break;
                }

                indexes[j + 1] = indexes[j];
                j--;
            }
            indexes[j + 1] = current;
        }

        return indexes;
    }

    /**
     * Start background health check for offline endpoints
     * Monitors endpoint recovery status and updates state when online
     *
     * @param original      Original connection parameters
     * @param index         Index of the offline endpoint in the list
     * @param inFlightRequest In-flight request container for health check responses
     */
    public void startBackgroundHealthCheck(ConnectionParam original, int index, InFlightRequest inFlightRequest) {
        ConnectionParam param = ConnectionParam.copyToBuilder(original).build();
        param.setBinaryMessageHandler(byteBuf -> {
            byteBuf.readerIndex(26);
            long id = byteBuf.readLongLE();
            byteBuf.readerIndex(8);
            Utils.retainByteBuf(byteBuf);

            try {
                FetchBlockHealthCheckResp resp = new FetchBlockHealthCheckResp(byteBuf);
                FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
                if (null != remove) {
                    remove.getFuture().complete(resp);
                }
            } catch (Exception e) {
                Utils.releaseByteBuf(byteBuf);
                log.error("Unexpected error handling fetch block data, id: {}", id, e);
                FutureResponse remove = inFlightRequest.remove(Action.FETCH_BLOCK_NEW.getAction(), id);
                if (null != remove) {
                    remove.getFuture().complete(FetchBlockHealthCheckResp.getFailedResp());
                }
            }
        });

        // Initialize and start background health check task
        BgHealthCheck bgHealthCheck = new BgHealthCheck(WSFunction.WS, param, index, inFlightRequest);
        bgHealthCheckSet.add(bgHealthCheck);
        bgHealthCheck.start();
    }

    /**
     * Internal method: Check if rebalancing is needed based on connection distribution
     * Rebalancing conditions (ALL must be met, designed to avoid frequent rebalancing):
     * 1. Total connections exceed the rebalance base count threshold (control minimum connection scale)
     * 2. Max connection count > Min connection count * (1 + rebalance threshold percentage) (relative difference)
     * 3. Max connection count >= Min connection count + 2 (absolute difference, ensure large gap)
     *
     * @param param Connection parameters with rebalance thresholds
     * @return true if rebalancing is needed, false otherwise
     */
    private boolean needRebalancingInner(ConnectionParam param) {
        List<Endpoint> endpoints = param.getEndpoints();
        EndpointCountStats stats = this.collectEndpointCountStats(endpoints);

        return stats.getTotalCount() >= param.getRebalanceConBaseCount()
                && stats.getMaxCount() >= (stats.getMinCount() * (1 + param.getRebalanceThreshold() / 100.0))
                && stats.getMaxCount() >= (stats.getMinCount() + 2);
    }

    /**
     * Handle rebalancing logic for the current connection
     * 1. Update cluster/global rebalancing state if conditions are no longer met
     * 2. Determine if the current connection needs to be rebalanced to a less loaded endpoint
     *
     * @param param           Connection parameters with rebalance thresholds
     * @param currentEndpoint The endpoint currently connected to
     * @return true if rebalancing is required for this connection, false otherwise
     */
    public boolean handleRebalancing(ConnectionParam param, Endpoint currentEndpoint) {
        if (!this.needRebalancingInner(param)) {
            Cluster cluster = this.endpointClusterMap.get(currentEndpoint);
            this.clusterRebalanceMap.get(cluster).set(false);

            // Update global rebalancing state if no cluster is rebalancing
            boolean hasClusterRebalancing = false;
            for (Map.Entry<Cluster, AtomicBoolean> entry : this.clusterRebalanceMap.entrySet()) {
                if (entry.getValue().get()) {
                    hasClusterRebalancing = true;
                    break;
                }
            }

            if (!hasClusterRebalancing) {
                this.globalRebalancing.set(false);
            }
            return false;
        }

        int minIndex = this.getMinConnectionEndpointIndex(param);
        int minCount = this.endpointInfoMap.get(param.getEndpoints().get(minIndex)).getConnectCount();
        int currentCount = this.endpointInfoMap.get(currentEndpoint).getConnectCount();

        // No rebalancing needed if current endpoint's load is close to the minimum (within threshold)
        return !(currentCount < (minCount + 2) || currentCount < (minCount * (1 + param.getRebalanceThreshold() / 100.0)));
    }

    /**
     * Get the index of the endpoint with the minimum connection count
     * Used to select the least loaded endpoint for rebalancing
     *
     * @param param Connection parameters with endpoint list
     * @return Index of the endpoint with the least connections (-1 if endpoint list is empty)
     */
    public int getMinConnectionEndpointIndex(ConnectionParam param) {
        List<Endpoint> endpoints = param.getEndpoints();
        EndpointCountStats stats = this.collectEndpointCountStats(endpoints);
        return stats.getMinIndex();
    }

    public void addBgHealthCheckInstance() {
        this.bgHealthCheckInstanceCounter.incrementAndGet();
    }

    public void removeBgHealthCheckInstance() {
        this.bgHealthCheckInstanceCounter.decrementAndGet();
    }

    public int getBgHealthCheckInstanceCount() {
        return this.bgHealthCheckInstanceCounter.get();
    }

    public void removeBgHealthCheck(BgHealthCheck bgHealthCheck) {
        this.bgHealthCheckSet.remove(bgHealthCheck);
    }

    public void clearAllForTest() {
        for (BgHealthCheck bgHealthCheck : this.bgHealthCheckSet) {
            bgHealthCheck.setCancelled();
        }

        int count = 0;
        while (!this.bgHealthCheckSet.isEmpty()) {
            try {
                Thread.sleep(100);
                count++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (count > 50) {
                log.warn("Timeout waiting for background health check threads to terminate");
                throw new RuntimeException("Timeout waiting for background health check threads to terminate");
            }
        }

        this.globalRebalancing.set(false);
        this.clusterRebalanceMap.clear();
        this.endpointInfoMap.clear();
        this.endpointClusterMap.clear();
        this.bgHealthCheckSet.clear();
        this.bgHealthCheckInstanceCounter.set(0);
    }
}