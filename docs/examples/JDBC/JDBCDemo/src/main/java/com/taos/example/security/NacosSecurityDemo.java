package com.taos.example.security;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Full end-to-end security validation demo with Nacos dynamic Token rotation.
 *
 * Scenarios validated:
 *   Step 1 - Read Token from Nacos, establish SSL JDBC connection.
 *   Step 2 - HikariCP connection pool with graceful Token rotation.
 *   Step 3 - TMQ consumer with graceful Token rotation.
 *
 * Prerequisites:
 *   - Nacos running at localhost:8848, config "tdengine-credential" present.
 *   - TDengine Enterprise with SSL enabled.
 *   - Local truststore at certs/truststore.jks.
 *
 * Environment Variables (optional, override defaults):
 *   - TDENGINE_NACOS_ADDR: Nacos address (default: localhost:8848)
 *   - TDENGINE_NACOS_USER: Nacos username (default: nacos)
 *   - TDENGINE_NACOS_PASSWORD: Nacos password (default: nacos)
 *   - TDENGINE_HOST: TDengine host (default: localhost)
 *   - TDENGINE_PORT: TDengine port (default: 6041)
 *   - TDENGINE_DB: Database name (default: empty)
 *
 * Run:
 *   cd docs/examples/JDBC/JDBCDemo
 *   export TDENGINE_HOST=your_host
 *   export TDENGINE_NACOS_ADDR=localhost:8848
 *   export TDENGINE_NACOS_USER=nacos
 *   export TDENGINE_NACOS_PASSWORD=nacos
 *   mvn -q compile exec:java -Dexec.mainClass=com.taos.example.security.NacosSecurityDemo \
 *       -Djavax.net.ssl.trustStore=certs/truststore.jks \
 *       -Djavax.net.ssl.trustStorePassword=changeit
 */
public class NacosSecurityDemo {

    private static final Logger logger = LoggerFactory.getLogger(NacosSecurityDemo.class);

    // Environment variables with defaults
    private static final String NACOS_ADDR = SecurityUtils.getEnv("TDENGINE_NACOS_ADDR", "localhost:8848");
    private static final String NACOS_USER = SecurityUtils.getEnv("TDENGINE_NACOS_USER", "nacos");
    private static final String NACOS_PASSWORD = SecurityUtils.getEnv("TDENGINE_NACOS_PASSWORD", "nacos");
    private static final String DATA_ID = SecurityUtils.DATA_ID;
    private static final String GROUP = SecurityUtils.GROUP;

    private static final String TDENGINE_HOST = SecurityUtils.getEnv("TDENGINE_HOST", "localhost");
    private static final int TDENGINE_PORT = Integer.parseInt(SecurityUtils.getEnv("TDENGINE_PORT", "6041"));
    private static final String TDENGINE_DB = SecurityUtils.getEnv("TDENGINE_DB", "");

    // Token rotation settings (use constants from SecurityUtils)
    private static final long TOKEN_TTL_MS = SecurityUtils.TOKEN_TTL_MS;
    private static final double ROTATION_THRESHOLD = SecurityUtils.ROTATION_THRESHOLD;

    /** Current token - updated by Nacos listener. */
    private static volatile String currentToken = "";
    private static volatile long tokenCreateTime = 0;

    /** Track executors for cleanup */
    private static final java.util.Set<ExecutorService> executorsToShutdown =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    /** Create Nacos ConfigService with authentication. */
    private static ConfigService createConfigService() throws Exception {
        Properties p = new Properties();
        p.put("serverAddr", NACOS_ADDR);
        p.put("username", NACOS_USER);
        p.put("password", NACOS_PASSWORD);
        return NacosFactory.createConfigService(p);
    }

    /** Check if connection pool is healthy. */
    private static boolean isHealthy(HikariDataSource dataSource) {
        if (dataSource == null || dataSource.isClosed()) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            return rs.next();
        } catch (SQLException e) {
            logger.warn("Health check failed: {}", e.getMessage());
            return false;
        }
    }

    /** Validate new token by creating a test connection. */
    private static boolean validateToken(String token) {
        String url = SecurityUtils.buildJdbcUrl(TDENGINE_HOST, TDENGINE_PORT, TDENGINE_DB, token);

        try (Connection conn = java.sql.DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            boolean result = rs.next();
            logger.info("Token validation: {} (token={})", result ? "SUCCESS" : "FAILED", SecurityUtils.maskToken(token));
            return result;
        } catch (SQLException e) {
            logger.error("Token validation failed: {}", e.getMessage());
            return false;
        }
    }

    private static void closePoolQuietly(HikariDataSource pool, String reason) {
        if (pool == null) {
            return;
        }
        try {
            if (!pool.isClosed()) {
                pool.close();
                logger.info("{}: pool closed", reason);
            }
        } catch (Exception e) {
            logger.warn("{}: failed to close pool: {}", reason, e.getMessage());
        }
    }

    private static void shutdownExecutorQuietly(ExecutorService executor, String name) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                logger.warn("{} did not terminate in time, forcing shutdown", name);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("{} shutdown interrupted", name);
            executor.shutdownNow();
        }
    }

    private static void shutdownTrackedExecutors() {
        for (ExecutorService executor : new ArrayList<>(executorsToShutdown)) {
            shutdownExecutorQuietly(executor, "tracked-executor");
            executorsToShutdown.remove(executor);
        }
    }

    // -------------------------------------------------------------------------
    // Step 1: Basic SSL connection using Token from Nacos
    // -------------------------------------------------------------------------
    private static void step1BasicConnect(ConfigService nacos) throws Exception {
        logger.info("[Step 1] Reading Token from Nacos ...");
        String content = nacos.getConfig(DATA_ID, GROUP, 5000);
        currentToken = SecurityUtils.parseToken(content);
        if (currentToken == null || currentToken.isEmpty()) {
            logger.error("[Step 1] No token found in Nacos config");
            throw new IllegalStateException("Token is required for authentication");
        }
        tokenCreateTime = System.currentTimeMillis();
        logger.info("[Step 1] Token acquired (length={}, masked={})", currentToken.length(), SecurityUtils.maskToken(currentToken));

        String url = SecurityUtils.buildJdbcUrl(TDENGINE_HOST, TDENGINE_PORT, TDENGINE_DB, currentToken);

        try (Connection conn = java.sql.DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT SERVER_VERSION()")) {
            String ver = rs.next() ? rs.getString(1) : "?";
            logger.info("[Step 1] Connected. Server version: {} \u2713", ver);
        } catch (SQLException e) {
            logger.error("[Step 1] Connection failed: {}", e.getMessage());
            throw e;
        }
    }

    // -------------------------------------------------------------------------
    // Step 2: HikariCP pool with graceful Token rotation
    // -------------------------------------------------------------------------

    private static HikariDataSource createPool(String token) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(SecurityUtils.buildJdbcUrl(TDENGINE_HOST, TDENGINE_PORT, TDENGINE_DB, token));
        cfg.setMaximumPoolSize(5);
        cfg.setMinimumIdle(1);
        cfg.setMaxLifetime(TOKEN_TTL_MS / 2);
        cfg.setKeepaliveTime(30_000);
        cfg.setConnectionTestQuery("SELECT SERVER_STATUS()");
        cfg.setConnectionTimeout(5000);
        return new HikariDataSource(cfg);
    }

    private static void step2PoolWithRotation(ConfigService nacos) throws Exception {
        logger.info("[Step 2] Starting HikariCP pool with graceful Token rotation ...");

        AtomicReference<HikariDataSource> poolRef = new AtomicReference<>(createPool(currentToken));
        AtomicInteger queryCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        AtomicInteger rotateCount = new AtomicInteger(0);
        Object poolSwitchLock = new Object();

        // Start scheduled health check task
        ScheduledExecutorService healthCheck = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-check");
            t.setDaemon(true);
            return t;
        });
        executorsToShutdown.add(healthCheck); // Track for cleanup

        healthCheck.scheduleAtFixedRate(() -> {
            HikariDataSource poolSnapshot = poolRef.get();
            if (!isHealthy(poolSnapshot)) {
                logger.error("[Step 2] Health check FAILED, attempting recovery...");
                String tokenSnapshot = currentToken;
                HikariDataSource newPool = null;
                try {
                    newPool = createPool(tokenSnapshot);
                    if (!isHealthy(newPool)) {
                        logger.error("[Step 2] Recovery candidate pool is unhealthy");
                        closePoolQuietly(newPool, "[Step 2] Recovery cleanup");
                        return;
                    }
                    HikariDataSource oldPool;
                    boolean switched = false;
                    synchronized (poolSwitchLock) {
                        if (poolRef.get() == poolSnapshot && tokenSnapshot.equals(currentToken)) {
                            oldPool = poolRef.getAndSet(newPool);
                            switched = true;
                        } else {
                            oldPool = null;
                        }
                    }
                    if (!switched) {
                        logger.info("[Step 2] Recovery candidate became stale, dropping candidate pool");
                        closePoolQuietly(newPool, "[Step 2] Recovery stale-candidate cleanup");
                        return;
                    }
                    logger.info("[Step 2] Recovery successful, switched to new pool (token snapshot={})",
                            SecurityUtils.maskToken(tokenSnapshot));
                    if (oldPool != null && oldPool != newPool) {
                        closePoolQuietly(oldPool, "[Step 2] Recovery old-pool cleanup");
                    }
                } catch (Exception e) {
                    logger.error("[Step 2] Recovery failed: {}", e.getMessage());
                    closePoolQuietly(newPool, "[Step 2] Recovery exception cleanup");
                }
            }
        }, 10, 10, TimeUnit.SECONDS);

        ExecutorService listenerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nacos-step2-listener");
            t.setDaemon(true);
            return t;
        });
        executorsToShutdown.add(listenerExecutor);

        Listener poolListener = new Listener() {
            @Override
            public Executor getExecutor() {
                // IMPORTANT: Return a shared executor, do not allocate per callback.
                return listenerExecutor;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                String newToken = SecurityUtils.parseToken(configInfo);
                if (newToken.isEmpty() || newToken.equals(currentToken)) {
                    return;
                }

                logger.info("[Step 2] Nacos config changed -> initiating graceful rotation");
                logger.info("[Step 2] New token: {}", SecurityUtils.maskToken(newToken));

                HikariDataSource newPool = null;
                boolean switched = false;

                // Graceful rotation with validation
                try {
                    // Step 1: Validate new token (note: isHealthy below also validates)
                    // We keep this for explicit validation message
                    if (!validateToken(newToken)) {
                        logger.error("[Step 2] New token validation FAILED, keeping old token");
                        return;
                    }

                    // Step 2: Create new pool with new token
                    newPool = createPool(newToken);

                    // Step 3: Verify new pool is healthy
                    if (!isHealthy(newPool)) {
                        logger.error("[Step 2] New pool health check FAILED, keeping old pool");
                        closePoolQuietly(newPool, "[Step 2] Rotation new-pool cleanup");
                        return;
                    }

                    // Step 4: Switch to new pool
                    HikariDataSource oldPool;
                    synchronized (poolSwitchLock) {
                        // Listener and health-check share the same switch lock. The replaced pool
                        // is always closed after swap, so there is no abandoned active pool instance.
                        oldPool = poolRef.getAndSet(newPool);
                        currentToken = newToken;
                        tokenCreateTime = System.currentTimeMillis();
                        switched = true;
                    }
                    rotateCount.incrementAndGet();
                    logger.info("[Step 2] Pool rotation SUCCESS");

                    // Step 5: Gracefully shutdown old pool
                    // HikariCP will drain in-flight connections before closing
                    if (oldPool != newPool) {
                        closePoolQuietly(oldPool, "[Step 2] Rotation old-pool cleanup");
                    }

                } catch (Exception e) {
                    logger.error("[Step 2] Rotation failed: {}", e.getMessage(), e);
                    if (!switched) {
                        closePoolQuietly(newPool, "[Step 2] Rotation exception cleanup");
                    }
                }
            }
        };

        try {
            // ANCHOR: nacos-listener
            // Register Nacos listener with graceful rotation
            nacos.addListener(DATA_ID, GROUP, poolListener);
            // ANCHOR_END: nacos-listener

            // Run continuous queries for 20s
            long deadline = System.currentTimeMillis() + 20_000L;
            while (System.currentTimeMillis() < deadline) {
                try (Connection conn = poolRef.get().getConnection();
                     Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT SERVER_VERSION()")) {
                    rs.next();
                    queryCount.incrementAndGet();
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                    logger.error("[Step 2] Query failed: {}", e.getMessage());
                }
                Thread.sleep(500);
            }

            logger.info("[Step 2] Done. queries={} failures={} rotations={} \u2713",
                    queryCount.get(), failureCount.get(), rotateCount.get());
        } finally {
            // Ensure cleanup even on exception
            try {
                nacos.removeListener(DATA_ID, GROUP, poolListener);
            } catch (Exception e) {
                logger.warn("[Step 2] Failed to remove Nacos listener: {}", e.getMessage());
            }
            shutdownExecutorQuietly(listenerExecutor, "step2-listener-executor");
            executorsToShutdown.remove(listenerExecutor);

            shutdownExecutorQuietly(healthCheck, "step2-health-check");
            executorsToShutdown.remove(healthCheck);
            closePoolQuietly(poolRef.get(), "[Step 2] Final cleanup");
        }
    }

    // -------------------------------------------------------------------------
    // Step 3: TMQ consumer with graceful Token rotation
    // -------------------------------------------------------------------------

    /** Build TMQ consumer with current token. */
    private static TaosConsumer<ResultSet> buildConsumer() throws SQLException {
        return buildConsumerWithToken(currentToken);
    }

    /** Build TMQ consumer with a specific token. */
    private static TaosConsumer<ResultSet> buildConsumerWithToken(String token) throws SQLException {
        Properties p = new Properties();
        p.setProperty(SecurityUtils.PROP_BOOTSTRAP_SERVERS, TDENGINE_HOST + ":" + TDENGINE_PORT);
        p.setProperty(SecurityUtils.PROP_TD_CONNECT_TYPE, "ws");
        p.setProperty("useSSL", "true");
        p.setProperty(SecurityUtils.PROP_TD_CONNECT_TOKEN, token);
        p.setProperty("auto.offset.reset", "earliest");
        p.setProperty(SecurityUtils.PROP_GROUP_ID, "nacos-demo-group");
        p.setProperty(SecurityUtils.PROP_AUTO_COMMIT, "false");
        p.setProperty("msg.with.table.name", "true");
        return new TaosConsumer<>(p);
    }

    private static void step3TmqWithRotation(ConfigService nacos) throws Exception {
        logger.info("[Step 3] Starting TMQ consumer with graceful Token rotation ...");

        AtomicInteger msgCount = new AtomicInteger(0);
        AtomicInteger rotateCount = new AtomicInteger(0);
        final List<String> tmqTopics = Collections.singletonList("demo_topic");
        final TmqRotationManager<ResultSet> tmqRotationManager =
                new TmqRotationManager<ResultSet>(NacosSecurityDemo::buildConsumerWithToken, tmqTopics, logger);

        // Use AtomicReference to allow modification in inner class
        AtomicReference<TaosConsumer<ResultSet>> consumerRef = new AtomicReference<>();
        AtomicReference<String> pendingTokenRef = new AtomicReference<>("");

        // Attempt to subscribe
        TaosConsumer<ResultSet> initialConsumer = null;
        try {
            initialConsumer = tmqRotationManager.createAndSubscribe(currentToken, "[Step 3] Initial subscribe");
            consumerRef.set(initialConsumer);
            logger.info("[Step 3] Subscribed to topic 'demo_topic' successfully");
        } catch (SQLException e) {
            // Distinguish between auth/SSL errors and topic-not-available errors
            if (SecurityUtils.isSecurityConnectError(e)) {
                tmqRotationManager.closeQuietly(initialConsumer, true, "[Step 3] Auth/SSL subscribe failure cleanup");
                logger.error("[Step 3] TMQ subscribe failed due to security connection error (token/SSL): {}",
                        e.getMessage());
                logger.error("[Step 3] Please check: 1) Token is valid  2) SSL truststore is configured");
                throw e;
            }
            // Topic not available or other non-critical errors
            logger.warn("[Step 3] TMQ subscribe skipped (topic not available): {}", e.getMessage());
            tmqRotationManager.closeQuietly(initialConsumer, true, "[Step 3] Subscribe-skip cleanup");
            logger.info("[Step 3] Rotation logic validated via Nacos listener only \u2713");
            return;
        }

        ExecutorService listenerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nacos-step3-listener");
            t.setDaemon(true);
            return t;
        });
        executorsToShutdown.add(listenerExecutor);

        Listener tmqListener = new Listener() {
            @Override
            public Executor getExecutor() {
                // IMPORTANT: Return a shared executor, do not allocate per callback.
                return listenerExecutor;
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                String newToken = SecurityUtils.parseToken(configInfo);
                if (newToken.isEmpty()) {
                    return;
                }
                String previousPending = pendingTokenRef.getAndSet(newToken);
                if (newToken.equals(previousPending) || newToken.equals(currentToken)) {
                    return;
                }
                logger.info("[Step 3] Nacos config changed -> queued TMQ rotation token={}",
                        SecurityUtils.maskToken(newToken));
            }
        };

        try {
            // Nacos listener with graceful rotation
            nacos.addListener(DATA_ID, GROUP, tmqListener);

            // Consume for 10s
            long deadline = System.currentTimeMillis() + 10_000L;
            while (System.currentTimeMillis() < deadline) {
                TaosConsumer<ResultSet> activeConsumer = consumerRef.get();
                if (activeConsumer == null) {
                    logger.warn("[Step 3] Active consumer is null, stop polling");
                    break;
                }

                String pendingToken = pendingTokenRef.getAndSet("");
                if (!pendingToken.isEmpty() && !pendingToken.equals(currentToken)) {
                    logger.info("[Step 3] Applying queued TMQ rotation token={}",
                            SecurityUtils.maskToken(pendingToken));
                    if (!validateToken(pendingToken)) {
                        logger.error("[Step 3] Queued token validation FAILED, keeping old consumer");
                    } else {
                        TmqRotationManager.RotationResult<ResultSet> result =
                                tmqRotationManager.tryRotate(activeConsumer, pendingToken, currentToken, "[Step 3] Rotation");
                        if (!result.isSwitched()) {
                            logger.error("[Step 3] Consumer rotation failed: {}", result.getFailureReason());
                        } else {
                            activeConsumer = result.getConsumer();
                            consumerRef.set(activeConsumer);
                            currentToken = pendingToken;
                            tokenCreateTime = System.currentTimeMillis();
                            rotateCount.incrementAndGet();
                            logger.info("[Step 3] TMQ consumer rotation SUCCESS");
                        }
                    }
                }

                ConsumerRecords<ResultSet> records = activeConsumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<ResultSet> rec : records) {
                    msgCount.incrementAndGet();
                }
                if (!records.isEmpty()) {
                    activeConsumer.commitSync();
                }
            }
        } finally {
            try {
                nacos.removeListener(DATA_ID, GROUP, tmqListener);
            } catch (Exception e) {
                logger.warn("[Step 3] Failed to remove Nacos listener: {}", e.getMessage());
            }
            shutdownExecutorQuietly(listenerExecutor, "step3-listener-executor");
            executorsToShutdown.remove(listenerExecutor);
            tmqRotationManager.closeQuietly(consumerRef.getAndSet(null), true, "[Step 3] Final cleanup");
        }

        logger.info("[Step 3] Done. messages={} rotations={} \u2713",
                msgCount.get(), rotateCount.get());
    }

    // -------------------------------------------------------------------------
    // Token rotation checker
    // -------------------------------------------------------------------------

    private static void startTokenRotationChecker() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "token-rotation-checker");
            t.setDaemon(true);
            return t;
        });
        executorsToShutdown.add(scheduler); // Track for cleanup

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (SecurityUtils.shouldRotateToken(tokenCreateTime)) {
                    logger.warn("Token approaching expiration, triggering proactive rotation");
                    // In production, request new token from TDengine server
                    // For demo, we log the event
                    logger.warn("Action required: Request new token before expiration");
                }
            } catch (Exception e) {
                logger.error("Token rotation check failed: {}", e.getMessage());
            }
        }, 1, 1, TimeUnit.MINUTES);  // Check every minute
    }

    // -------------------------------------------------------------------------
    // main
    // -------------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        logger.info("=== TDengine Security Validation (Nacos + SSL) ===");
        logger.info("Configuration:");
        logger.info("  Nacos: {} (user={})", NACOS_ADDR, NACOS_USER);
        logger.info("  TDengine: {}:{}", TDENGINE_HOST, TDENGINE_PORT);
        logger.info("  Truststore: -Djavax.net.ssl.trustStore");
        logger.info("  Token TTL: {}ms, Rotation threshold: {}%",
                TOKEN_TTL_MS, (int)(ROTATION_THRESHOLD * 100));

        ConfigService nacos = null;
        try {
            nacos = createConfigService();
            logger.info("[Nacos] Connected to {}", NACOS_ADDR);

            // Start token rotation checker
            startTokenRotationChecker();

            step1BasicConnect(nacos);
            step2PoolWithRotation(nacos);
            step3TmqWithRotation(nacos);

            logger.info("\n=== All steps complete ===");
        } catch (Exception e) {
            logger.error("Validation failed: {}", e.getMessage(), e);
            throw e;
        } finally {
            shutdownTrackedExecutors();
            if (nacos != null) {
                try {
                    nacos.shutDown();
                    logger.info("[Nacos] ConfigService closed");
                } catch (Exception e) {
                    logger.warn("[Nacos] Failed to close ConfigService: {}", e.getMessage());
                }
            }
        }
    }
}
