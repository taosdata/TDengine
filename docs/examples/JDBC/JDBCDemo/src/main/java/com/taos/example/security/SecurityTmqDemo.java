package com.taos.example.security;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Scenario 3: TMQ subscription with 80%-lifetime proactive rotation.
 *
 * Prerequisites: TDengine Enterprise with SSL enabled, topic already created.
 *
 * Run (set env var first):
 *   export TDENGINE_NACOS_ADDR=localhost:8848
 *   export TDENGINE_NACOS_USER=nacos
 *   export TDENGINE_NACOS_PASSWORD=nacos
 *   cd docs/examples/JDBC/JDBCDemo
 *   mvn compile exec:java -Dexec.mainClass=com.taos.example.security.SecurityTmqDemo
 */
public class SecurityTmqDemo {

    private static final Logger logger = LoggerFactory.getLogger(SecurityTmqDemo.class);

    private static final String HOST  = SecurityUtils.getEnv("TDENGINE_HOST", "localhost");
    private static final int    PORT  = Integer.parseInt(SecurityUtils.getEnv("TDENGINE_PORT", "6041"));
    private static final String GROUP = "my-consumer-group";
    private static final String NACOS_ADDR = SecurityUtils.getEnv("TDENGINE_NACOS_ADDR", "localhost:8848");
    private static final String NACOS_USER = SecurityUtils.getEnv("TDENGINE_NACOS_USER", "nacos");
    private static final String NACOS_PASSWORD = SecurityUtils.getEnv("TDENGINE_NACOS_PASSWORD", "nacos");
    private static final List<String> TOPICS =
            Collections.singletonList("my-topic");

    // ANCHOR: consumer-build
    /** Current Token - updated by config center */
    private static volatile String currentToken =
            System.getenv().getOrDefault("TDENGINE_TOKEN", "");
    private static volatile ConfigService nacosConfigService;

    private static final TmqRotationManager<Map<String, Object>> TMQ_ROTATION_MANAGER =
            new TmqRotationManager<Map<String, Object>>(SecurityTmqDemo::buildConsumerWithToken, TOPICS, logger);

    /**
     * Build a TMQ Consumer with the current Token and SSL (wss scheme).
     * Using the same groupId after rotation resumes from the last committed offset.
     */
    private static TaosConsumer<Map<String, Object>> buildConsumer() throws SQLException {
        return buildConsumerWithToken(currentToken);
    }

    /**
     * Build a TMQ Consumer with a specific Token and SSL (wss scheme).
     */
    private static TaosConsumer<Map<String, Object>> buildConsumerWithToken(String token) throws SQLException {
        Properties p = new Properties();

        p.setProperty("bootstrap.servers", HOST + ":" + PORT);
        p.setProperty("td.connect.type",   "ws");                // use WebSocket consumer
        p.setProperty("useSSL", "true");                        // enable SSL for TMQ
        p.setProperty("td.connect.token",  token);              // TMQ token for subscription auth
        p.setProperty("auto.offset.reset", "earliest");         // consume from earliest offset for easy testing
        p.setProperty("group.id",          GROUP);
        p.setProperty("enable.auto.commit", "false");           // manual commitSync
        p.setProperty("msg.with.table.name", "true");
        return new TaosConsumer<Map<String, Object>>(p);
    }
    // ANCHOR_END: consumer-build

    private static long nextRefreshAt(long createdAt, long tokenTtlMs) {
        return createdAt
                + (long) (tokenTtlMs * SecurityUtils.ROTATION_THRESHOLD)
                + (long) (Math.random() * tokenTtlMs * 0.1);
    }

    private static TmqRotationManager.RotationResult<Map<String, Object>> rotateConsumer(
            TaosConsumer<Map<String, Object>> consumer, String stage) {
        String newToken = fetchNewToken();
        TmqRotationManager.RotationResult<Map<String, Object>> result =
                TMQ_ROTATION_MANAGER.tryRotate(consumer, newToken, currentToken, stage);
        if (result.isSwitched()) {
            currentToken = newToken;
        }
        return result;
    }

    // ANCHOR: tmq-rotation
    /**
     * Consume loop with proactive rotation (80% TTL + jitter).
     *
     * Actual rotation order (implemented by {@link TmqRotationManager#tryRotate}):
     *   1. fetchNewToken()      - retrieve a fresh token from the config center
     *   2. build + subscribe    - create and verify a new consumer first
     *   3. commitSync()         - best-effort commit on old consumer
     *   4. unsubscribe + close  - release old consumer resources
     *   5. switch reference     - caller adopts the new consumer
     *
     * Note:
     * - If step 2 fails, keep using the old consumer.
     * - If step 3 fails, rotation still continues to avoid auth-expired deadlock.
     */
    public static void consumeWithRotation(long tokenTtlMs) throws Exception {
        currentToken = fetchNewToken();
        if (currentToken == null || currentToken.isEmpty()) {
            throw new IllegalStateException(
                    "No token found from Nacos config (dataId="
                            + SecurityUtils.DATA_ID + ", group=" + SecurityUtils.GROUP + ")");
        }

        long createdAt = System.currentTimeMillis();
        long refreshAt = nextRefreshAt(createdAt, tokenTtlMs);

        TaosConsumer<Map<String, Object>> consumer =
                TMQ_ROTATION_MANAGER.createAndSubscribe(currentToken, "[TMQ] initial");

        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ConsumerRecords<Map<String, Object>> records =
                            consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<Map<String, Object>> record : records) {
                        // process business logic here
                         Map<String, Object> map = record.value();
                        // processRecord(map);
                    }
                    if (!records.isEmpty()) {
                        consumer.commitSync();  // manual offset commit
                    }

                    // Proactive rotation: triggered at 80% of token lifetime (+ jitter)
                    if (System.currentTimeMillis() >= refreshAt) {
                        TmqRotationManager.RotationResult<Map<String, Object>> rotation =
                                rotateConsumer(consumer, "[TMQ] proactive-rotation");
                        if (!rotation.isSwitched()) {
                            logger.warn("[TMQ] proactive rotation failed: {}", rotation.getFailureReason());
                            refreshAt += SecurityUtils.ROTATION_RETRY_DELAY_MS;
                            continue;
                        }

                        consumer = rotation.getConsumer();
                        createdAt = System.currentTimeMillis();
                        refreshAt = nextRefreshAt(createdAt, tokenTtlMs);
                        logger.info("[TMQ] proactive rotation succeeded");
                    }

                } catch (SQLException e) {
                    if (SecurityUtils.isAuthFailure(e)) {
                        logger.warn("[TMQ] authentication failure detected, attempting token rotation recovery: {}",
                                e.getMessage());
                        TmqRotationManager.RotationResult<Map<String, Object>> rotation =
                                rotateConsumer(consumer, "[TMQ] auth-failure-rotation");
                        if (!rotation.isSwitched()) {
                            logger.error("[TMQ] auth-failure rotation recovery failed: {}",
                                    rotation.getFailureReason());
                            throw e;
                        }
                        consumer = rotation.getConsumer();
                        createdAt = System.currentTimeMillis();
                        refreshAt = nextRefreshAt(createdAt, tokenTtlMs);
                        logger.info("[TMQ] auth-failure rotation recovery succeeded");
                        continue;
                    }
                    if (SecurityUtils.isSecurityConnectError(e)) {
                        logger.error("[TMQ] security connection error (token/SSL). "
                                + "Check token validity and TLS trust chain: {}", e.getMessage());
                    }
                    throw e;
                }
            }
        } finally {
            try {
                consumer.commitSync();
            } catch (SQLException e) {
                logger.warn("[TMQ] final commit failed: {}", e.getMessage());
            }
            TMQ_ROTATION_MANAGER.closeQuietly(consumer, true, "[TMQ] final cleanup");
        }
    }
    // ANCHOR_END: tmq-rotation

    private static ConfigService getConfigService() throws Exception {
        if (nacosConfigService != null) {
            return nacosConfigService;
        }
        synchronized (SecurityTmqDemo.class) {
            if (nacosConfigService == null) {
                Properties p = new Properties();
                p.put("serverAddr", NACOS_ADDR);
                p.put("username", NACOS_USER);
                p.put("password", NACOS_PASSWORD);
                nacosConfigService = NacosFactory.createConfigService(p);
            }
            return nacosConfigService;
        }
    }

    private static void closeConfigServiceQuietly() {
        ConfigService configService = nacosConfigService;
        if (configService == null) {
            return;
        }
        try {
            configService.shutDown();
        } catch (Exception e) {
            logger.warn("[TMQ] failed to close Nacos config service: {}", e.getMessage());
        } finally {
            nacosConfigService = null;
        }
    }

    /** Fetch the latest Token from Nacos config. */
    private static String fetchNewToken() {
        try {
            String content = getConfigService().getConfig(SecurityUtils.DATA_ID, SecurityUtils.GROUP, 3000);
            String token = SecurityUtils.parseToken(content);
            if (!token.isEmpty()) {
                return token;
            }
            logger.warn("[TMQ] token not found in Nacos config content: dataId={}, group={}",
                    SecurityUtils.DATA_ID, SecurityUtils.GROUP);
        } catch (Exception e) {
            logger.warn("[TMQ] failed to fetch token from Nacos: {}", e.getMessage());
        }
        return currentToken;
    }

    public static void main(String[] args) throws Exception {
        long tokenTtlMs = 3_600_000L;  // example Token TTL = 1 hour
        try {
            consumeWithRotation(tokenTtlMs);
        } finally {
            closeConfigServiceQuietly();
        }
    }
}
