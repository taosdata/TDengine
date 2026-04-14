package com.taos.example.security;

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
 * Scenario 3: TMQ subscription with 80%-lifetime proactive rotation + fallback on auth error.
 *
 * Prerequisites: TDengine Enterprise with SSL enabled, topic already created.
 *
 * Run (set env var first):
 *   export TDENGINE_TOKEN=your_token
 *   cd docs/examples/JDBC/JDBCDemo
 *   mvn compile exec:java -Dexec.mainClass=com.taos.example.security.SecurityTmqDemo
 */
public class SecurityTmqDemo {

    private static final Logger logger = LoggerFactory.getLogger(SecurityTmqDemo.class);

    private static final String HOST  = SecurityUtils.getEnv("TDENGINE_HOST", "td1.internal.taosdata.com");
    private static final int    PORT  = Integer.parseInt(SecurityUtils.getEnv("TDENGINE_PORT", "6041"));
    private static final String GROUP = "my-consumer-group";
    private static final List<String> TOPICS =
            Collections.singletonList("my-topic");

    // ANCHOR: consumer-build
    /** Current Token - updated by config center */
    private static volatile String currentToken =
            System.getenv().getOrDefault("TDENGINE_TOKEN", "");

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
     * Consume loop with proactive rotation (80% TTL + jitter) and auth-error recovery.
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
        if (currentToken == null || currentToken.isEmpty()) {
            throw new IllegalStateException("TDENGINE_TOKEN is required for TMQ demo");
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
                    // Fallback: auth error triggers immediate rotation
                    if (SecurityUtils.isAuthError(e)) {
                        logger.warn("[TMQ] auth error detected, attempting recovery rotation: {}", e.getMessage());
                        TmqRotationManager.RotationResult<Map<String, Object>> rotation =
                                rotateConsumer(consumer, "[TMQ] auth-recovery");
                        if (!rotation.isSwitched()) {
                            logger.error("[TMQ] auth recovery failed: {}", rotation.getFailureReason());
                            throw e;
                        }

                        consumer = rotation.getConsumer();
                        createdAt = System.currentTimeMillis();
                        refreshAt = nextRefreshAt(createdAt, tokenTtlMs);
                        logger.info("[TMQ] auth recovery rotation succeeded");
                        continue;
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

    /** Fetch the latest Token from the config center (replace with Nacos / Vault / K8s Secret SDK). */
    private static String fetchNewToken() {
        return System.getenv().getOrDefault("TDENGINE_TOKEN", currentToken);
    }

    public static void main(String[] args) throws Exception {
        long tokenTtlMs = 3_600_000L;  // example Token TTL = 1 hour
        consumeWithRotation(tokenTtlMs);
    }
}
