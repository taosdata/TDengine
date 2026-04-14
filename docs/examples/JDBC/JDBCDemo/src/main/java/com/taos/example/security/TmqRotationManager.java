package com.taos.example.security;

import com.taosdata.jdbc.tmq.TaosConsumer;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * Shared TMQ rotation helper used by security demos.
 *
 * <p>Rotation order:
 * 1) build + subscribe new consumer
 * 2) commit current offsets (best effort)
 * 3) close old consumer
 * 4) switch caller reference to new consumer
 */
final class TmqRotationManager<T> {

    @FunctionalInterface
    interface ConsumerFactory<T> {
        TaosConsumer<T> create(String token) throws SQLException;
    }

    static final class RotationResult<T> {
        private final TaosConsumer<T> consumer;
        private final boolean switched;
        private final String failureReason;

        private RotationResult(TaosConsumer<T> consumer, boolean switched, String failureReason) {
            this.consumer = consumer;
            this.switched = switched;
            this.failureReason = failureReason;
        }

        static <T> RotationResult<T> switched(TaosConsumer<T> consumer) {
            return new RotationResult<T>(consumer, true, "");
        }

        static <T> RotationResult<T> failed(String reason) {
            return new RotationResult<T>(null, false, reason == null ? "unknown error" : reason);
        }

        TaosConsumer<T> getConsumer() {
            return consumer;
        }

        boolean isSwitched() {
            return switched;
        }

        String getFailureReason() {
            return failureReason;
        }
    }

    private final ConsumerFactory<T> consumerFactory;
    private final List<String> topics;
    private final Logger logger;

    TmqRotationManager(ConsumerFactory<T> consumerFactory, List<String> topics, Logger logger) {
        this.consumerFactory = consumerFactory;
        this.topics = topics == null ? Collections.<String>emptyList() : topics;
        this.logger = logger;
    }

    TaosConsumer<T> createAndSubscribe(String token, String stage) throws SQLException {
        TaosConsumer<T> consumer = consumerFactory.create(token);
        try {
            consumer.subscribe(topics);
            logger.info("{}: subscribed topics={}", stage, topics);
            return consumer;
        } catch (SQLException e) {
            closeQuietly(consumer, true, stage + " subscribe failure cleanup");
            throw e;
        }
    }

    RotationResult<T> tryRotate(TaosConsumer<T> currentConsumer, String newToken, String currentToken, String stage) {
        if (newToken == null || newToken.trim().isEmpty()) {
            return RotationResult.failed("new token is empty");
        }

        if (newToken.equals(currentToken)) {
            return RotationResult.failed("new token is the same as current token");
        }        

        TaosConsumer<T> newConsumer;
        try {
            newConsumer = createAndSubscribe(newToken, stage + " new-consumer");
        } catch (SQLException e) {
            return RotationResult.failed("failed to build/subscribe new consumer: " + e.getMessage());
        }

        if (currentConsumer != null) {
            try {
                currentConsumer.commitSync();
                logger.info("{}: current offsets committed", stage);
            } catch (SQLException e) {
                // Keep switching even if commit fails, to avoid auth-expired deadlock.
                logger.warn("{}: offset commit failed: {}", stage, e.getMessage());
            }
        }

        if (currentConsumer != newConsumer) {
            closeQuietly(currentConsumer, true, stage + " old-consumer cleanup");
        }
        return RotationResult.switched(newConsumer);
    }

    void closeQuietly(TaosConsumer<T> consumer, boolean unsubscribe, String stage) {
        if (consumer == null) {
            return;
        }
        if (unsubscribe) {
            try {
                consumer.unsubscribe();
            } catch (Exception e) {
                logger.warn("{}: failed to unsubscribe consumer: {}", stage, e.getMessage());
            }
        }
        try {
            consumer.close();
            logger.info("{}: consumer closed", stage);
        } catch (Exception e) {
            logger.warn("{}: failed to close consumer: {}", stage, e.getMessage());
        }
    }
}
