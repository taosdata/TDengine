package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.common.Consumer;
import com.taosdata.jdbc.common.ConsumerManager;
import com.taosdata.jdbc.utils.StringUtils;
import com.taosdata.jdbc.utils.Utils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TaosConsumer<V> implements AutoCloseable {

    private static final long NO_CURRENT_THREAD = -1L;
    // currentThread holds the threadId of the current thread accessing
    // used to prevent multi-threaded access
    private final AtomicLong currentThread = new AtomicLong(NO_CURRENT_THREAD);
    // refcount is used to allow reentrant access by the thread who has acquired currentThread
    private final AtomicInteger refcount = new AtomicInteger(0);
    private volatile boolean closed = false;

    private final Consumer<V> consumer;
    private final Deserializer<V> deserializer;

    /**
     * Note: after creating a {@link TaosConsumer} you must always {@link #close()}
     * it to avoid resource leaks.
     */
    @SuppressWarnings("unchecked")
    public TaosConsumer(Properties properties) throws SQLException {
        if (null == properties)
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_TMQ_CONF_NULL, "consumer properties must not be null!");

        String servers = properties.getProperty(TMQConstants.BOOTSTRAP_SERVERS);
        if (!StringUtils.isEmpty(servers)) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, servers);
        }

        String s = properties.getProperty(TMQConstants.VALUE_DESERIALIZER);
        if (!StringUtils.isEmpty(s)) {
            deserializer = (Deserializer<V>) Utils.newInstance(Utils.parseClassType(s));
        } else {
            deserializer = (Deserializer<V>) new MapDeserializer();
        }

        deserializer.configure(properties);
        String type = properties.getProperty(TMQConstants.CONNECT_TYPE);
        consumer = (Consumer<V>) ConsumerManager.getConsumer(type);
        consumer.create(properties);
    }

    public void subscribe(Collection<String> topics) throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.subscribe(topics);
        } finally {
            release();
        }
    }

    public void unsubscribe() throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.unsubscribe();
        } finally {
            release();
        }
    }

    public Set<String> subscription() throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.subscription();
        } finally {
            release();
        }
    }

    public ConsumerRecords<V> poll(Duration timeout) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.poll(timeout, deserializer);
        } finally {
            release();
        }
    }

    /**
     * jni consumer will call back whit error code when commit async
     *
     * @param code error code
     * @deprecated This callback method is no longer used.
     */
    @Deprecated
    public void commitCallbackHandler(int code) {
    }

    @SuppressWarnings("unused")
    public void commitAsync() throws SQLException {
        consumer.commitAsync((r, e) -> {
        });
    }

    @SuppressWarnings("unused")
    public void commitAsync(OffsetCommitCallback<V> callback) throws SQLException {
        consumer.commitAsync(callback);
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback<V> callback) throws SQLException {
        consumer.commitAsync(offsets, callback);
    }

    @SuppressWarnings("unused")
    public void commitSync() throws SQLException {
        consumer.commitSync();
    }

    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) throws SQLException {
        consumer.commitSync(offsets);
    }

    public void seek(TopicPartition partition, long offset) throws SQLException {
        if (offset < 0)
            throw TSDBError.createIllegalArgumentException(TSDBErrorNumbers.ERROR_TMQ_SEEK_OFFSET);

        acquireAndEnsureOpen();
        try {
            consumer.seek(partition, offset);
        } finally {
            release();
        }
    }

    public long position(TopicPartition tp) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.position(tp);
        } finally {
            release();
        }
    }

    public Map<TopicPartition, Long> position(String topic) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.position(topic);
        } finally {
            release();
        }
    }

    public Map<TopicPartition, Long> beginningOffsets(String topic) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.beginningOffsets(topic);
        } finally {
            release();
        }
    }

    public Map<TopicPartition, Long> endOffsets(String topic) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.endOffsets(topic);
        } finally {
            release();
        }
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.seekToBeginning(partitions);
        } finally {
            release();
        }
    }

    public void seekToEnd(Collection<TopicPartition> partitions) throws SQLException {
        acquireAndEnsureOpen();
        try {
            consumer.seekToEnd(partitions);
        } finally {
            release();
        }
    }

    public Set<TopicPartition> assignment() throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.assignment();
        } finally {
            release();
        }
    }

    public OffsetAndMetadata committed(TopicPartition partition) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.committed(partition);
        } finally {
            release();
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) throws SQLException {
        acquireAndEnsureOpen();
        try {
            return consumer.committed(partitions);
        } finally {
            release();
        }
    }

    /**
     * Acquire the light lock and ensure that the consumer hasn't been closed.
     *
     * @throws IllegalStateException If the consumer has been closed
     */
    private void acquireAndEnsureOpen() {
        acquire();
        if (this.closed) {
            release();
            throw new IllegalStateException("This consumer has already been closed.");
        }
    }

    /**
     * Acquire the light lock protecting this consumer from multi-threaded access.
     * Instead of blocking when the lock is not available, however,
     * we just throw an exception (since multi-threaded usage is not supported).
     *
     * @throws ConcurrentModificationException if another thread already has the lock
     */
    private void acquire() {
        long threadId = Thread.currentThread().getId();
        if (threadId != currentThread.get() && !currentThread.compareAndSet(NO_CURRENT_THREAD, threadId))
            throw new ConcurrentModificationException("Consumer is not safe for multi-threaded access");
        refcount.incrementAndGet();
    }

    /**
     * Release the light lock protecting the consumer from multi-threaded access.
     */
    private void release() {
        if (refcount.decrementAndGet() == 0)
            currentThread.set(NO_CURRENT_THREAD);
    }

    @Override
    public void close() throws SQLException {
        acquire();
        try {
            consumer.close();
        } finally {
            closed = true;
            release();
        }
    }
}
