package com.taosdata.jdbc.common;

import com.taosdata.jdbc.tmq.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public interface Consumer<V> {

    /**
     * create consumer
     *
     * @param properties ip / port / user / password and so on.
     * @throws SQLException jni exception
     */
    void create(Properties properties) throws SQLException;

    /**
     * subscribe topics
     *
     * @param topics collection of topics
     */
    void subscribe(Collection<String> topics) throws SQLException;

    /**
     * unsubscribe topics
     */
    void unsubscribe() throws SQLException;

    /**
     * get subscribe topics
     *
     * @return topic set
     */
    Set<String> subscription() throws SQLException;

    /**
     * get result records
     *
     * @param timeout      wait time for poll data
     * @param deserializer convert resultSet to javaBean
     * @return ConsumerRecord is the collection of javaBean
     * @throws SQLException java reflect exception or resultSet convert exception
     */
    ConsumerRecords<V> poll(Duration timeout, Deserializer<V> deserializer) throws SQLException;

    /**
     * commit offset with sync
     */
    void commitSync() throws SQLException;

    /**
     * close consumer
     */
    void close() throws SQLException;

    void commitAsync(OffsetCommitCallback<V> callback) throws SQLException;

    /**
     * If this API is invoked for the same partition more than once, the latest offset will be used on the next poll().
     */
    void seek(TopicPartition partition, long offset) throws SQLException;

    /**
     * Get the offset of the next record that will be fetched.
     */
    long position(TopicPartition partition) throws SQLException;

    Map<TopicPartition, Long> position(String topic) throws SQLException;

    Map<TopicPartition, Long> beginningOffsets(String topic) throws SQLException;

    Map<TopicPartition, Long> endOffsets(String topic) throws SQLException;

    void seekToBeginning(Collection<TopicPartition> partitions) throws SQLException;

    void seekToEnd(Collection<TopicPartition> partitions) throws SQLException;

    /**
     * Get the set of partitions currently assigned to this consumer.
     */
    Set<TopicPartition> assignment() throws SQLException;

    /**
     * Get the last committed offset for the given partition.
     * This offset will be used as the position for the consumer in the event of a failure.
     */
    OffsetAndMetadata committed(TopicPartition partition) throws SQLException;

    Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) throws SQLException;

    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) throws SQLException;

    void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback<V> callback);

}
