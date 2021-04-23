package com.taosdata.tsync;

import com.taosdata.tsync.domain.ConsumerRecord;
import com.taosdata.tsync.domain.Topic;
import com.taosdata.tsync.domain.TopicPartition;
import com.taosdata.tsync.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TQueueConsumer extends TQueueBase {
    private static final Logger logger = LogManager.getLogger(TQueueConsumer.class);

    private static final String UNSUBSCRIBE_TOPIC = null;
    private static final int UNSUBSCRIBE_PARTITION = 0;

    private String topic = UNSUBSCRIBE_TOPIC;
    private int partition = UNSUBSCRIBE_PARTITION;
    private Map<Integer, AtomicLong> partitionOffsets = new HashMap<>();
    private final Object LOCK = new Object();
    private int cur_topic_partition_hash;

    public TQueueConsumer(Properties properties) {
        super(properties);
        for (String topic : topics.keySet()) {
            Topic t = topics.get(topic);
            for (int pIndex = 1; pIndex <= t.partitions(); pIndex++) {
                int hashCode = TopicPartition.hashCode(topic, pIndex);
                partitionOffsets.put(hashCode, new AtomicLong(TQueueBase.INVALID_OFFSET));
            }
        }
    }

    /**
     * 为Consumer指定要消费的主题和分区，每次调用这个方法会覆盖上次的主题和分区
     *
     * @param topic
     * @param partition
     * @return
     */
    public long assign(String topic, int partition) {
        int hashCode = TopicPartition.hashCode(topic, partition);
        // return current offset if topicPartition already assigned
        if (hashCode == TopicPartition.hashCode(this.topic, partition))
            return partitionOffsets.get(hashCode).get();

        // assign a new topicPartition for consumer
        if (!partitions.containsKey(hashCode)) {
            flushTopicPartitions();
            if (!partitions.containsKey(hashCode)) {
                String message = "topic-partition: " + topic + "-" + partition + " not exists!";
                logger.error(message);
                throw new IllegalArgumentException(message);
            }
        }
        this.topic = topic;
        this.partition = partition;
        cur_topic_partition_hash = hashCode;
        //flush topic-partition offset
        synchronized (LOCK) {
            long off = currentOffset(topic, partition);
            if (off != INVALID_OFFSET)
                this.partitionOffsets.get(hashCode).getAndSet(off);
        }
        return this.partitionOffsets.get(hashCode).get();
    }

    public List<ConsumerRecord> poll(Duration timeout) throws Exception {
        if (topic.equals(UNSUBSCRIBE_TOPIC) || partition == UNSUBSCRIBE_PARTITION) {
            String message = "topic-partition: " + topic + "-" + partition + " is invalid";
            logger.error(message);
            throw new Exception(message);
        }

        if (timeout.isNegative()) {
            String message = "timeout value is negative";
            logger.error(message);
            throw new Exception(message);
        }

        if (partitionOffsets.get(cur_topic_partition_hash).get() == INVALID_OFFSET) {
            String message = "the offset for topic-partition:" + topic + "-" + partition + " is " + INVALID_OFFSET;
            logger.error(message);
            throw new Exception(message);
        }

        synchronized (LOCK) {
            long start = System.nanoTime();
            do {
                List<ConsumerRecord> records = fetchRows();
                if (!records.isEmpty()) {
                    this.partitionOffsets.get(cur_topic_partition_hash).getAndSet(records.get(records.size() - 1).offset());
                }
//                logger.debug("offset: " + partitionOffsets.get(cur_topic_partition_hash).get());
                return records;
            } while ((System.nanoTime() - start) * 1000 < timeout.toNanos());
        }
    }

    private List<ConsumerRecord> fetchRows() {
        List<ConsumerRecord> records = new ArrayList<>();
        final String sql = "select * from " + topic + ".p" + partition + " where off > ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setLong(1, partitionOffsets.get(cur_topic_partition_hash).get());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                long offset = Utils.toMicroSecond(rs.getTimestamp(1));
                long ts = rs.getTimestamp(2).getTime();
                byte[] message = rs.getBytes(3);
                ConsumerRecord consumerRecord = new ConsumerRecord(topic, partition, offset, ts, message);
                records.add(consumerRecord);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return records;
    }


}