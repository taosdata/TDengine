package com.taosdata.tsync.tqueue;

import com.taosdata.tsync.entity.ConsumerRecord;
import com.taosdata.tsync.entity.TopicPartition;
import com.taosdata.tsync.enums.TQueueConstants;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class TQueueConsumer extends TQueueBase {
    private static final Logger logger = LoggerFactory.getLogger(TQueueConsumer.class);
    private static final long STARTED_OFFSET = 0;
    private static final long INVALID_OFFSET = -1;

    private final Object LOCK = new Object();
    private String topic;
    private int partition;
    private volatile int cur_topic_partition_hash;
    private final Map<Integer, AtomicLong> partitionOffsets = new HashMap<>();

    public TQueueConsumer(Properties properties) {
        super(properties);
//        if (!isOffsetDatabaseExist()) {
//            createOffsetDatabase();
//        }
//        if (!isOffsetTableExist()) {
//            createOffsetTable();
//            logger.warn("table[" + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + "." + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + "] is not exists, and all partitions' offset in topic:" + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + " will be set to 0");
//        }
    }

    /*
    private boolean isOffsetDatabaseExist() {
        boolean isExist = false;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show databases");
            while (rs.next()) {
                String dbname = rs.getString("name");
                if (TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME.equals(dbname)) {
                    isExist = true;
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return isExist;
    }

    private void createOffsetDatabase() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("create database if not exists " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isOffsetTableExist() {
        boolean isExist = false;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("use " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME);
            ResultSet rs = stmt.executeQuery("show tables like '" + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + "'");
            while (rs.next()) {
                String table_name = rs.getString("table_name");
                if (TQueueConstants.DEFAULT_OFFSET_TABLE_NAME.equals(table_name)) {
                    isExist = true;
                    break;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return isExist;
    }

    private void createOffsetTable() {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("create table if not exists " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + "." + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + " (ts timestamp, _topic nchar(192), _partition int, _offset bigint)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
     */

    /**
     * 为Consumer指定要消费的主题和分区，每次调用这个方法会覆盖当前consumer正在消费的主题和分区
     *
     * @param topic
     * @param partition
     * @return
     */
    public long assign(String topic, int partition) {
        int hashCode = TopicPartition.hashCode(topic, partition);
        // return current offset if topicPartition already assigned
        if (hashCode == TopicPartition.hashCode(this.topic, this.partition))
            return partitionOffsets.get(hashCode).get();

        long currentOffset;
        if (!partitionOffsets.containsKey(hashCode)) {
            // use current offset from tqueue: topic_info.partition_offset
            currentOffset = queryCurrentOffsetFromTQueue(topic, partition);
            if (currentOffset == INVALID_OFFSET) {
                writeOffset(topic, partition, STARTED_OFFSET);
                currentOffset = STARTED_OFFSET;
            }
            partitionOffsets.put(hashCode, new AtomicLong(currentOffset));
        } else {
            currentOffset = partitionOffsets.get(hashCode).get();
        }

        this.topic = topic;
        this.partition = partition;
        this.cur_topic_partition_hash = hashCode;
        return currentOffset;
    }

    /**
     * 为Consumer指定要消费的主题和分区，每次调用这个方法会覆盖当前consumer正在消费的主题、分区、offset
     *
     * @param topic
     * @param partition
     * @param offset
     * @return
     * @throws TQueueException
     */
    private long assign(String topic, int partition, long offset) throws TQueueException {
        int hashCode = TopicPartition.hashCode(topic, partition);
        // return current offset if topicPartition already assigned
        if (hashCode == TopicPartition.hashCode(this.topic, this.partition))
            return partitionOffsets.get(hashCode).get();

        if (offset < 0) {
            String errorMsg = "offset is less than 0";
            logger.error(errorMsg);
            throw new TQueueException(errorMsg);
        }

        // assign a new topicPartition for consumer
        if (!partitions.containsKey(hashCode)) {
            flushTopicPartitions();
            if (!partitions.containsKey(hashCode)) {
                String message = "topic: " + topic + ", partition: " + partition + " not exists!";
                logger.error(message);
                throw new TQueueException(message);
            }
        }

        this.topic = topic;
        this.partition = partition;
        this.cur_topic_partition_hash = hashCode;
        this.partitionOffsets.put(hashCode, new AtomicLong(offset));
        return offset;
    }

    private long queryCurrentOffsetFromTQueue(String topic, int partition) {
        long offset = INVALID_OFFSET;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(_offset) from " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + "." + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + " where _topic = '" + topic + "' and _partition = " + partition);
            while (rs.next()) {
                offset = rs.getLong("last_row(_offset)");
            }
        } catch (SQLException e) {
            logger.warn(e.getMessage());
            e.printStackTrace();
        }
        return offset;
    }

    private void writeOffset(String topic, int partition, long offset) {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("insert into " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + "." + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + " values(now, '" + topic + "', " + partition + ", " + offset + ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<ConsumerRecord> poll() throws TQueueException {
        if (topic == null || partition == 0) {
            String message = "topic: " + topic + ", partition: " + partition + " is invalid";
            logger.error(message);
            throw new TQueueException(message);
        }

        List<ConsumerRecord> records;
        synchronized (LOCK) {
            records = fetchRows();
            if (!records.isEmpty()) {
                long currentOffset = records.get(records.size() - 1).offset();
                this.partitionOffsets.get(cur_topic_partition_hash).getAndSet(currentOffset);
            }
        }

        return records;
    }

    private List<ConsumerRecord> fetchRows() throws TQueueException {
        List<ConsumerRecord> records = new ArrayList<>();

        final String sql = "select * from " + topic + ".p" + partition + " where off > ? order by off asc";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            long currentOffset = partitionOffsets.get(cur_topic_partition_hash).get();
            pstmt.setLong(1, currentOffset);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                long offset = Utils.toMicroSecond(rs.getTimestamp(1));
                long ts = rs.getTimestamp(2).getTime();
                byte[] message = rs.getBytes(3);

                if (offset <= currentOffset) {
                    logger.error("queryOffset: " + offset + " < currentOffset: " + currentOffset);
                } else {
                    ConsumerRecord consumerRecord = new ConsumerRecord(topic, partition, offset, ts, message);
                    records.add(consumerRecord);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new TQueueException(e.getMessage());
        }
        return records;
    }

}