package com.taosdata.tsync.tqueue;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.entity.Topic;
import com.taosdata.tsync.entity.TopicPartition;
import com.taosdata.tsync.enums.TQueueConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TQueueBase {
    private static final Logger logger = LoggerFactory.getLogger(TQueueBase.class);
//    public static final long INVALID_OFFSET = -1;

    protected Connection connection;
    protected Map<String, Topic> topics = new HashMap<>();
    protected Map<Integer, TopicPartition> partitions = new HashMap<>();

    public TQueueBase(Properties properties) {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        if (host == null || host.isEmpty() || host.replaceAll("\\s", "").isEmpty()) {
            throw new RuntimeException("TQueue error: host is null");
        }
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT, String.valueOf(TQueueConstants.DEFAULT_PORT));
        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER, TQueueConstants.DEFAULT_USER);
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TQueueConstants.DEFAULT_PASSWORD);

        final String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=" + user + "&password=" + password;
        try {
            this.connection = DriverManager.getConnection(url, properties);
            flushTopics();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    public Topic getTopic(String topic) {
        return topics.get(topic);
    }

    public synchronized boolean containsTopic(String topic) {
        if (topics.containsKey(topic))
            return true;
        flushTopics();
        return topics.containsKey(topic);
    }

    protected void flushTopics() {
        topics.clear();
        // get all topics in tqueue
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show topics");
            while (rs.next()) {
                String topic = rs.getString("name");
                Timestamp created_time = rs.getTimestamp("created_time");
                int partitions = rs.getInt("partitions");
                topics.put(topic, new Topic(topic, partitions, created_time));
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    protected void flushTopicPartitions() {
        flushTopics();
        this.partitions.clear();
        for (String topic : topics.keySet()) {
            int partitions = topics.get(topic).partitions();
            for (int i = 1; i <= partitions; i++) {
                TopicPartition topicPartition = new TopicPartition(topic, i);
                this.partitions.put(topicPartition.hashCode(), topicPartition);
            }
        }
    }

    protected void writeMessage(String topic, int partition, long offset, long ts, byte[] messages) {
        final String update = "insert into " + topic + ".p" + partition + "(off, ts, content) values(?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(update)) {
            pstmt.setLong(1, offset);
            pstmt.setTimestamp(2, new Timestamp(ts));
            pstmt.setBytes(3, messages);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }


}
