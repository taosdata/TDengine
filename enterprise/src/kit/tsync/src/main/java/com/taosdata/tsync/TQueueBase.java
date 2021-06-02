package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.entity.Topic;
import com.taosdata.tsync.entity.TopicPartition;
import com.taosdata.tsync.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TQueueBase {
    private static final Logger logger = LogManager.getLogger(TQueueBase.class);
    public static final long INVALID_OFFSET = -1;

    protected Connection connection;
    protected Map<String, Topic> topics = new HashMap<>();
    protected Map<Integer, TopicPartition> partitions = new HashMap<>();

    public TQueueBase(Properties properties) {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        if (host == null || host.isEmpty() || host.replaceAll("\\s", "").isEmpty())
            throw new RuntimeException("host is null");
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT, "6041");
        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
//        if (!properties.containsKey(ConsumerConfig.TIMESTAMP_FORMAT))
//            properties.setProperty(ConsumerConfig.CHARSET_CONFIG, "UTF-8");

        final String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=" + user + "&password=" + password;
        try {
            this.connection = DriverManager.getConnection(url, properties);
            flushTopics();
        } catch (SQLException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
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

    protected long currentOffset(String topic, int partition) {
        long offset = INVALID_OFFSET;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(off) from " + topic + ".p" + partition);
            while (rs.next()) {
                offset = Utils.toMicroSecond(rs.getTimestamp(1));
            }
            if (offset == INVALID_OFFSET) {
                // TODO: should change to "select count(off) from topic.p1"
                ResultSet rss = stmt.executeQuery("select * from " + topic + ".p" + partition);
                if (rss.wasNull())
                    offset = 0;
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return offset;
    }
}
