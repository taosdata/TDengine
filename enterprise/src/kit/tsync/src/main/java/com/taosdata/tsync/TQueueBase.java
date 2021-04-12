package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.domain.Topic;
import com.taosdata.tsync.domain.TopicPartition;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TQueueBase {

    protected Connection connection;
    protected Map<String, Topic> topics = new HashMap<>();
    protected Map<Integer, TopicPartition> partitions = new HashMap<>();

    public TQueueBase(Properties properties) {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");

        final String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=" + user + "&password=" + password;
        try {
            this.connection = DriverManager.getConnection(url, properties);
            flushTopics();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private void flushTopics() {
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
            e.printStackTrace();
        }
    }
}
