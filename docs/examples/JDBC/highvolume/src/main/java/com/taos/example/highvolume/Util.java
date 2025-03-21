package com.taos.example.highvolume;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.kafka.clients.admin.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Util {
    private final static Logger logger = LoggerFactory.getLogger(Util.class);

    public static String getTableNamePrefix() {
        return "d_";
    }

    public static Connection getConnection() throws SQLException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        if (jdbcURL == null || jdbcURL == "") {
            jdbcURL = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
        }
        return DriverManager.getConnection(jdbcURL);
    }

    public static Connection getConnection(int batchSize, int cacheSize, int writeThreadNum) throws SQLException {
        String jdbcURL = System.getenv("TDENGINE_JDBC_URL");
        if (jdbcURL == null || jdbcURL == "") {
            jdbcURL = "jdbc:TAOS-WS://localhost:6041/?user=root&password=taosdata";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, String.valueOf(batchSize));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, String.valueOf(cacheSize));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, String.valueOf(writeThreadNum));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        return DriverManager.getConnection(jdbcURL, properties);
    }

    public static String getKafkaBootstrapServers() {
        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrapServers == null || kafkaBootstrapServers == "") {
            kafkaBootstrapServers = "localhost:9092";
        }

        return kafkaBootstrapServers;
    }

    public static String getKafkaTopic() {
        return "test-meters-topic";
    }

    public static void createKafkaTopic() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {
            String topicName = getKafkaTopic();
            int numPartitions = getPartitionCount();
            short replicationFactor = 1;

            ListTopicsResult topics = adminClient.listTopics();
            Set<String> existingTopics = topics.names().get();

            if (!existingTopics.contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
                CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.all().get(); // 等待创建完成
                logger.info("Topic " + topicName + " created successfully.");
            }

        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to delete/create topic: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static int getPartitionCount() {
        return 5;
    }
}
