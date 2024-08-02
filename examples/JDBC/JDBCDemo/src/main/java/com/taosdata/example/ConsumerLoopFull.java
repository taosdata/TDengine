package com.taosdata.example;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// ANCHOR: consumer_demo
public class ConsumerLoopFull {
    static private Connection connection;
    static private Statement statement;

    public static TaosConsumer<ResultBean> getConsumer() throws SQLException {
// ANCHOR: create_consumer
        Properties config = new Properties();
        config.setProperty("td.connect.type", "jni");
        config.setProperty("bootstrap.servers", "localhost:6030");
        config.setProperty("auto.offset.reset", "latest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group1");
        config.setProperty("client.id", "1");
        config.setProperty("td.connect.user", "root");
        config.setProperty("td.connect.pass", "taosdata");
        config.setProperty("value.deserializer", "com.taosdata.example.ConsumerLoopFull$ResultDeserializer");
        config.setProperty("value.deserializer.encoding", "UTF-8");

        try {
            return new TaosConsumer<>(config);
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to create jni consumer, host : " + config.getProperty("bootstrap.servers") + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create consumer", ex);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("Failed to create consumer", e);
        }
// ANCHOR_END: create_consumer
    }

    public static void pollDataExample() throws SQLException {
        try (TaosConsumer<ResultBean> consumer = getConsumer()) {
            // subscribe to the topics
            List<String> topics = Collections.singletonList("topic_meters");

            consumer.subscribe(topics);
            System.out.println("subscribe topics successfully");
            for (int i = 0; i < 50; i++) {
                // poll data
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process the data here
                    System.out.println("data: " + JSON.toJSONString(bean));
                }
            }
            // unsubscribe the topics
            consumer.unsubscribe();
            System.out.println("unsubscribed topics successfully");
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to poll data from topic_meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to poll data from topic_meters", ex);
        }
    }

    public static void pollExample() throws SQLException {
// ANCHOR: poll_data_code_piece
        try (TaosConsumer<ResultBean> consumer = getConsumer()) {
            List<String> topics = Collections.singletonList("topic_meters");

            // subscribe to the topics
            consumer.subscribe(topics);
            System.out.println("subscribe topics successfully");
            for (int i = 0; i < 50; i++) {
                // poll data
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process the data here
                    System.out.println("data: " + JSON.toJSONString(bean));
                }
            }

        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to poll data; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to poll data", ex);
        }
// ANCHOR_END: poll_data_code_piece
    }

    public static void seekExample() throws SQLException {
// ANCHOR: consumer_seek
        try (TaosConsumer<ResultBean> consumer = getConsumer()) {
            List<String> topics = Collections.singletonList("topic_meters");

            // subscribe to the topics
            consumer.subscribe(topics);
            System.out.println("subscribe topics successfully");
            ConsumerRecords<ResultBean> records = ConsumerRecords.emptyRecord();
            // make sure we have got some data
            while (records.isEmpty()) {
                records = consumer.poll(Duration.ofMillis(100));
            }

            for (ConsumerRecord<ResultBean> record : records) {
                System.out.println("first data polled: " + JSON.toJSONString(record.value()));
                Set<TopicPartition> assignment = consumer.assignment();
                // seek to the beginning of the all partitions
                consumer.seekToBeginning(assignment);
                System.out.println("assignment seek to beginning successfully");
                break;
            }

            // poll data agagin
            records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<ResultBean> record : records) {
                // process the data here
                System.out.println("second data polled: " + JSON.toJSONString(record.value()));
                break;
            }


        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("seek example failed; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("seek example failed", ex);
        }
// ANCHOR_END: consumer_seek
    }


    public static void commitExample() throws SQLException {
// ANCHOR: commit_code_piece
        try (TaosConsumer<ResultBean> consumer = getConsumer()) {
            List<String> topics = Collections.singletonList("topic_meters");

            consumer.subscribe(topics);
            for (int i = 0; i < 50; i++) {
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process your data here
                    System.out.println("data: " + JSON.toJSONString(bean));
                }
                if (!records.isEmpty()) {
                    // after processing the data, commit the offset manually
                    consumer.commitSync();
                }
            }
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to execute consumer functions. ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to execute consumer functions", ex);
        }
// ANCHOR_END: commit_code_piece
    }

    public static void unsubscribeExample() throws SQLException {
        TaosConsumer<ResultBean> consumer = getConsumer();
        List<String> topics = Collections.singletonList("topic_meters");
        consumer.subscribe(topics);
// ANCHOR: unsubscribe_data_code_piece
        try {
            consumer.unsubscribe();
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to unsubscribe consumer. ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to unsubscribe consumer", ex);
        } finally {
            consumer.close();
        }
// ANCHOR_END: unsubscribe_data_code_piece
    }

    public static class ResultDeserializer extends ReferenceDeserializer<ResultBean> {

    }

    // use this class to define the data structure of the result record
    public static class ResultBean {
        private Timestamp ts;
        private double current;
        private int voltage;
        private double phase;
        private int groupid;
        private String location;

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public double getCurrent() {
            return current;
        }

        public void setCurrent(double current) {
            this.current = current;
        }

        public int getVoltage() {
            return voltage;
        }

        public void setVoltage(int voltage) {
            this.voltage = voltage;
        }

        public double getPhase() {
            return phase;
        }

        public void setPhase(double phase) {
            this.phase = phase;
        }

        public int getGroupid() {
            return groupid;
        }

        public void setGroupid(int groupid) {
            this.groupid = groupid;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }

    public static void prepareData() throws SQLException {
        StringBuilder insertQuery = new StringBuilder();
        insertQuery.append("INSERT INTO " +
                "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                "VALUES ");
        for (int i = 0; i < 10000; i++) {
            insertQuery.append("(NOW + ").append(i).append("a, 10.30000, 219, 0.31000) ");
        }
        try {
            int affectedRows = statement.executeUpdate(insertQuery.toString());
            assert affectedRows == 10000;
        } catch (SQLException ex) {
            System.out.println("Failed to insert data to power.meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to insert data to power.meters", ex);
        }
    }

    public static void prepareMeta() throws SQLException {
        try {
            statement.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
            statement.executeUpdate("USE power");
            statement.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
            statement.executeUpdate("CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
        } catch (SQLException ex) {
            System.out.println("Failed to create db and table, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create db and table", ex);
        }
    }

    public static void initConnection() throws SQLException {
        String url = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException ex) {
            System.out.println("Failed to create connection, url:" + url + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create connection", ex);
        }
        try {
            statement = connection.createStatement();
        } catch (SQLException ex) {
            System.out.println("Failed to create statement, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create statement", ex);
        }
        System.out.println("Connection created successfully.");
    }

    public static void closeConnection() throws SQLException {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException ex) {
            System.out.println("Failed to close statement, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to close statement", ex);
        }

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            System.out.println("Failed to close connection, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to close connection", ex);
        }
        System.out.println("Connection closed Successfully.");
    }


    public static void main(String[] args) throws SQLException {
        initConnection();
        prepareMeta();

        // create a single thread executor
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // submit a task
        executor.submit(() -> {
            try {
                // please use one example at a time
                pollDataExample();
//                seekExample();
//                pollExample();
//                commitExample();
                unsubscribeExample();
            } catch (SQLException ex) {
                System.out.println("Failed to poll data from topic_meters, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            }
            System.out.println("pollDataExample executed successfully");
        });

        prepareData();
        closeConnection();

        System.out.println("Data prepared successfully");

        // close the executor, which will make the executor reject new tasks
        executor.shutdown();

        try {
            // wait for the executor to terminate
            boolean result = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            assert result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Wait executor termination failed.");
        }

        System.out.println("program end.");
    }
}
// ANCHOR_END: consumer_demo
