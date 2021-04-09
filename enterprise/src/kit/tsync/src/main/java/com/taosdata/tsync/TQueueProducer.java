package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.domain.Topic;
import com.taosdata.tsync.domain.TopicPartition;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TQueueProducer {

    private Connection connection;
    private Map<String, Topic> topics = new HashMap<>();
    private Map<Integer, TopicPartition> partitions = new HashMap<>();
    private ExecutorService threadPool = Executors.newCachedThreadPool();

    public TQueueProducer(Properties properties) {
        String host = properties.getProperty(TSDBDriver.PROPERTY_KEY_HOST);
        String port = properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT);
        String user = properties.getProperty(TSDBDriver.PROPERTY_KEY_USER);
        String password = properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD);

        final String url = "jdbc:TAOS-RS://" + host + ":" + port + "/?user=" + user + "&password=" + password;
        try {
            this.connection = DriverManager.getConnection(url, properties);
            flushTopics();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Future<RecordMetadata> send(ProducerRecord record) throws Exception {
        String topic = record.getTopic();
        if (!topics.containsKey(topic)) {
            throw new Exception("topic[ " + topic + " ] not exists");
        }

        Future<RecordMetadata> task = threadPool.submit(new ProducerTask(record));
        return task;
    }

    public void send(ProducerRecord record, Callback callback) {
        String topic = record.getTopic();
        if (!topics.containsKey(topic)) {
            callback.onCompletion(null, new Exception("topic[ " + topic + " ] not exists"));
            return;
        }

        Future<RecordMetadata> task = threadPool.submit(new ProducerTask(record));
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = task.get();
            callback.onCompletion(recordMetadata, null);
        } catch (InterruptedException | ExecutionException e) {
            callback.onCompletion(null, e);
        }

    }

    private class ProducerTask implements Callable {
        private final ProducerRecord record;
        private volatile AtomicLong offset = new AtomicLong();

        private ProducerTask(ProducerRecord record) {
            this.record = record;
        }

        @Override
        public RecordMetadata call() throws Exception {
            final String topic = record.getTopic();
            final int partition = record.getPartition();
            long offset = this.offset.getAndIncrement();
            final long ts = System.currentTimeMillis();
            final byte[] messages = record.getMessage().getBytes();
            final long serializedValueSize = messages.length;

            final String update = "insert into " + topic + ".p" + partition + "(off, ts, content) values(?, ?, ?)";
            try (PreparedStatement pstmt = connection.prepareStatement(update)) {
                pstmt.setLong(1, offset);
                pstmt.setTimestamp(2, new Timestamp(ts));
                pstmt.setBytes(3, messages);
                pstmt.executeUpdate();
            }

            try (Statement stmt = connection.createStatement()) {
                final String query = "select last_row(off) from " + topic + ".p" + partition;
                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                offset = rs.getTimestamp(1).getTime();
            }

            return new RecordMetadata(record.getTopic(), record.getPartition(), offset, ts, serializedValueSize);
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
