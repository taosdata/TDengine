package com.taos.example.highvolume;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

class ConsumerTask implements Runnable, Stoppable {
    private final static Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
    private final int taskId;
    private final int writeThreadCount;
    private final int batchSizeByRow;
    private final int cacheSizeByRow;
    private final String dbName;
    private volatile boolean active = true;

    public ConsumerTask(int taskId,
                        int writeThradCount,
                        int batchSizeByRow,
                        int cacheSizeByRow,
                        String dbName) {
        this.taskId = taskId;
        this.writeThreadCount = writeThradCount;
        this.batchSizeByRow = batchSizeByRow;
        this.cacheSizeByRow = cacheSizeByRow;
        this.dbName = dbName;
    }

    @Override
    public void run() {

        // 配置 Kafka 消费者的属性
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Util.getKafkaBootstrapServers());

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(batchSizeByRow));
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "3000");

        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(2 * 1024 * 1024));

        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "15000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "60000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        List<String> topics = Collections.singletonList(Util.getKafkaTopic());

        try {
            consumer.subscribe(topics);
        } catch (Exception e) {
            logger.error("Consumer Task {} Error", taskId, e);
            return;
        }

        try (Connection connection = Util.getConnection(batchSizeByRow, cacheSizeByRow, writeThreadCount);
             PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + dbName +".meters (tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)")) {
            long i = 0L;
            long lastTimePolled = System.currentTimeMillis();
            while (active) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    i++;
                    Meters meters = Meters.fromString(record.value());
                    pstmt.setString(1, meters.getTableName());
                    pstmt.setTimestamp(2, meters.getTs());
                    pstmt.setFloat(3, meters.getCurrent());
                    pstmt.setInt(4, meters.getVoltage());
                    pstmt.setFloat(5, meters.getPhase());
                    pstmt.addBatch();

                    if (i % batchSizeByRow == 0) {
                        pstmt.executeBatch();
                    }
                    if (i % (10L * batchSizeByRow) == 0){
                        //pstmt.executeUpdate();
                        consumer.commitAsync();
                    }
                }

                if (!records.isEmpty()){
                    lastTimePolled = System.currentTimeMillis();
                } else {
                    if (System.currentTimeMillis() - lastTimePolled > 1000 * 60) {
                        lastTimePolled = System.currentTimeMillis();
                        logger.error("Consumer Task {} has been idle for 10 seconds, stopping", taskId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Consumer Task {} Error", taskId, e);
        } finally {
            // 关闭消费者
            consumer.close();
        }
    }

    public void stop() {
        logger.info("stop");
        this.active = false;
    }
}