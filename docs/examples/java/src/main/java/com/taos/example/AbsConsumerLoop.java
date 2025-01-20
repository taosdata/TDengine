package com.taos.example;

import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.ReferenceDeserializer;
import com.taosdata.jdbc.tmq.TaosConsumer;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

// ANCHOR: consumer_demo
public abstract class AbsConsumerLoop {
    private final TaosConsumer<ResultBean> consumer;
    private final List<String> topics;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;

    public AbsConsumerLoop() throws SQLException {

// ANCHOR: create_consumer
        Properties config = new Properties();
        config.setProperty("td.connect.type", "jni");
        config.setProperty("bootstrap.servers", "localhost:6030");
        config.setProperty("auto.offset.reset", "latest");
        config.setProperty("msg.with.table.name", "true");
        config.setProperty("enable.auto.commit", "true");
        config.setProperty("auto.commit.interval.ms", "1000");
        config.setProperty("group.id", "group1");
        config.setProperty("client.id", "client1");
        config.setProperty("value.deserializer", "com.taos.example.AbsConsumerLoop$ResultDeserializer");
        config.setProperty("value.deserializer.encoding", "UTF-8");
        try {
            this.consumer = new TaosConsumer<>(config);
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to create jni consumer with " + config.getProperty("bootstrap.servers") + " ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to create consumer", ex);
        }
// ANCHOR_END: create_consumer

        this.topics = Collections.singletonList("topic_meters");
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }

    public abstract void process(ResultBean result);

    public void pollDataCodePiece() throws SQLException {
// ANCHOR: poll_data_code_piece
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process your data here
                    process(bean);
                }
            }
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to poll data; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw new SQLException("Failed to poll data", ex);
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
// ANCHOR_END: poll_data_code_piece
    }

    public void commitCodePiece() throws SQLException {
// ANCHOR: commit_code_piece
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    // process your data here
                    process(bean);
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
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
// ANCHOR_END: commit_code_piece
    }

    public void unsubscribeCodePiece() throws SQLException {
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

    public void pollData() throws SQLException {
        try {

// ANCHOR: poll_data
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> record : records) {
                    ResultBean bean = record.value();
                    process(bean);
                }
            }
// ANCHOR_END: poll_data

            consumer.unsubscribe();
        } finally {
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown.set(true);
        shutdownLatch.await();
    }

    public static class ResultDeserializer extends ReferenceDeserializer<ResultBean> {

    }

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
}
// ANCHOR_END: consumer_demo
