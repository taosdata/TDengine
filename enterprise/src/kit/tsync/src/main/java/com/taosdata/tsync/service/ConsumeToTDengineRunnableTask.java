package com.taosdata.tsync.service;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.entity.ConsumerRecord;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsumeToTDengineRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineRunnableTask.class);

    private List<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private Connection taosdConnection;
    private int pollingInterval;

    @Override
    public void run() {
        try {
            String host = taosdConnection.getClientInfo(TSDBDriver.PROPERTY_KEY_HOST);
            logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.toArray()) + " to TDengine: " + host);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        while (!Thread.currentThread().isInterrupted()) {
            try {
                doWriteToTDengine();
            } catch (SQLException e) {
                logger.error("failed to create statement");
                e.printStackTrace();
            } catch (InterruptedException e) {
                logger.warn(Thread.currentThread().getName() + " is interrupted.");
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void doWriteToTDengine() throws Exception {
        for (int partitionId : partitionsToWrite) {
            consumer.assign(topic, partitionId);
            List<ConsumerRecord> records = consumer.pollAndMark();
            for (ConsumerRecord record : records) {
                final String topic = record.topic();
                final int partition = record.partition();
                final long offset = record.offset();
                String message = new String(record.value(), StandardCharsets.UTF_8);
                logger.trace(String.format("topic: %s, partition: %d, offset: %d, value = %s", topic, partition, offset, message));
                tryExecuteSQL(message);
            }
        }
        TimeUnit.MILLISECONDS.sleep(pollingInterval);
    }

    public void tryExecuteSQL(String sql) {
        try {
            logger.trace("execute sql >>> " + sql);
            try (Statement statement = taosdConnection.createStatement()) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //setter
    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(TQueueConsumer consumer) {
        this.consumer = consumer;
    }

    public void setTaosdConnection(Connection taosdConnection) {
        this.taosdConnection = taosdConnection;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

}
