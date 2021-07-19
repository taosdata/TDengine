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
import java.util.concurrent.atomic.AtomicLong;

public class ConsumeToTDengineRunnableTask implements Runnable, Countable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineRunnableTask.class);
    private static final AtomicLong count = new AtomicLong(0);

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

    @Override
    public long getCount() {
        return count.get();
    }

    private void doWriteToTDengine() throws Exception {
        for (int partitionId : partitionsToWrite) {
            consumer.assign(topic, partitionId);
            List<ConsumerRecord> records = consumer.poll();
            for (ConsumerRecord record : records) {
                String message = new String(record.value(), StandardCharsets.UTF_8);
                logger.debug("count: " + count.incrementAndGet() + ", topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset() + ", value: " + message);
                tryExecuteSQL(message);
            }
        }
        if (pollingInterval > 0)
            TimeUnit.MILLISECONDS.sleep(pollingInterval);
    }

    public void tryExecuteSQL(String sql) {
        try (Statement statement = taosdConnection.createStatement()) {
            statement.execute(sql);
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
