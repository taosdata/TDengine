package com.taosdata.tsync.service;

import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class WriteToTDengineRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WriteToTDengineRunnableTask.class);

    private Collection<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private Connection taosdConnection;
    private Statement statement;
    private int pollingInterval;
    private SchemaMissingStrategy schemaMissing;
    private SchemaConfiguration schemaConfiguration;

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.stream().toArray()));

        try {
            doSchemaMissingStrategy();
            statement = taosdConnection.createStatement();
            doWriteToTDengine();
        } catch (SQLException e) {
            logger.error("failed to create statement");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doSchemaMissingStrategy() {
        try (Statement stmt = taosdConnection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show databases");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void doWriteToTDengine() throws Exception {


        while (true) {
            for (int partitionId : partitionsToWrite) {
                consumer.assign(topic, partitionId);
                List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord record : records) {
                    String topic = record.topic();
                    int partition = record.partition();
                    long offset = record.offset();
                    String message = new String(record.value(), "UTF-8");
                    logger.trace(String.format("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, message));
                    tryExecuteSQL(message);
                }
            }
            TimeUnit.SECONDS.sleep(pollingInterval);
        }
    }

    public void tryExecuteSQL(String sql) {
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //setter
    public void setPartitionsToWrite(Collection<Integer> partitionsToWrite) {
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

    public void setSchemaMissing(SchemaMissingStrategy schemaMissing) {
        this.schemaMissing = schemaMissing;
    }

    public void setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        this.schemaConfiguration = schemaConfiguration;
    }
}
