package com.taosdata.tsync.service;

import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class WriteToTDengineRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WriteToTDengineRunnableTask.class);

    private Collection<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private Connection taosdConnection;

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.stream().toArray()));
        try {
            doWriteToTDengine();
        } catch (Exception e) {
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
                    String value = new String(record.value(), "UTF-8");
                    System.out.printf("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, value);
                }
            }
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
}
