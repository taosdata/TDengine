package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.ConsumerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConsumeToFileRunnableTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToFileRunnableTask.class);

    private List<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private int pollingInterval;
    private File directory;

    @Override
    public void run() {
        logger.info("consume topic:" + topic + ", partitions: " + Arrays.toString(partitionsToWrite.toArray()) + " to file: " + directory.getName());

        while (!Thread.currentThread().isInterrupted()) {
            try {
                doWriteToFile();
            } catch (InterruptedException e) {
                logger.warn(Thread.currentThread().getName() + " is interrupted.");
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void doWriteToFile() throws InterruptedException, TQueueException {
        for (int partitionId : partitionsToWrite) {
            consumer.assign(topic, partitionId);
            List<ConsumerRecord> records = consumer.poll();
            for (ConsumerRecord record : records) {
                final String topic = record.topic();
                final int partition = record.partition();
                final long offset = record.offset();
                String message = new String(record.value(), StandardCharsets.UTF_8);
                logger.trace(String.format("topic: %s, partition: %d, offset: %d, value = %s", topic, partition, offset, message));
            }
        }
        TimeUnit.MILLISECONDS.sleep(pollingInterval);
    }

    // setters
    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(TQueueConsumer consumer) {
        this.consumer = consumer;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }
}
