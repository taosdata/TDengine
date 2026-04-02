package com.taosdata.tsync.tqueue;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.entity.ConsumerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OnePartitionTwoConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(OnePartitionTwoConsumerTest.class);
    private static final String host = "192.168.1.66";
    private static final String topic = "tq_topic_1";

    private class ConsumerTask implements Runnable {
        private final TQueueConsumer consumer;
        private final String topic;
        private final int partitionId;
        private final AtomicLong count = new AtomicLong(0);

        private ConsumerTask(TQueueConsumer consumer, String topic, int partitionId) {
            this.consumer = consumer;
            this.topic = topic;
            this.partitionId = partitionId;
        }

        private Set<Long> offsetSet = new HashSet<>();

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    consumer.assign(topic, partitionId);

                    long messageCount = 0;
                    List<ConsumerRecord> records = consumer.poll();
                    for (ConsumerRecord record : records) {
                        long offset = record.offset();
                        if (offsetSet.contains(offset)) {
                            logger.error("offset: " + offset + " already exists");
                        }
                        count.incrementAndGet();
                        messageCount++;
                    }

                    logger.debug("total: " + count.get() + ", count: " + messageCount);
                    if (messageCount == 0) {
                        TimeUnit.MILLISECONDS.sleep(1000);
                    }
                }
            } catch (TQueueException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void MultiThreadConsumeOnePartition() throws InterruptedException {
        // given
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
        props.setProperty(ConsumerConfig.TIMEZONE_CONFIG, "UTC-8");
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, "TIMESTAMP");
        // when
        List<Thread> threads = IntStream.range(1, 11).mapToObj(i -> new Thread(new ConsumerTask(new TQueueConsumer(props), topic, i))).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void TwoThreadConsumeOnePartition() throws InterruptedException {
        // given
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
//        props.setProperty(ConsumerConfig.TIMEZONE_CONFIG, "UTC-8");
//        props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, "TIMESTAMP");

        // when
        List<Thread> threads = IntStream.range(1, 3).mapToObj(i -> new Thread(new ConsumerTask(new TQueueConsumer(props), topic, 1))).collect(Collectors.toList());

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void OneThreadConsumeOnePartition() throws InterruptedException {
        // given
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);

        // when
        Thread thread = new Thread(new ConsumerTask(new TQueueConsumer(props), topic, 1));

        thread.start();

        thread.join();
    }
}
