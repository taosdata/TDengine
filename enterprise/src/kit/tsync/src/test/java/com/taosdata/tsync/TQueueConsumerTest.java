package com.taosdata.tsync;

import com.taosdata.tsync.entity.consumer.ConsumerConfig;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TQueueConsumerTest {

    private static final String host = "192.168.17.156";
    private TQueueConsumer consumer;

    @Test
    public void assign() {
        for (int partitionId = 1; partitionId <= 10; partitionId++) {
            consumer.assign("tq_test", partitionId);
        }
    }

    @Test
    public void poll() {
        try {
            long count = 0;
            while (true) {
                doPoll();
                TimeUnit.MILLISECONDS.sleep(1000);
                count++;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doPoll() {
        try {
            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                String value = new String(record.value(), "UTF-8");
                System.out.printf("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
        props.setProperty(ConsumerConfig.PORT_CONFIG, "6041");
        props.setProperty(ConsumerConfig.USER_CONFIG, "root");
        props.setProperty(ConsumerConfig.PASSWORD_CONFIG, "tqueue");
        consumer = new TQueueConsumer(props);
    }

}
