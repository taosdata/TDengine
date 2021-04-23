package com.taosdata.tsync;

import com.taosdata.tsync.domain.ConsumerConfig;
import com.taosdata.tsync.domain.ConsumerRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TQueueConsumerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, "master");
        props.setProperty(ConsumerConfig.PORT_CONFIG, "6041");
        props.setProperty(ConsumerConfig.USER_CONFIG, "root");
        props.setProperty(ConsumerConfig.PASSWORD_CONFIG, "taosdata");

        TQueueConsumer consumer = new TQueueConsumer(props);
        consumer.assign("tq_test", 1);

        long count = 0;

        while (true) {
            List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                String value = new String(record.value(), "UTF-8");
                System.out.printf("topic: %s, partition: %d, offset: %d, value = %s%n", topic, partition, offset, value);
                count++;
            }
        }


    }
}
