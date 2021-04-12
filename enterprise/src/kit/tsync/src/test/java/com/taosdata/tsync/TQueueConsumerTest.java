package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.domain.ConsumerConfig;
import com.taosdata.tsync.domain.ConsumerRecord;
import com.taosdata.tsync.domain.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TQueueConsumerTest {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, "master");
        props.setProperty(ConsumerConfig.PORT_CONFIG, "6041");
        props.setProperty(ConsumerConfig.USER_CONFIG, "root");
        props.setProperty(ConsumerConfig.PASSWORD_CONFIG, "taosdata");
        props.setProperty(ConsumerConfig.CHARSET_CONFIG, "UTF-8");
        props.setProperty(ConsumerConfig.LOCALE_CONFIG, "en_US.UTF-8");
        props.setProperty(ConsumerConfig.TIMEZONE_CONFIG, "UTC-8");

        TQueueConsumer consumer = new TQueueConsumer(props);
        consumer.subscribe(Collections.singletonList("test"));

        while (true) {
            ConsumerRecords records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();
                String value = record.value();
                System.out.printf(
                        "topic: %s, partition: %d, offset: %d, value = %s%n",
                        topic, partition, offset, value
                );
            }
        }
    }
}
