package com.taosdata.tsync.tqueue;

import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.entity.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TQueueConsumerTest {

    private static final String host = "192.168.17.156";
    private static final String topic = "tq_test";

    @Before
    public void before() {

    }

    @Test
    public void test() {
        // given

        // when
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
        TQueueConsumer consumer = new TQueueConsumer(props);
        int recordCount = 0;
        while (true) {
            try {
                consumer.assign(topic, 1);
                List<ConsumerRecord> records = consumer.poll();
                for (ConsumerRecord record : records) {
                    String value = new String(record.value(), StandardCharsets.UTF_8);
                    recordCount++;
                    System.out.println("recordCount: " + recordCount + ", values: " + value);
                }
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }




}
