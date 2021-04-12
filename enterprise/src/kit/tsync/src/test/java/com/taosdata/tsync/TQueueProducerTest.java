package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.domain.Person;
import com.taosdata.tsync.domain.ProducerConfig;
import com.taosdata.tsync.domain.ProducerRecord;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class TQueueProducerTest {

    private static final String TOPIC = "tq_test";
    private static final Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, "master");
        props.setProperty(ProducerConfig.PORT_CONFIG, "6041");
        props.setProperty(ProducerConfig.USER_CONFIG, "root");
        props.setProperty(ProducerConfig.PASSWORD_CONFIG, "taosdata");
        props.setProperty(ProducerConfig.CHARSET_CONFIG, "UTF-8");
        props.setProperty(ProducerConfig.LOCALE_CONFIG, "en_US.UTF-8");
        props.setProperty(ProducerConfig.TIMEZONE_CONFIG, "UTC-8");

        TQueueProducer producer = new TQueueProducer(props);
        IntStream.range(1, 11).forEach(partition -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    ProducerRecord record = new ProducerRecord(
                            TOPIC,
                            partition,
                            new Person("name_" + i, random.nextInt(), random.nextBoolean()).toString()
                    );

//                    RecordMetadata metadata = producer.send(record).get();
//                    System.out.println(metadata);
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                        System.out.println(metadata);
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}