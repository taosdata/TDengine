package com.taosdata.tsync;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

public class TQueueProducerTest {

    private static final String TOPIC = "tq_test";
    private static final Random random = new Random(System.currentTimeMillis());

    public static void main(String[] args) throws SQLException {

        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "master");
        props.setProperty(TSDBDriver.PROPERTY_KEY_PORT, "6041");
        props.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
        props.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
        props.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        props.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

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