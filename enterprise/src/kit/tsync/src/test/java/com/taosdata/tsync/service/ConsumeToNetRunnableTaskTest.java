package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.factory.ConsumeToNetRunnableTaskFactory;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToNetRunnableTaskTest {

    private static final String host_tq = "192.168.17.156";
    private static final String topic = "tq_test";

    private static Connection conn;
    private Properties props;

    @Test
    public void test() {
        // given
        TQueueConsumer consumer = new TQueueConsumer(props);
        List<Integer> partitions = IntStream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).boxed().collect(Collectors.toList());
        String host = "127.0.0.1";
        int port = 8899;

        // when
        ConsumeToNetRunnableTask runnable = new ConsumeToNetRunnableTaskFactory()
                .setConsumer(consumer)
                .setTopic(topic)
                .setPartitionsToWrite(partitions)
                .setPollingInterval(1000)
                .setHost(host)
                .setPort(port)
                .build();
        Thread thread = new Thread(runnable);
        thread.start();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        insertFewDataIntoTQueue();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        runnable.shutdown();

        // assert

    }

    private void insertFewDataIntoTQueue() {
        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 10; i++) {
                IntStream.range(1, 11).boxed().forEach(partitionId -> {
                    try {
                        stmt.execute("insert into " + topic + ".p" + partitionId + " values(1, now, 'Hello~~~')");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host_tq);
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            cleanTQueueTopic();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void cleanTQueueTopic() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 10");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}