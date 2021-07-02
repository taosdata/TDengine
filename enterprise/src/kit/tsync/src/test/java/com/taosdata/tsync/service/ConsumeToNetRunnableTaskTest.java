package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.factory.ConsumeToNetRunnableTaskFactory;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToNetRunnableTaskTest {

    private static final String host_tq = "192.168.17.156";
    private static final String topic = "tq_test";

    private Connection conn;

    @Test
    public void test() {
        // given
        List<Integer> partitions = IntStream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).boxed().collect(Collectors.toList());
        String host = "127.0.0.1";
        int port = 8899;

        // when
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host_tq);
        TQueueConsumer consumer = new TQueueConsumer(props);

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
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Before
    public void before() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host_tq + ":6041/?user=root&password=tqueue");
            cleanTQueueTopic();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void cleanTQueueTopic() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists topic_info");
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 10");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}