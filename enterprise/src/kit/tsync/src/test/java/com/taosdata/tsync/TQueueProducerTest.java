package com.taosdata.tsync;

import com.taosdata.tsync.entity.producer.ProducerConfig;
import com.taosdata.tsync.entity.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TQueueProducerTest {

    private static final String host = "192.168.17.156";
    private static final String topic = "tq_test";
    private static final int partitionSize = 10;

    @Test
    public void produceToOnePartition() {
        // given
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer producer = new TQueueProducer(props);
        final long recordSize = 1000;

        // when
        try {
            for (int i = 0; i < recordSize; i++) {
                ProducerRecord<String> record = new ProducerRecord(topic, 1, "hello~~~");
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(recordSize, count());
    }

    @Test
    public void produceToMultiPartitions() {
        // given
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer producer = new TQueueProducer(props);
        final long recordSize = 1000;
        final int[] partitions = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // when
        IntStream.of(partitions).forEach(partitionIndex -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord(topic, partitionIndex, "hello~~~");
                    producer.send(record);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // then
        Assert.assertEquals(recordSize * partitions.length, count());
    }

    @Test
    public void produceToMultiPartitionsMultiThreads() {
        // given
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer producer = new TQueueProducer(props);
        final long recordSize = 1000;
        final int[] partitions = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // when
        List<Thread> threads = IntStream.of(partitions).mapToObj(pIndex -> new Thread(() -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord(topic, pIndex, "hello~~~");
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        })).collect(Collectors.toList());
        // start threads
        threads.forEach(Thread::start);
        // wait threads
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // close producer
        producer.close();

        // then
        Assert.assertEquals(recordSize * partitions.length, count());
    }

    @Test
    public void multiThreadSendToOnePartition() {
        // given
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer producer = new TQueueProducer(props);
        final long recordSize = 1000;
        final int threadSize = 20;

        // when
        List<Thread> threads = IntStream.range(0, threadSize).mapToObj(threadIndex -> new Thread(() -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord(topic, 1, "hello~~~");
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "thread-" + threadIndex)).collect(Collectors.toList());
        // start threads
        threads.forEach(Thread::start);
        // wait threads
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // close producer
        producer.close();

        // then
        Assert.assertEquals(recordSize * threadSize, count());
    }


    @Before
    public void before() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=tqueue");
            Statement stmt = conn.createStatement();
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions " + partitionSize);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private long count() {
        long count = 0;
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=tqueue");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) from " + topic + ".ps");
            rs.next();
            count = rs.getLong("count(*)");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }


}