package com.taosdata.tsync.tqueue;

import com.taosdata.tsync.entity.ProducerConfig;
import com.taosdata.tsync.entity.ProducerRecord;
import com.taosdata.tsync.exceptions.TQueueException;
import com.taosdata.tsync.utils.SqlUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TQueueProducerTest {

    private String host = "192.168.17.156";
    private String topic = "tq_test";

    @Before
    public void before() {
        SqlUtil.execute(host, topic, "root", "tqueue", "drop topic if exists tq_test");
        SqlUtil.execute(host, topic, "root", "tqueue", "create topic if not exists tq_test partitions 10");
    }

    @Test
    public void produceToOnePartition() throws SQLException, InterruptedException, TQueueException, ExecutionException {
        // given
        final long recordSize = 10000;

        // when
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer<String> producer = new TQueueProducer<>(props);
        for (int i = 0; i < recordSize; i++) {
            ProducerRecord<String> record = new ProducerRecord<>(topic, 1, "hello~~~");
            producer.send(record);
        }

        // then
        ResultSet rs = SqlUtil.executeQuery(host, topic, "root", "tqueue", "select count(*) from tq_test.ps");
        rs.next();
        int actual = rs.getInt("count(*)");
        Assert.assertEquals(recordSize, actual);
    }

    @Test
    public void produceToMultiPartitions() throws SQLException {
        // given
        final long recordSize = 1000;
        final int[] partitions = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // when
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, "192.168.17.156");
        TQueueProducer<String> producer = new TQueueProducer<>(props);

        IntStream.of(partitions).forEach(partitionIndex -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord<>("tq_test", partitionIndex, "hello~~~");
                    producer.send(record);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // then
        ResultSet rs = SqlUtil.executeQuery(host, topic, "root", "tqueue", "select count(*) from tq_test.ps");
        rs.next();
        int actual = rs.getInt("count(*)");
        Assert.assertEquals(recordSize * partitions.length, actual);
    }

    @Test
    public void produceToMultiPartitionsMultiThreads() throws SQLException {
        // given
        final long recordSize = 1000;
        final int[] partitions = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // when
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer<String> producer = new TQueueProducer<>(props);
        List<Thread> threads = IntStream.of(partitions).mapToObj(pIndex -> new Thread(() -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord<>(topic, pIndex, "hello~~~");
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        })).collect(Collectors.toList());
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();

        // then
        ResultSet rs = SqlUtil.executeQuery(host, topic, "root", "tqueue", "select count(*) from tq_test.ps");
        rs.next();
        int actual = rs.getInt("count(*)");
        Assert.assertEquals(recordSize * partitions.length, actual);
    }

    @Test
    public void multiThreadSendToOnePartition() throws SQLException {
        // given
        final long recordSize = 1000;
        final int threadSize = 20;

        // when
        Properties props = new Properties();
        props.setProperty(ProducerConfig.HOST_CONFIG, host);
        TQueueProducer<String> producer = new TQueueProducer<>(props);
        List<Thread> threads = IntStream.range(0, threadSize).mapToObj(threadIndex -> new Thread(() -> {
            try {
                for (int i = 0; i < recordSize; i++) {
                    ProducerRecord<String> record = new ProducerRecord<>("tq_test", 1, "hello~~~");
                    producer.send(record, (metadata, e) -> {
                        if (e != null)
                            e.printStackTrace();
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "thread-" + threadIndex)).collect(Collectors.toList());
        threads.forEach(Thread::start);
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();

        // then
        ResultSet rs = SqlUtil.executeQuery(host, topic, "root", "tqueue", "select count(*) from tq_test.ps");
        rs.next();
        int actual = rs.getInt("count(*)");
        Assert.assertEquals(recordSize * threadSize, actual);
    }

}