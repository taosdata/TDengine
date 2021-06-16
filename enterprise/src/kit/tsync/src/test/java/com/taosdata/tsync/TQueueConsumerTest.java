package com.taosdata.tsync;

import com.taosdata.tsync.entity.consumer.ConsumerConfig;
import com.taosdata.tsync.entity.consumer.ConsumerRecord;
import com.taosdata.tsync.entity.producer.ProducerRecord;
import com.taosdata.tsync.enums.TQueueConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class TQueueConsumerTest {

    private static final String host = "192.168.17.156";
    private static final String topic = "tq_test";
    private int recordCount = 0;

    @Test
    public void assign() {
        // given
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
        TQueueConsumer consumer = new TQueueConsumer(props);
        int[] partitions = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // when
        IntStream.of(partitions).forEach(partitionId -> {
            try {
                consumer.assign("tq_test", partitionId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // then
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 1));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 2));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 3));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 4));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 5));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 6));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 7));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 8));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 9));
        Assert.assertEquals(0, selectOffsetFromTQueue(topic, 10));
    }

    @Test
    public void poll() {
        // given
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.HOST_CONFIG, host);
        TQueueConsumer consumer = new TQueueConsumer(props);

        // when
        Thread consumerThread = createConsumerThread(consumer, topic, 1, 30 * 1000);
        consumerThread.start();
        // wait for 3 seconds
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // produce few data
        TQueueProducer producer = new TQueueProducer(props);
        final long recordSize = 10000;
        try {
            for (int i = 0; i < recordSize; i++) {
                ProducerRecord<String> record = new ProducerRecord(topic, 1, "hello~~~");
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // wait for consume thread finish
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(recordSize, recordCount);
    }

    private Thread createConsumerThread(TQueueConsumer consumer, String topic, int partition, long duration) {
        return new Thread(() -> {
            long start = System.currentTimeMillis();
            recordCount = 0;

            do {
                try {
                    consumer.assign(topic, partition);
                    List<ConsumerRecord> records = consumer.poll();
                    for (ConsumerRecord record : records) {
                        String value = new String(record.value(), "UTF-8");
                        recordCount++;
                    }
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } while (System.currentTimeMillis() < start + duration);

        }, "consumer");
    }

    private long selectOffsetFromTQueue(String topic, int partition) {
        long offset = -1;
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=tqueue");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select last_row(_offset) from " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME + "." + TQueueConstants.DEFAULT_OFFSET_TABLE_NAME + " where _topic = '" + topic + "' and _partition = " + partition);
            rs.next();
            offset = rs.getLong("last_row(_offset)");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return offset;
    }

    @Before
    public void before() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=tqueue");
            Statement stmt = conn.createStatement();
            stmt.execute("drop topic if exists " + topic);
            stmt.execute("create topic if not exists " + topic + " partitions 10");
            stmt.execute("drop database if exists " + TQueueConstants.DEFAULT_OFFSET_DATABASE_NAME);
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
