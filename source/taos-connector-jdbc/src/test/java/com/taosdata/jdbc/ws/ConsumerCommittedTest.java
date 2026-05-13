package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerCommittedTest {

        static final String HOST = TestEnvUtil.getHost();
        private static final String DB_NAME = TestUtils.camelToSnake(ConsumerCommittedTest.class);
    private static final String SUPER_TABLE = "st";
    private static Connection connection;
    private static Statement statement;
    private static ScheduledExecutorService scheduledExecutorService;
    private static final String TOPIC = "topic_" + DB_NAME;

    @Test
    public void testJNI() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_TYPE, "jni");
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getJniPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.GROUP_ID, "g_jni");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            // subscribe topic
            consumer.subscribe(Collections.singletonList(TOPIC));
            // poll data
            ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                records = consumer.poll(Duration.ofMillis(100));
            }
            Assert.assertFalse(records.isEmpty());
            // sync commit
            consumer.commitSync();
            consumer.poll(Duration.ofMillis(100));
            // subscription
            Set<String> subscription = consumer.subscription();
            Assert.assertTrue(subscription.contains(TOPIC));
            // position
            Map<TopicPartition, Long> position = consumer.position(TOPIC);
            // assignment
            Set<TopicPartition> assignment = consumer.assignment();
            Assert.assertTrue(assignment.containsAll(position.keySet()));
            // seek to beginning
            consumer.seekToBeginning(position.keySet());
            // seek to end
            consumer.seekToEnd(position.keySet());
            consumer.poll(Duration.ofMillis(100));
            consumer.commitSync();
            // committed
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(position.keySet());
            committed.keySet().forEach(topicPartition -> {
                System.out.println(topicPartition + " : " + committed.get(topicPartition));
            });
            Assert.assertFalse(committed.isEmpty());
            // sync commit with offset
            consumer.commitSync(committed);
            // unsubscribe
            consumer.unsubscribe();
        }
    }

    @Test
    public void testWS() throws Exception {
        TimeUnit.MILLISECONDS.sleep(1000);
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");
        properties.setProperty(TMQConstants.GROUP_ID, "g_ws");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            // subscribe topic
            consumer.subscribe(Collections.singletonList(TOPIC));
            // poll data
            ConsumerRecords<ResultBean> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                records = consumer.poll(Duration.ofMillis(100));
            }
            Assert.assertFalse(records.isEmpty());
            // sync commit
            consumer.commitSync();
            records = consumer.poll(Duration.ofMillis(100));
            // subscription
            Set<String> subscription = consumer.subscription();
            Assert.assertTrue(subscription.contains(TOPIC));
            // position
            Map<TopicPartition, Long> position = consumer.position(TOPIC);
            // assignment
            Set<TopicPartition> assignment = consumer.assignment();
            Assert.assertTrue(assignment.containsAll(position.keySet()));
            // seek to beginning
            consumer.seekToBeginning(position.keySet());
            // seek to end
            consumer.seekToEnd(position.keySet());
            consumer.poll(Duration.ofMillis(100));
            consumer.commitSync();
            // committed
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(position.keySet());
            committed.keySet().forEach(topicPartition -> {
                System.out.println(topicPartition + " : " + committed.get(topicPartition));
            });
            Assert.assertFalse(committed.isEmpty());
            // sync commit with offset
            consumer.commitSync(committed);
            // unsubscribe
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists " + TOPIC);
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
        statement.execute("create table if not exists ct1 using " + SUPER_TABLE + " tags(2000)");
        statement.execute("create table if not exists ct2 using " + SUPER_TABLE + " tags(3000)");

        AtomicInteger a = new AtomicInteger(1);
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct1 values(now, " + a.getAndIncrement() + ", 0.2, 'a', '一', true)" +
                                "(now+1s," + a.getAndIncrement() + ", 0.4, 'b', '二', false)" +
                                "(now+2s," + a.getAndIncrement() + ", 0.6, 'c', '三', false)");
                statement.executeUpdate(
                        "insert into ct0 values(now, " + a.getAndIncrement() + ", 0.2, 'a', '一', true)" +
                                "(now+1s," + a.getAndIncrement() + ", 0.4, 'b', '二', false)" +
                                "(now+2s," + a.getAndIncrement() + ", 0.6, 'c', '三', false)");
                statement.executeUpdate(
                        "insert into ct2 values(now, " + a.getAndIncrement() + ", 0.2, 'a', '一', true)" +
                                "(now+1s," + a.getAndIncrement() + ", 0.4, 'b', '二', false)" +
                                "(now+2s," + a.getAndIncrement() + ", 0.6, 'c', '三', false)");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        statement.executeUpdate("create topic if not exists " + TOPIC + " as select ts, c1, c2, c3, c4, c5, t1 from ct1");
    }

    @AfterClass
    public static void after() throws SQLException {
        if (null != scheduledExecutorService) {
            scheduledExecutorService.shutdown();
        }
        if (connection != null) {
            if (statement != null) {
                TestUtils.waitTransactionDone(connection);
                statement.executeUpdate("drop topic if exists " + TOPIC);
                statement.executeUpdate("drop database if exists " + DB_NAME);
                statement.close();
            }
            connection.close();
        }
    }
}

