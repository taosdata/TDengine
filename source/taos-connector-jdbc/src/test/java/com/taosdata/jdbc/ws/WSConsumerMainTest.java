package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBError;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WSConsumerMainTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerMainTest.class);
    private static final String SUPER_TABLE = "st";
    private static Connection connection;
    private static Statement statement;
    private static final String APP_NAME = "jdbc_tmq_appName";
    private static final String APP_IP = "192.168.1.2";
    private static final String[] topics = {"topic_ws_map" + DB_NAME, "topic_ws_bean" + DB_NAME};

    public void checkAppInfo() throws SQLException, InterruptedException {

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("show connections")) {
                while (resultSet.next()) {

                    String name = resultSet.getString("user_app");
                    String ip = resultSet.getString("user_ip");
                    String connectionInfo = resultSet.getString("connector_info");

                    System.out.println("name: " + name + ", ip: " + ip + ", connectionInfo: " + connectionInfo);
                    if (APP_NAME.equals(name)
                            && APP_IP.equals(ip)
                            && connectionInfo.split("-").length == 4
                            && connectionInfo.length() >= "jdbc-ws-v3.0.0-ncid000".length()) {
                        return;
                    }
                }
            }
        }
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNKNOWN, "App info not found in show connections");
    }
    @Test
    public void testWSMap() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct0 values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
        TimeUnit.MILLISECONDS.sleep(11);

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, APP_NAME);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, APP_IP);

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            // check connection info
            checkAppInfo();

            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Map<String, Object> map = r.value();
                    Assert.assertEquals(7, map.size());
                    Assert.assertTrue(map.get("ts") instanceof Timestamp);
                }
            }
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testWSBeanObject() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        List<String> strings = Arrays.asList("a", "b", "c");
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate(
                        "insert into ct1 values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
        TimeUnit.MILLISECONDS.sleep(11);
        String topic = topics[1];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct1");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, "192.168.1.1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, "APP_NAME");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Asia/Shanghai");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<ResultBean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> r : consumerRecords) {
                    ResultBean bean = r.value();
                    Assert.assertTrue(strings.contains(bean.getC3()));
                }
            }
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @BeforeClass
    public static void before() throws SQLException {
        TestUtils.runInMain();
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        for (String topic : topics) {
            statement.executeUpdate("drop topic if exists " + topic);
        }
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");

        statement.execute("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
        statement.execute("create table if not exists ct1 using " + SUPER_TABLE + " tags(2000)");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
                        statement.executeUpdate("drop topic if exists " + topic);
                    }
                    statement.executeUpdate("drop database if exists " + DB_NAME);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}

