package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class WSLoadBalanceTmqTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(WSLoadBalanceTmqTest.class);
    private static final String SUPER_TABLE = "st";
    private static Connection connection;
    private static Statement statement;
    private static TaosAdapterMock mockA;

    @Test
    public void testTmqFailOver() throws Exception {
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
        String topic = "topic_ws_fail_over";
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct1");

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, HOST + mockA.getListenPort() + "," + HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<ResultBean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> r : consumerRecords) {
                    ResultBean bean = r.value();
                    Assert.assertTrue(strings.contains(bean.getC3()));
                }

                if (i == 1) {
                    mockA.stop();
                }
            }
            TimeUnit.MILLISECONDS.sleep(10);
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testTmqRebalance() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        List<String> strings = Arrays.asList("a", "b", "c");

        String topic = "topic_ws_rebalance";
        mockA.stop();
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as select ts, c1, c2, c3, c4, c5, t1 from ct1");

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, HOST + ":" + mockA.getListenPort() + "," + HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "1");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        TaosConsumer<ResultBean> consumer1 = new TaosConsumer<>(properties);
        consumer1.subscribe(Collections.singletonList(topic));
        TaosConsumer<ResultBean> consumer2 = new TaosConsumer<>(properties);
        consumer2.subscribe(Collections.singletonList(topic));

        mockA.start();

        for (int i = 0; i < 10; i++) {
            ConsumerRecords<ResultBean> consumerRecords = consumer1.poll(Duration.ofMillis(100));
            for (ConsumerRecord<ResultBean> r : consumerRecords) {
                ResultBean bean = r.value();
                Assert.assertTrue(strings.contains(bean.getC3()));
            }

            ConsumerRecords<ResultBean> consumerRecords2 = consumer2.poll(Duration.ofMillis(100));
            for (ConsumerRecord<ResultBean> r : consumerRecords2) {
                ResultBean bean = r.value();
                Assert.assertTrue(strings.contains(bean.getC3()));
            }
            TimeUnit.MILLISECONDS.sleep(10);
        }

        Assert.assertEquals(0, RebalanceManager.getInstance().getEndpointInfo(new Endpoint(HOST, mockA.getListenPort(), false)).getConnectCount());
        Assert.assertEquals(2, RebalanceManager.getInstance().getEndpointInfo(new Endpoint(HOST, 6041, false)).getConnectCount());
        consumer1.unsubscribe();
        consumer2.unsubscribe();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST, mockA.getListenPort(), false));
    }

    @BeforeClass
    public static void before() throws SQLException, IOException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());

        if (null != RebalanceManager.getInstance().getEndpointInfo(new Endpoint(HOST, 6041, false))) {
            Assert.assertEquals(0, RebalanceManager.getInstance().getEndpointInfo(new Endpoint(HOST, 6041, false)).getConnectCount());
        }

        mockA = new TaosAdapterMock();
        mockA.start();

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists topic_ws_fail_over");
        statement.executeUpdate("drop topic if exists topic_ws_rebalance");
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
        statement.execute("create table if not exists ct1 using " + SUPER_TABLE + " tags(2000)");
    }

    @AfterClass
    public static void after() {
        try {
            if (mockA != null) mockA.stop();
            if (connection != null) {
                if (statement != null) {
                    statement.executeUpdate("drop topic if exists topic_ws_fail_over");
                    statement.executeUpdate("drop topic if exists topic_ws_rebalance");
                    statement.executeUpdate("drop database if exists " + DB_NAME);
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        RebalanceManager.getInstance().clearAllForTest();
    }
}