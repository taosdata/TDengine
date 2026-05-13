package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WSConsumerAutoCommitTest {

        static final String HOST = TestEnvUtil.getHost();
        private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerAutoCommitTest.class);
    private static final String SUPER_TABLE = "st";
    private static final String TOPIC = "ws_topic_with_bean";

    private static Connection connection;

    private static ScheduledExecutorService scheduledExecutorService;
    private static int count = 0;
    private volatile boolean stop = false;

    @Test
    public void TestWithBean() throws Exception {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                if (!stop) {
                    Statement statement = connection.createStatement();
                    String sql = String.format("insert into %s.ct0 (ts, c1) values (now, %s)", DB_NAME, count);
                    statement.executeUpdate(sql);
                    count++;
                }
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }, 20, 10, TimeUnit.MILLISECONDS);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "30");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.ws.WSConsumerAutoCommitTest$BeanDeserializer");

        int last = 0;
        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            for (int i = 0; i < 5; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    last = bean.getC1();
                }
            }
            Thread.sleep(30);
            //this poll will commit msg last received
            consumer.poll(Duration.ofMillis(10));

        }

        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<Bean> r : consumerRecords) {
                Bean bean = r.value();
                //new msg value will bigger than last received
                assert (bean.getC1() > last);
                break; //NOSONAR
            }
            consumer.unsubscribe();
        }
        stop = true;
        scheduledExecutorService.shutdown();
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {

            statement.executeUpdate("drop topic if exists " + TOPIC);
            statement.executeUpdate("drop database if exists " + DB_NAME);
            statement.executeUpdate("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
            statement.executeUpdate("use " + DB_NAME);
            statement.executeUpdate("create stable if not exists " + SUPER_TABLE
                    + " (ts timestamp, c1 int) tags(t1 int)");
            statement.executeUpdate("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
            statement.executeUpdate("create topic if not exists " + TOPIC + " as select ts, c1, t1 from ct0");
        }
    }

    @AfterClass
    public static void after() throws InterruptedException, SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists " + TOPIC);
        statement.executeUpdate("drop database if exists " + DB_NAME);
        TimeUnit.SECONDS.sleep(3);
        connection.close();
        }

    static class BeanDeserializer extends ReferenceDeserializer<Bean> {
    }

    static class Bean {
        private Timestamp ts;
        private Integer c1;
        private Integer t1;

        public Timestamp getTs() {
            return ts;
        }

        public void setTs(Timestamp ts) {
            this.ts = ts;
        }

        public Integer getC1() {
            return c1;
        }

        public void setC1(Integer c1) {
            this.c1 = c1;
        }

        public Integer getT1() {
            return t1;
        }

        public void setT1(Integer t1) {
            this.t1 = t1;
        }
    }
}

