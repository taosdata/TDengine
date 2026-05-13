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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WSConsumerNullTest {
    private static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerNullTest.class);
    private static final String SUPER_TABLE = "tmq_type";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_" + DB_NAME};

    @Test
    public void testWSBeanObject() throws Exception {
        try {
            statement.executeUpdate("insert into " + DB_NAME + ".ct0 (ts) values(now)");
        } catch (SQLException e) {
            // ignore
        }
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, t1 from " + DB_NAME + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try (TaosConsumer<ResultBean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<ResultBean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<ResultBean> r : consumerRecords) {
                    ResultBean bean = r.value();
                    Assert.assertEquals(0, bean.getC1());
                    Assert.assertNull(bean.getC2());
                    Assert.assertNull(bean.getC3());
                    Assert.assertNull(bean.getC4());
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();
        statement.execute("drop topic if exists topic_ws_bean_type");
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
    }

    @AfterClass
    public static void after() throws InterruptedException {
        try {
            if (connection != null) {
                if (statement != null) {
                    for (String topic : topics) {
                        TimeUnit.SECONDS.sleep(3);
                        statement.executeUpdate("drop topic " + topic);
                        statement.executeUpdate("drop database if exists " + DB_NAME);
                    }
                    statement.close();
                }
                connection.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }
}

