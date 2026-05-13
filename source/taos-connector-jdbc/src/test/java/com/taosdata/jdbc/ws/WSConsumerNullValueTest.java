package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.Bean;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WSConsumerNullValueTest {
    private static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerNullValueTest.class);
    private static final String SUPER_TABLE = "tmq_type";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_" + DB_NAME};

    private void insertOneRow() throws SQLException {
        statement.executeUpdate("insert into " + DB_NAME + ".ct0 values(now, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)");
    }
    @Test
    public void testWSBeanObject() throws Exception {
        insertOneRow();

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, t1 from " + DB_NAME + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.common.ResultDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");

        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    Assert.assertNull(bean.getC1());
                    Assert.assertNull(bean.getC2());
                    Assert.assertNull(bean.getC3());
                    Assert.assertNull(bean.getC4());
                    Assert.assertNull(bean.getC5());
                    Assert.assertNull(bean.getC6());
                    Assert.assertNull(bean.getC7());
                    Assert.assertNull(bean.getC9());

                    Assert.assertNull(bean.getC12());
                    Assert.assertNull(bean.getC14());
                    Assert.assertNull(bean.getC15());
                    Assert.assertNull(bean.getC16());
                    Assert.assertNull(bean.getC17());
                    Assert.assertNull(bean.getC18());
                    Assert.assertNull(bean.getC19());
                    Assert.assertNull(bean.getC20());

                    Assert.assertEquals(1000.0, bean.getT1(), 0.000001);
                }
            }
            consumer.unsubscribe();
        }
    }
    @Test
    public void testWSBeanMap() throws Exception {
        insertOneRow();

        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic +
                " as select ts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, t1 from " + DB_NAME + ".ct0");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_bean");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Map<String, Object> map = r.value();
                    Assert.assertEquals(22 , map.size());
                    Assert.assertTrue(map.get("ts") instanceof Timestamp);

                    for (int j = 1; j <= 20; j++) {
                        Assert.assertNull(map.get("c" + j));
                    }

                    Assert.assertEquals(1000.0, (int)map.get("t1"), 0.000001);

                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        TestUtils.runInMain();

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();

        statement.executeUpdate("drop topic if exists " + topics[0]);
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + SUPER_TABLE
                + " (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 SMALLINT, c7 TINYINT, " +
                "c8 BOOL, c9 nchar(100), c10 GEOMETRY(100), c11 VARBINARY(100), c12 double, c13 timestamp, " +
                "c14 tinyint unsigned, c15 smallint unsigned, c16 int unsigned, c17 bigint unsigned, c18 decimal(38, 10), c19 decimal(18, 6), c20 blob) tags(t1 int)");
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
            //
        }
    }
}