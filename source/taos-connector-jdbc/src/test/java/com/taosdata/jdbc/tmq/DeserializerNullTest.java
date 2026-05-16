package com.taosdata.jdbc.tmq;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@FixMethodOrder
public class DeserializerNullTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(DeserializerNullTest.class);
    private static final String SUPER_TABLE = "st";
    private static final String TOPIC = "topic_with_bean";

    private static Connection connection;

    @Test
    public void JNI_01_TestWithBean() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.DeserializerNullTest$BeanDeserializer");

        try (TaosConsumer<Bean> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            for (int i = 0; i < 1; i++) {
                ConsumerRecords<Bean> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Bean> r : consumerRecords) {
                    Bean bean = r.value();
                    Assert.assertEquals(1000, bean.getT1().intValue());
                    Assert.assertNull(bean.c1);
                    Assert.assertNull(bean.c2);
                    Assert.assertNull(bean.c3);
                    Assert.assertNull(bean.c4);
                    Assert.assertNull(bean.c5);
                }
            }
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
        try (Statement statement = connection.createStatement()) {

            statement.executeUpdate("drop database if exists " + DB_NAME);
            statement.executeUpdate("create database if not exists " + DB_NAME + " WAL_RETENTION_PERIOD 3650");
            statement.executeUpdate("use " + DB_NAME);
            statement.executeUpdate("create stable if not exists " + SUPER_TABLE
                    + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");
            statement.executeUpdate("create table if not exists ct0 using " + SUPER_TABLE + " tags(1000)");
            statement.executeUpdate("insert into " + DB_NAME + ".ct0 (ts) values (now)");
            statement.executeUpdate("create topic if not exists " + TOPIC + " as select ts, c1, c2, c3, c4, c5, t1 from ct0");
        }
    }

    @AfterClass
    public static void after() {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("drop topic if exists " + TOPIC);
            statement.executeUpdate("drop database if exists " + DB_NAME);
        } catch (SQLException e) {
            // nothing
        }
        try {
            connection.close();
        } catch (SQLException e) {
            // nothing
        }
    }

    static class BeanDeserializer extends ReferenceDeserializer<Bean> {
    }

    static class Bean {
        private Timestamp ts;
        private Integer c1;
        private Float c2;
        private String c3;
        private byte[] c4;
        private Integer t1;
        private Boolean c5;

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

        public Float getC2() {
            return c2;
        }

        public void setC2(Float c2) {
            this.c2 = c2;
        }

        public String getC3() {
            return c3;
        }

        public void setC3(String c3) {
            this.c3 = c3;
        }

        public byte[] getC4() {
            return c4;
        }

        public void setC4(byte[] c4) {
            this.c4 = c4;
        }

        public Integer getT1() {
            return t1;
        }

        public void setT1(Integer t1) {
            this.t1 = t1;
        }

        public Boolean getC5() {
            return c5;
        }

        public void setC5(Boolean c5) {
            this.c5 = c5;
        }
    }
}