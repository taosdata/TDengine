package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WSConsumerSubscribeDBTest {
    private static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerSubscribeDBTest.class);
    private static final String SUPER_TABLE_1 = "st1";
    private static final String SUPER_TABLE_2 = "st2";
    private static final String SUPER_TABLE_FULL_TYPE = "st3";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_" + DB_NAME};

    @Test
    public void testWSEhnMapDB() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map1");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        int subTable1Count = 0;
        int subTable2Count = 0;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate(
                            "insert into subtb1 using "+ SUPER_TABLE_1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                    "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");

                    statement.executeUpdate(
                            "insert into subtb2 using "+ SUPER_TABLE_2 + " tags(1, 2) values(now, " + a.getAndIncrement() + ")" +
                                    "(now+1s," + a.getAndIncrement() + ")");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("subtb1")){
                        Assert.assertEquals(6, r.value().getMap().size());
                        subTable1Count++;
                    }
                    if (r.value().getTableName().equalsIgnoreCase("subtb2")){
                        Assert.assertEquals(2, r.value().getMap().size());
                        subTable2Count++;
                    }
                }
            }

            Assert.assertEquals(3, subTable1Count);
            Assert.assertEquals(2, subTable2Count);
            consumer.unsubscribe();
        }
    }

    @Test
    public void testWSEhnMapDBAllType() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as database " + DB_NAME);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map2");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        boolean pass = false;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate("insert into " + DB_NAME + ".ct0 values(1747474225447, 1, 100, 2.2, 2.3, '1', 12, 2, true, '一', 'POINT(1 1)', '\\x0101', 1.2234, 1747474225448, 255, 65535, 4294967295, 18446744073709551615, -12345678901234567890123.4567890000, 12345678.901234)");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("ct0")){
                        Assert.assertEquals(20, r.value().getMap().size());

                        Assert.assertTrue(r.value().getMap().get("ts") instanceof Timestamp);
                        Assert.assertTrue(r.value().getMap().get("c1") instanceof Integer);
                        Assert.assertTrue(r.value().getMap().get("c2") instanceof Long);
                        Assert.assertTrue(r.value().getMap().get("c3") instanceof Float);
                        Assert.assertTrue(r.value().getMap().get("c4") instanceof Double);
                        Assert.assertTrue(r.value().getMap().get("c5") instanceof byte[]);
                        Assert.assertTrue(r.value().getMap().get("c6") instanceof Short);
                        Assert.assertTrue(r.value().getMap().get("c7") instanceof Byte);
                        Assert.assertTrue(r.value().getMap().get("c8") instanceof Boolean);
                        Assert.assertTrue(r.value().getMap().get("c9") instanceof String);
                        Assert.assertTrue(r.value().getMap().get("c10") instanceof byte[]);
                        Assert.assertTrue(r.value().getMap().get("c11") instanceof byte[]);
                        Assert.assertTrue(r.value().getMap().get("c12") instanceof Double);
                        Assert.assertTrue(r.value().getMap().get("c13") instanceof Timestamp);
                        Assert.assertTrue(r.value().getMap().get("c14") instanceof Short);
                        Assert.assertTrue(r.value().getMap().get("c15") instanceof Integer);
                        Assert.assertTrue(r.value().getMap().get("c16") instanceof Long);
                        Assert.assertTrue(r.value().getMap().get("c17") instanceof BigInteger);
                        Assert.assertTrue(r.value().getMap().get("c18") instanceof BigDecimal);
                        Assert.assertTrue(r.value().getMap().get("c19") instanceof BigDecimal);

                        Assert.assertEquals(new Timestamp(1747474225447L), r.value().getMap().get("ts"));
                        Assert.assertEquals(r.value().getMap().get("c1"), 1);
                        Assert.assertEquals(100L, r.value().getMap().get("c2"));
                        Assert.assertEquals(2.2F, r.value().getMap().get("c3"));
                        Assert.assertEquals(2.3, r.value().getMap().get("c4"));
                        Assert.assertArrayEquals("1".getBytes(), (byte[]) r.value().getMap().get("c5"));
                        Assert.assertEquals((short) 12, r.value().getMap().get("c6"));
                        Assert.assertEquals((byte) 2, r.value().getMap().get("c7"));
                        Assert.assertEquals(true, r.value().getMap().get("c8"));
                        Assert.assertEquals("一", r.value().getMap().get("c9"));
                        Assert.assertArrayEquals(new byte[]{1, 1}, (byte[]) r.value().getMap().get("c11"));
                        Assert.assertEquals(1.2234, r.value().getMap().get("c12"));
                        Assert.assertEquals(new Timestamp(1747474225448L), r.value().getMap().get("c13"));
                        Assert.assertEquals((short) 255, r.value().getMap().get("c14"));
                        Assert.assertEquals(65535, r.value().getMap().get("c15"));
                        Assert.assertEquals(4294967295L, r.value().getMap().get("c16"));
                        Assert.assertEquals(new BigInteger("18446744073709551615"), r.value().getMap().get("c17"));
                        Assert.assertEquals(new BigDecimal("-12345678901234567890123.4567890000"), r.value().getMap().get("c18"));
                        Assert.assertEquals(new BigDecimal("12345678.901234"), r.value().getMap().get("c19"));
                        pass = true;
                    }
                }
            }

            consumer.unsubscribe();
            Assert.assertTrue(pass);
        }
    }

    @Test
    public void testWSEhnMapStable() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = topics[0];
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as STABLE " + SUPER_TABLE_1);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map3");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        int beforeCol = 0;
        int afterCol = 0;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate(
                            "insert into subtb1 using "+ SUPER_TABLE_1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false)" +
                                    "(now+2s," + a.getAndIncrement() + ",0.6,'c','三', false)");

                    statement.executeUpdate(
                            "ALTER STABLE " + SUPER_TABLE_1 + " ADD COLUMN c6 int");

                    statement.executeUpdate(
                            "insert into subtb1 using "+ SUPER_TABLE_1 + " tags(1) values(now, " + a.getAndIncrement() + ", 0.2, 'a','一', true, 6)" +
                                    "(now+1s," + a.getAndIncrement() + ",0.4,'b','二', false, 7)");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("subtb1") && r.value().getMap().size() == 6){
                        beforeCol++;
                    }
                    if (r.value().getTableName().equalsIgnoreCase("subtb1") && r.value().getMap().size() == 7){
                        afterCol++;
                    }
                }
            }
            Assert.assertEquals(3, beforeCol);
            Assert.assertEquals(2, afterCol);
            consumer.unsubscribe();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
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
        statement.execute("create stable if not exists " + SUPER_TABLE_1
                + " (ts timestamp, c1 int, c2 float, c3 nchar(10), c4 binary(10), c5 bool) tags(t1 int)");

        statement.execute("create stable if not exists " + SUPER_TABLE_2
                + " (ts timestamp, cc1 int) tags(t1 int, t2 int)");

        statement.execute("create stable if not exists " + SUPER_TABLE_FULL_TYPE
                + " (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(10), c6 SMALLINT, c7 TINYINT, " +
                "c8 BOOL, c9 nchar(100), c10 GEOMETRY(100), c11 VARBINARY(100), c12 double, c13 timestamp, " +
                "c14 tinyint unsigned, c15 smallint unsigned, c16 int unsigned, c17 bigint unsigned, c18 decimal(38, 10), c19 decimal(18, 6)) tags(t1 int)");
        statement.execute("create table if not exists ct0 using " + SUPER_TABLE_FULL_TYPE + " tags(1000)");
    }

    @After
    public void after() throws InterruptedException {
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

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
    }
}

