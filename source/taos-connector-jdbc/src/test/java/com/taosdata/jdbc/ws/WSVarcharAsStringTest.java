package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.tmq.*;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WSVarcharAsStringTest {

    static final String HOST = TestEnvUtil.getHost();
    static final int PORT = TestEnvUtil.getWsPort();
    private final String dbName = TestUtils.camelToSnake(WSVarcharAsStringTest.class);
    private Connection connection;
    private Statement statement;
    private static final String TOPIC = "topic_ws_map_varchar_as_string";
    private static final String TOPIC_DATABASE = "topic_ws_map_varchar_as_string_db";

    @Description("query")
    @Test
    public void queryResult()  {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("show " + dbName + ".stables")) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Assert.assertEquals("stable_name", metaData.getColumnLabel(1));
            Assert.assertEquals("stable_name", metaData.getColumnName(1));
            Assert.assertEquals("VARCHAR", metaData.getColumnTypeName(1));
            Assert.assertEquals(Types.VARCHAR, metaData.getColumnType(1));
            Assert.assertEquals(String.class.getName(), metaData.getColumnClassName(1));
            while (resultSet.next()) {
                String tableName = resultSet.getString("stable_name");
                Assert.assertEquals("中文", tableName);
                Object tableName2 = resultSet.getObject(1);
                Assert.assertEquals("中文", tableName2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
                        "insert into d1001 values(now, '一')" +
                                "(now+1s, '二')" +
                                "(now+2s, '三')");
            } catch (SQLException e) {
                // ignore
            }
        }, 0, 10, TimeUnit.MILLISECONDS);
        TimeUnit.MILLISECONDS.sleep(11);

        // create topic
        statement.executeUpdate("create topic if not exists " + TOPIC + " as select ts, c1 from `中文`");

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":"+TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_VARCHAR_AS_STRING, "true");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Map<String, Object> map = r.value();
                    Assert.assertEquals(2, map.size());
                    Assert.assertTrue(map.get("ts") instanceof Timestamp);
                    Assert.assertTrue(map.get("c1") instanceof String);
                }
            }
            consumer.unsubscribe();
        }
        scheduledExecutorService.shutdown();
    }

    @Test
    public void testWSEhnMapDB() throws Exception {
        AtomicInteger a = new AtomicInteger(1);
        String topic = TOPIC_DATABASE;
        // create topic
        statement.executeUpdate("create topic if not exists " + topic + " as database " + dbName);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, TestEnvUtil.getHost() + ":"+TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map1");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapEnhanceDeserializer");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_VARCHAR_AS_STRING, "true");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty("fetch.max.wait.ms", "5000");
        properties.setProperty("min.poll.rows", "1000");

        int subTable1Count = 0;
        try (TaosConsumer<TMQEnhMap> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<TMQEnhMap> consumerRecords = consumer.poll(Duration.ofMillis(100));
                if (i == 0){
                    statement.executeUpdate(
                            "insert into d1001 values(now, '一')" +
                                    "(now+1s, '二')" +
                                    "(now+2s, '三')");
                }
                if (consumerRecords.isEmpty()){
                    continue;
                }
                for (ConsumerRecord<TMQEnhMap> r : consumerRecords) {
                    if (r.value().getTableName().equalsIgnoreCase("d1001")){
                        Assert.assertEquals(2, r.value().getMap().size());
                        Assert.assertTrue(r.value().getMap().get("c1") instanceof String);
                        subTable1Count++;
                    }
                }
            }

            Assert.assertEquals(3, subTable1Count);
            consumer.unsubscribe();
        }
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&conmode=1&varcharAsString=true";
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        statement = connection.createStatement();

        statement.executeQuery("drop topic if exists " + TOPIC);
        statement.executeQuery("drop topic if exists " + TOPIC_DATABASE);
        statement.executeQuery("drop database if exists " + dbName);
        statement.executeQuery("create database " + dbName + " keep 36500");
        statement.executeQuery("use " + dbName);

        statement.executeQuery("CREATE STABLE `中文` (ts timestamp, c1 varchar(100)) TAGS (groupId int)");
        statement.executeQuery("CREATE TABLE d1001 USING `中文` TAGS (2)");
        statement.executeQuery("INSERT INTO d1001 USING `中文` TAGS (2) VALUES (NOW, '中文2')");
    }

    @After
    public void after() throws SQLException {
        try {
            if (null != statement) {
                TimeUnit.SECONDS.sleep(3);
                statement.executeQuery("drop topic if exists " + TOPIC);
                statement.executeQuery("drop topic if exists " + TOPIC_DATABASE);
                statement.executeQuery("drop database if exists " + dbName);
                statement.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (Exception e){
            //Ignore exceptions during cleanup
        }
    }
}

