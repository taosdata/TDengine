package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.tmq.meta.MetaDeleteData;
import com.taosdata.jdbc.ws.tmq.meta.MetaType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
@RunWith(Parameterized.class)
public class WSConsumerMetaDeleteTest {
    private static final String HOST = "127.0.0.1";
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerMetaDeleteTest.class);
    private static final String SUPER_TABLE = "st";
    private static final String SUPER_TABLE_JSON = "st_json";
    private static Connection connection;
    private static Statement statement;
    private static final String[] topics = {"topic_ws_map" + DB_NAME, "topic_db" + DB_NAME, "topic_json" + DB_NAME};
    private final String topicWith;

    public WSConsumerMetaDeleteTest(String topicWith) {
        this.topicWith = topicWith;
    }
    @Parameterized.Parameters
    public static Collection<String> data() {
        return Arrays.asList("stable " + SUPER_TABLE , "database " + DB_NAME);
    }

    @Test
    public void testDeleteData() throws Exception {
        String topic = topics[0];
        // create topic
        statement.executeUpdate("drop topic if exists " + topic);
        statement.executeUpdate("create topic if not exists " + topic + " with meta as " + topicWith);

        Properties properties = new Properties();
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, HOST + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "latest");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_map");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        properties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "1");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(Duration.ofMillis(100));
            {
                int idx = 1;
                String sql = String.format("insert into %s using %s.%s tags(%s, '%s', %s) values (1756792428951, 1)", "act" + idx, DB_NAME, SUPER_TABLE, idx, "t" + idx, "true");
                statement.execute(sql);
                String sql2 = String.format("delete from %s.act%s where t1 = \"t1\"", DB_NAME, idx);
                statement.execute(sql2);
            }

            int looptime = 10;
            boolean getData = false;
            boolean getDelete = false;
            while (looptime > 0) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    if (r.getMeta() != null && r.getMeta().getType() == MetaType.DELETE) {
                        MetaDeleteData meta = (MetaDeleteData) r.getMeta();
                        getDelete = true;
                        Assert.assertTrue(meta.getSql().startsWith("delete from `act1`"));
                    }

                    if (r.value() != null){
                        getData = true;
                        Assert.assertEquals(2, r.value().size());
                    }
                }
                looptime--;
                if (getDelete && getData){
                    break;
                }
            }
            Assert.assertTrue(getDelete && getData);
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":6041/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
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
                + " (ts timestamp, c1 int) tags(t1 int, t2 varchar(10), t3 bool)");
        statement.execute("create stable if not exists " + SUPER_TABLE_JSON
                + " (ts timestamp, c1 int) tags(t1 json)");
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

