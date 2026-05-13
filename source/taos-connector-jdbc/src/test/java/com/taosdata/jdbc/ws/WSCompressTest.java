package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.AbstractConnection;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TMQConstants;
import com.taosdata.jdbc.tmq.TaosConsumer;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class WSCompressTest {

        static final String HOST = TestEnvUtil.getHost();
        static final int PORT = TestEnvUtil.getWsPort();
    private static final String DB_NAME = TestUtils.camelToSnake(WSCompressTest.class);
    private static final String TABLE_NAME = "compressA";

    private static final String TOPIC_NAME = "compress_topic";
    private static Connection connection;
    @Description("inertRows")
    @Test
    public void inertRows() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // 必须超过1024个字符才会触发压缩
            statement.execute("INSERT INTO " + DB_NAME + "." + TABLE_NAME + " (tbname, location, groupId, ts, current, voltage, phase) \n" +
                    "                values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32) \n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:36.780', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:37.781', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:38.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:39.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:40.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:41.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:42.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:43.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:44.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:45.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:46.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:47.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:48.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:49.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:50.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:51.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:52.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:53.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:54.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:55.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:56.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:57.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:58.779', 10.15, 217, 0.33)\n" +
                    "                ('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:59.779', 10.16, 217, 0.33)");

            ResultSet resultSet = statement.executeQuery("select * from " + DB_NAME + "." + TABLE_NAME + " order by ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(2) > 10.15);
        }
    }

    @Description("test schemaless insert use compress")
    @Test
    public void schemaless() throws SQLException {
        String lineDemo = "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243097\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243100\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243101\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243102\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243103\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243104\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243105\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243106\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243107\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243108\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243109\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243110\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243111\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243112\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243113\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243114\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243115\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243116\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243117\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243118\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243119\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243120\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243121\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243122\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243123\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243124\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243125\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243126\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.3000002f64,voltage=219i32,phase=0.31f64 1710128243127\n";
        lineDemo += "meters,groupid=2,location=California.SanFrancisco current=10.4000002f64,voltage=219i32,phase=0.31f64 1710128243128\n";

        ((AbstractConnection)connection).write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.MILLI_SECONDS);

        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select current from " + DB_NAME + ".meters order by _ts desc limit 1");
            resultSet.next();
            Assert.assertTrue(resultSet.getFloat(1) > 10.3);
        }
    }

    @Description("test tmq use compress")
    @Test
    public void tmq() throws SQLException {
        inertRows();
        // create topic
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("create topic if not exists " + TOPIC_NAME + " as select * from " + TABLE_NAME);
        }

        Properties properties = new Properties();
        properties.setProperty("td.connect.type", "ws");
        properties.setProperty("bootstrap.servers", TestEnvUtil.getHost() + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "withBean");
        properties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        properties.setProperty(TMQConstants.VALUE_DESERIALIZER, "com.taosdata.jdbc.tmq.MapDeserializer");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Assert.assertEquals("California.SanFrancisco", new String((byte[]) r.value().get("location"), StandardCharsets.UTF_8));
                }
            }
            consumer.unsubscribe();
        }
    }

    @BeforeClass
    public static void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_COMPRESSION, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop topic if exists " + TOPIC_NAME);
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database " + DB_NAME);
        statement.execute("use " + DB_NAME);
        statement.execute("CREATE STABLE " + TABLE_NAME + " (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);");
        statement.close();
    }

    @AfterClass
    public static void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop topic if exists " + TOPIC_NAME);
                statement.executeUpdate("drop database if exists " + DB_NAME);
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }
}

