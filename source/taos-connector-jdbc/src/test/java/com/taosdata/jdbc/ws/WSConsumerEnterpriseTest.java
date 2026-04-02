package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
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
import org.junit.runner.RunWith;

import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test TMQ consumer with token authentication (Enterprise Edition only)
 * You need to start taosadapter before testing this method
 */
@RunWith(CatalogRunner.class)
@TestTarget(alias = "test tmq consumer with token", author = "yjshe", version = "3.3.0")
public class WSConsumerEnterpriseTest {
    private static String host = "localhost";
    private static final String DB_NAME = TestUtils.camelToSnake(WSConsumerEnterpriseTest.class);
    private static final String SUPER_TABLE = "st";
    private static Connection connection;
    private static Statement statement;
    private static final String topicName = "topic_token_test_" + DB_NAME;

    @Test
    @Description("test tmq consumer with td.connect.token parameter")
    public void testConsumerWithToken() throws Exception {
        String token = "";
        // create token
        try (Connection conn = DriverManager.getConnection("jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("CREATE TOKEN jdbc_tmq_token FROM user " + TestEnvUtil.getUser() + " ENABLE 1 PROVIDER 'root' ttl 1");
            TestUtils.waitTransactionDone(conn);
            rs.next();
            token = rs.getString(1);
            System.out.println("created token = " + token);
            rs.close();
        }

        if (token == null || token.isEmpty()) {
            Assert.fail("no token created");
        }

        AtomicInteger a = new AtomicInteger(1);
        statement.executeUpdate("insert into ct0 values(now, " + a.getAndIncrement() + ")");

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setName("topic-thread-" + t.getId());
            return t;
        });
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                statement.executeUpdate("insert into ct0 values(now, " + a.getAndIncrement() + ")");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.MILLISECONDS);

        // create topic
        statement.executeUpdate("create topic if not exists " + topicName + " as select ts, c1 from ct0");

        // create consumer with td.connect.token
        Properties properties = new Properties();
        properties.setProperty(TMQConstants.TMQ_CONNECT_TOKEN, token);  // use td.connect.token
        properties.setProperty(TMQConstants.CONNECT_USER, TestEnvUtil.getUser());  // still set user/password (server will handle priority)
        properties.setProperty(TMQConstants.CONNECT_PASS, TestEnvUtil.getPassword());
        properties.setProperty(TMQConstants.BOOTSTRAP_SERVERS, host + ":" + TestEnvUtil.getWsPort());
        properties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        properties.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "true");
        properties.setProperty(TMQConstants.GROUP_ID, "ws_token_group");
        properties.setProperty(TMQConstants.CONNECT_TYPE, "ws");

        try (TaosConsumer<Map<String, Object>> consumer = new TaosConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topicName));

            // poll some data
            int totalRecords = 0;
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<Map<String, Object>> consumerRecords = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Map<String, Object>> r : consumerRecords) {
                    Map<String, Object> map = r.value();
                    Assert.assertEquals(2, map.size());  // ts and c1
                    Assert.assertTrue(map.get("ts") instanceof Timestamp);
                    totalRecords++;
                }
                if (totalRecords > 0) {
                    break;
                }
            }

            Assert.assertTrue("Should have received at least some records", totalRecords > 0);
            System.out.println("Successfully consumed " + totalRecords + " records using token authentication");

            consumer.unsubscribe();
        } finally {
            scheduledExecutorService.shutdown();

            // revoke token
            try (Connection conn = DriverManager.getConnection("jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword());
                    Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("drop token jdbc_tmq_token");
                TestUtils.waitTransactionDone(conn);
                System.out.println("token dropped");
            }
        }
    }

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtils.runInEnterprise();

        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost != null) {
            host = specifyHost;
        }

        String url = "jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        connection = DriverManager.getConnection(url);
        statement = connection.createStatement();

        // create database
        statement.executeUpdate("create database if not exists " + DB_NAME);
        TestUtils.waitTransactionDone(connection);
        statement.executeUpdate("use " + DB_NAME);

        // create super table
        String createSuperTable = "create stable if not exists " + SUPER_TABLE + " (ts timestamp, c1 int) tags (t1 int)";
        statement.executeUpdate(createSuperTable);

        // create child table
        statement.executeUpdate("create table if not exists ct0 using " + SUPER_TABLE + " tags(1)");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (statement != null) {
            statement.executeUpdate("drop topic if exists " + topicName);
            statement.executeUpdate("drop database if exists " + DB_NAME);
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
