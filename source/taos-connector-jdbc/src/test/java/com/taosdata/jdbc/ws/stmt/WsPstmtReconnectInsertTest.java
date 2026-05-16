package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@RunWith(Parameterized.class)
@FixMethodOrder
public class WsPstmtReconnectInsertTest {

    static final String HOST = TestEnvUtil.getHost();
    static final String DB_NAME = TestUtils.camelToSnake(WsPstmtReconnectInsertTest.class);
    static final String TABLE_NAME = "wpt";
    static Connection connection;
    private final String actionStr;
    private final String mode;

    public WsPstmtReconnectInsertTest(String actionStr, String mode) {
        this.actionStr = actionStr;
        this.mode = mode;
    }
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "\"action\":\"stmt2_init\"", "" },
                { "\"action\":\"stmt2_prepare\"", "" },
                { "\"action\":\"stmt2_bind\"", "" },
                { "\"action\":\"stmt2_exec\"", "" },

                { "\"action\":\"stmt2_init\"", "line" },
                { "\"action\":\"stmt2_prepare\"", "line" },
                { "\"action\":\"stmt2_bind\"", "line" },
                { "\"action\":\"stmt2_exec\"", "line" }
        });
    }

    private void stmt2Write(String url, Properties properties) throws SQLException {

        String sql = "INSERT INTO " + DB_NAME + "." + TABLE_NAME + "(tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (Connection connection = DriverManager.getConnection(url, properties);
                PreparedStatement pstmt = connection.prepareStatement(sql)) {

            for (int i = 1; i <= 5; i++) {
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < 1; j++) {
                    pstmt.setString(1, "d_bind_中国人" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

                    pstmt.setTimestamp(4, new Timestamp(current + i));
                    pstmt.setFloat(5, 1.0f);
                    pstmt.setInt(6, 2);
                    pstmt.setFloat(7, 3.0f);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((5), Utils.getSqlRows(connection, DB_NAME + "." + TABLE_NAME));
            Assert.assertEquals((1), Utils.getSqlRows(connection, DB_NAME + "." + "`d_bind_中国人1`"));
            pstmt.execute("delete from " + DB_NAME + "." + TABLE_NAME);
        }
    }
    @Test
    public void testStmt2Reconnect() throws SQLException, IOException {
        TaosAdapterMock mockB = new TaosAdapterMock(this.actionStr, 1);
        mockB.start();

        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://"
                + HOST + ":" + mockB.getListenPort() + ","
                + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        if ("line".equalsIgnoreCase(this.mode)) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, this.mode);
        }

        stmt2Write(url, properties);
        mockB.stop();
    }

    @Test
    public void testStmt2MultiReconnect() throws SQLException, IOException {
        TaosAdapterMock mockB = new TaosAdapterMock();
        mockB.start();

        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://"
                + HOST + ":" + mockB.getListenPort() + ","
                + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        if ("line".equalsIgnoreCase(this.mode)) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, this.mode);
        }

        String sql = "INSERT INTO " + DB_NAME + "." + TABLE_NAME + "(tbname, groupId, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

        try (Connection connection = DriverManager.getConnection(url, properties);
                PreparedStatement pstmt1 = connection.prepareStatement(sql);
                PreparedStatement pstmt2 = connection.prepareStatement(sql)) {

            PreparedStatement pstmt;
            for (int i = 1; i <= 5; i++) {
                if (i % 2 == 1) {
                    pstmt = pstmt1;
                } else {
                    pstmt = pstmt2;
                }
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < 1; j++) {
                    pstmt.setString(1, "d_bind_中国人" + i);
                    pstmt.setInt(2, i);
                    pstmt.setString(3, "location_" + i);

                    pstmt.setTimestamp(4, new Timestamp(current + i));
                    pstmt.setFloat(5, 1.0f);
                    pstmt.setInt(6, 2);
                    pstmt.setFloat(7, 3.0f);
                    pstmt.addBatch();
                }

                if (i == 1){
                    mockB.stop();
                }

                int[] exeResult = pstmt.executeBatch();

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
            Assert.assertEquals((5), Utils.getSqlRows(connection, DB_NAME + "." + TABLE_NAME));
            Assert.assertEquals((1), Utils.getSqlRows(connection, DB_NAME + "." + "`d_bind_中国人1`"));
            pstmt1.execute("delete from " + DB_NAME + "." + TABLE_NAME);
        }

        mockB.stop();
    }

    @BeforeClass
    public static void setUp() throws SQLException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        TestUtils.runInMain();

        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database " + DB_NAME);
        statement.execute("use " + DB_NAME);
        statement.execute("create stable if not exists " + DB_NAME + "." + TABLE_NAME + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.close();
    }

    @AfterClass
    public static void tearDown() throws SQLException {
        if (connection == null) {
            return;
        }

        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + DB_NAME);
        }
        connection.close();
        System.gc();
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }
}

