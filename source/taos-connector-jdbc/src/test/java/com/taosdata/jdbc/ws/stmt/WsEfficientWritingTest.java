package com.taosdata.jdbc.ws.stmt;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.Utils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.WSEWPreparedStatement;
import com.taosdata.jdbc.ws.loadbalance.RebalanceManager;
import com.taosdata.jdbc.ws.loadbalance.RebalanceTestUtil;
import io.netty.util.ResourceLeakDetector;
import org.junit.*;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder
public class WsEfficientWritingTest {
    private final String host = "localhost";
    private final String db_name = TestUtils.camelToSnake(WsEfficientWritingTest.class);
    private final String tableName = "wpt";
    private final String tableNameCopyData = "wpt_cp";
    private final String tableReconnect = "wpt_rc";
    private final String tableNcharTag = "wpt_nchar";
    private final String asyncSqlTable = tableReconnect;
    private Connection connection;
    private TaosAdapterMock taosAdapterMock;
    private final int numOfSubTable = 100;
    private final int numOfRow = 100;
    private final int proxyPort = 8041;
    private final int proxyPort2 = 8042;
    private static final Random random = new Random(System.currentTimeMillis());

    public void createSubTable() throws SQLException{
        // create sub table first
        String createSql = "create table";
        for (int i = 1; i <= numOfSubTable; i++) {
            createSql += " if not exists d_bind_" + i + " using " + db_name + "." + tableName + " tags(" + i + ", \"location_" + i + "\")";
        }
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSql);
        } catch (Exception ex) {
            System.out.printf("Failed to create sub table, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            ex.printStackTrace();
            throw ex;
        }
    }

    @Test
    public void testStmt2InsertStdApiNoTag() throws SQLException {

        String sql = "INSERT INTO " + db_name + "." + tableName + "(tbname, ts, current, voltage, phase) VALUES (?,?,?,?,?)";

        long current = System.currentTimeMillis();
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "d_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setFloat(3, random.nextFloat() * 30);
                    pstmt.setInt(4, random.nextInt(300));
                    pstmt.setFloat(5, random.nextFloat());
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableName));
    }

    @Test
    public void testCopyData() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableNameCopyData + "(tbname, ts, b, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        Timestamp ts = new Timestamp(current);
        byte[] bytes = new byte[10];

        try (Connection con= getConnection(true);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "cpd_bind_" + i);
                    pstmt.setTimestamp(2, ts);
                    pstmt.setBytes(3, bytes);
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }

                ts.setTime(ts.getTime() + 1000); // 增加 1 s
                bytes[0] = (byte)(bytes[0] + 1);

                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData));
        Assert.assertEquals(numOfSubTable, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData + " where b = '\\x63000000000000000000'"));
    }

    @Test
    public void testReconnect() throws SQLException, InterruptedException, IOException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        taosAdapterMock = new TaosAdapterMock(proxyPort2);
        taosAdapterMock.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(host, proxyPort2, false));

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, proxyPort2);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                if (j == 1){
                    taosAdapterMock.stop();
                    Thread.sleep(1000);
                    taosAdapterMock = new TaosAdapterMock(proxyPort2);
                    taosAdapterMock.start();
                    RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(host, proxyPort2, false));
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(numOfSubTable, affectedRows);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + tableReconnect));

        if (taosAdapterMock != null) {
            taosAdapterMock.stop();
        }
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
    }

    @Test
    public void testWriteException() throws SQLException, InterruptedException, IOException {
        taosAdapterMock = new TaosAdapterMock(proxyPort);
        taosAdapterMock.start();

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, proxyPort);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                if (j == 0){
                    taosAdapterMock.stop();
                    Thread.sleep(1000);
                }

                try {
                    pstmt.executeUpdate();
                    Assert.fail("Should throw exception");
                } catch (SQLException e) {
                    Assert.assertTrue(e.getMessage().startsWith("InsertedRows: "));
                    break;
                }
            }
        }
    }

    @Test
    @Ignore
    public void manualTest() throws SQLException, InterruptedException {

        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnection(false, false, 6041);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(numOfSubTable, affectedRows);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection,db_name + "." + tableReconnect));
    }

    @Test
    public void testAsyncSql() throws SQLException {
        String sql = "ASYNC_INSERT INTO " + db_name + "." + asyncSqlTable + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        try (Connection con = getConnectionNormal();
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                current = current + 1000;
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "rc_bind_" + i);
                    pstmt.setTimestamp(2, new Timestamp(current));
                    pstmt.setInt(3, random.nextInt(300));
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }
                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(Statement.SUCCESS_NO_INFO, ele);
                }

                int affectedRows = pstmt.executeUpdate();
                Assert.assertEquals(affectedRows, numOfSubTable);
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection, db_name + "." + asyncSqlTable));
    }

    @Test(expected = SQLException.class)
    public void testStrictCheck() throws SQLException, InterruptedException {
        String sql = "INSERT INTO " + db_name + "." + tableNameCopyData + "(tbname, ts, b, groupId) VALUES (?,?,?,?)";

        long current = System.currentTimeMillis();
        Timestamp ts = new Timestamp(current);
        byte[] bytes = new byte[100];

        try (Connection con= getConnection(true, true);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (int j = 0; j < numOfRow; j++) {
                for (int i = 1; i <= numOfSubTable; i++) {
                    // set columns
                    pstmt.setString(1, "cpd_bind_" + i);
                    pstmt.setTimestamp(2, ts);
                    pstmt.setBytes(3, bytes);
                    pstmt.setInt(4, i);
                    pstmt.addBatch();
                }

                ts.setTime(ts.getTime() + 1000); // 增加 1 s
                bytes[0] = (byte)(bytes[0] + 1);

                int[] exeResult = pstmt.executeBatch();
                Assert.assertEquals(exeResult.length, numOfSubTable);

                for (int ele : exeResult){
                    Assert.assertEquals(ele, Statement.SUCCESS_NO_INFO);
                }
            }
        }

        Assert.assertEquals(numOfSubTable * numOfRow, Utils.getSqlRows(connection,db_name + "." + tableNameCopyData));
        Assert.assertEquals(numOfSubTable, Utils.getSqlRows(connection, db_name + "." + tableNameCopyData + " where b = '\\x63000000000000000000'"));

    }

    @Test(expected = SQLException.class)
    public void testGetMetaDataThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.getMetaData();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataAddBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                WSEWPreparedStatement pstmt = (WSEWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataAddBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataExecuteBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                WSEWPreparedStatement pstmt = (WSEWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataExecuteBatch();
        }
    }

    @Test(expected = SQLException.class)
    public void testColumnDataCloseBatchThrowsSQLException() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableReconnect + "(tbname, ts, i, groupId) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                WSEWPreparedStatement pstmt = (WSEWPreparedStatement)con.prepareStatement(sql)) {
            pstmt.columnDataCloseBatch();
        }
    }
    @Test
    public void testNcharTag() throws SQLException {
        String sql = "INSERT INTO " + db_name + "." + tableNcharTag + "(tbname, ts, i, tag_nchar) VALUES (?,?,?,?)";
        try (Connection con = getConnection(false);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            long current = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                pstmt.setString(1, "ncahr_bind_1");
                pstmt.setTimestamp(2, new Timestamp(current + i));
                pstmt.setInt(3, 100);
                pstmt.setNString(4, "中国人");
                pstmt.addBatch();
                pstmt.executeBatch();
            }
            pstmt.executeUpdate();
        }
    }

    @Before
    public void before() throws SQLException {
        connection = getConnection(false);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create stable if not exists " + db_name + "." + tableName + " (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        statement.execute("create stable if not exists " + db_name + "." + tableNameCopyData + " (ts TIMESTAMP, b varbinary(10)) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + tableReconnect + " (ts TIMESTAMP, i INT) TAGS (groupId INT)");
        statement.execute("create stable if not exists " + db_name + "." + tableNcharTag + " (ts TIMESTAMP, i INT) TAGS (tag_nchar nchar(100))");

        statement.close();

        createSubTable();
    }

    private Connection getConnection(boolean copyData) throws SQLException {
        return getConnection(copyData, false, TestEnvUtil.getWsPort());
    }

    private Connection getConnection(boolean copyData, boolean strictCheck) throws SQLException {
        return getConnection(copyData, strictCheck, TestEnvUtil.getWsPort());
    }
    private Connection getConnectionNormal() throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        return DriverManager.getConnection(url, properties);
    }

    private Connection getConnection(boolean copyData, boolean strictCheck, int port) throws SQLException {
        String url = "jdbc:TAOS-WS://" + host + ":" + port + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "stmt");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "100");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "5");

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "50000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        if (copyData) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_COPY_DATA, "true");
        }

        if (strictCheck){
            properties.setProperty(TSDBDriver.PROPERTY_KEY_STRICT_CHECK, "true");
        }
        return DriverManager.getConnection(url, properties);
    }

    @After
    public void after() throws SQLException, InterruptedException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop database if exists " + db_name);
        }
        connection.close();
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }

    @BeforeClass
    public static void setUp() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
    }

    @AfterClass
    public static void tearDown() {
        System.gc();
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
    }
}

