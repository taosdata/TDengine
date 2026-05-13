package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.enums.SchemalessProtocolType;
import com.taosdata.jdbc.enums.SchemalessTimestampType;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;
@SuppressWarnings("java:S1874")
public class RestfulConnectionTest {


    private static Connection conn;
    private static final String DB_NAME = TestUtils.camelToSnake(RestfulConnectionTest.class);

    @Test
    public void getConnection() {
        // already test in beforeClass method
    }

    @Test
    public void createStatement() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show cluster alive");
            rs.next();
            int status = rs.getInt("status");
            System.out.println("status = " + status);

            assertTrue(status > 0);
        }
    }

    @Test
    public void prepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive");
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        System.out.println("status = " + status);

        assertTrue(status > 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void prepareCall() throws SQLException {
        conn.prepareCall("show cluster alive");
    }

    @Test
    public void nativeSQL() throws SQLException {
        String nativeSQL = conn.nativeSQL("select * from " + DB_NAME);
        Assert.assertEquals("select * from " + DB_NAME, nativeSQL);
    }

    @Test
    public void setAutoCommit() throws SQLException {
        conn.setAutoCommit(true);
        conn.setAutoCommit(false);
    }

    @Test
    public void getAutoCommit() throws SQLException {
        Assert.assertTrue(conn.getAutoCommit());
    }

    @Test
    public void commit() throws SQLException {
        conn.commit();
    }

    @Test
    public void rollback() throws SQLException {
        conn.rollback();
    }

    @Test
    public void close() {
        // connection will close in afterClass method
    }

    @Test
    public void isClosed() throws SQLException {
        assertFalse(conn.isClosed());
    }

    @Test
    public void getMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        Assert.assertNotNull(meta);
        assertEquals("com.taosdata.jdbc.rs.RestfulDriver", meta.getDriverName());
    }

    @Test
    public void setReadOnly() throws SQLException {
        conn.setReadOnly(true);
    }

    @Test
    public void isReadOnly() throws SQLException {
        Assert.assertTrue(conn.isReadOnly());
    }

    @Test
    public void setCatalog() throws SQLException {
        conn.setCatalog(DB_NAME);
        assertEquals(DB_NAME, conn.getCatalog());
    }

    @Test
    public void getCatalog() throws SQLException {
        conn.setCatalog("log");
        assertEquals("log", conn.getCatalog());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTransactionIsolation() throws SQLException {
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Test
    public void getTransactionIsolation() throws SQLException {
        assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
    }

    @Test
    public void getWarnings() throws SQLException {
        Assert.assertNull(conn.getWarnings());
    }

    @Test
    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("show cluster alive");
        rs.next();
        int status = rs.getInt("status");
        System.out.println("status = " + status);
        assertTrue(status > 0);

        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        System.out.println("status = " + status);

        assertTrue(status > 0);

        conn.prepareStatement("select 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareCall() throws SQLException {
        conn.prepareCall("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTypeMap() throws SQLException {
        conn.getTypeMap();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTypeMap() throws SQLException {
        conn.setTypeMap(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setHoldability() throws SQLException {
        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void getHoldability() throws SQLException {
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSavepoint() throws SQLException {
        conn.setSavepoint();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testSetSavepoint() throws SQLException {
        conn.setSavepoint(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testRollback() throws SQLException {
        conn.rollback(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void releaseSavepoint() throws SQLException {
        conn.releaseSavepoint(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStatement1() throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet rs = stmt.executeQuery("show cluster alive");
        rs.next();
        int status = rs.getInt("status");
        System.out.println("status = " + status);

        assertTrue(status > 0);

        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement1() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        System.out.println("status = " + status);

        assertTrue(status > 0);

        conn.prepareStatement("select 1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareCall1() throws SQLException {
        conn.prepareCall("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement2() throws SQLException {
        conn.prepareStatement("", Statement.RETURN_GENERATED_KEYS);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement3() throws SQLException {
        conn.prepareStatement("", new int[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement4() throws SQLException {
        conn.prepareStatement("", new String[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createClob() throws SQLException {
        conn.createClob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createBlob() throws SQLException {
        conn.createBlob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createNClob() throws SQLException {
        conn.createNClob();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createSQLXML() throws SQLException {
        conn.createSQLXML();
    }

    @Test(expected = SQLException.class)
    public void isValid() throws SQLException {
        Assert.assertTrue(conn.isValid(10));
        Assert.assertTrue(conn.isValid(0));
        conn.isValid(-1);
    }

    @Test
    public void setClientInfo() throws SQLClientInfoException {
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "en_US.UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "UTC-8");
    }

    @Test
    public void testSetClientInfo() throws SQLClientInfoException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        conn.setClientInfo(properties);
    }

    @Test
    public void getClientInfo() throws SQLException {
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        Properties info = conn.getClientInfo();
        String charset = info.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET);
        assertEquals("UTF-8", charset);
        String locale = info.getProperty(TSDBDriver.PROPERTY_KEY_LOCALE);
        assertEquals("en_US.UTF-8", locale);
        String timezone = info.getProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
        assertEquals("UTC-8", timezone);
    }

    @Test
    public void testGetClientInfo() throws SQLException {
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String charset = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET);
        assertEquals("UTF-8", charset);
        String locale = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_LOCALE);
        assertEquals("en_US.UTF-8", locale);
        String timezone = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
        assertEquals("UTC-8", timezone);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createArrayOf() throws SQLException {
        conn.createArrayOf("", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void createStruct() throws SQLException {
        conn.createStruct("", null);
    }

    @Test
    public void setSchema() throws SQLException {
        conn.setSchema("test");
    }

    @Test
    public void getSchema() throws SQLException {
        Assert.assertNull(conn.getSchema());
    }

    @Test
    public void abort() throws SQLException {
        conn.abort(null);
    }

    @Test
    public void setNetworkTimeout() throws SQLException {
        conn.setNetworkTimeout(null, 1000);
    }

    @Test
    public void getNetworkTimeout() throws SQLException {
        int timeout = conn.getNetworkTimeout();
        assertEquals(0, timeout);
    }

    @Test
    public void unwrap() throws SQLException {
        RestfulConnection restfulConnection = conn.unwrap(RestfulConnection.class);
        Assert.assertNotNull(restfulConnection);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(conn.isWrapperFor(RestfulConnection.class));
    }

    // Test write method throws unsupported exception
    @Test(expected = SQLException.class)
    public void testWriteMethodUnsupported() throws SQLException {
        String[] lines = {"test line"};
        ((RestfulConnection)conn).write(lines, SchemalessProtocolType.LINE, SchemalessTimestampType.NOT_CONFIGURED, 0, 1L);
    }

    // Test writeRaw method throws unsupported exception
    @Test(expected = SQLException.class)
    public void testWriteRawMethodUnsupported() throws SQLException {
        String line = "test raw line";
        ((RestfulConnection)conn).writeRaw(line, SchemalessProtocolType.LINE, SchemalessTimestampType.NOT_CONFIGURED, 0, 1L);
    }

    // Test getter methods of RestfulConnection
    @Test
    public void testRestfulConnectionGetters() throws SQLException {
        RestfulConnection restfulConn = conn.unwrap(RestfulConnection.class);

        // Verify basic getters (values depend on actual connection config)
        assertNotNull("Host should not be null", restfulConn.getHost());
        assertNotNull("Port should not be null", restfulConn.getPort());
        assertNotNull("URL should not be null", restfulConn.getUrl());
        assertNotNull("Auth should not be null (Basic auth format)", restfulConn.getAuth());
        assertNull("Database name mismatch", restfulConn.getDatabase()); // Catalog matches database

        // Test other getters
        assertNull("Token should be null for basic auth", restfulConn.getToken());
        assertNotNull("Timezone should not be null", restfulConn.getTz());
        assertFalse("SSL should be disabled by default", restfulConn.isUseSsl());
    }

    // Test createStatement when connection is closed
    @Test(expected = SQLException.class)
    public void testCreateStatementAfterClose() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());
        tempConn.close();
        tempConn.createStatement();
    }

    // Test prepareStatement when connection is closed
    @Test(expected = SQLException.class)
    public void testPrepareStatementAfterClose() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());
        tempConn.close();
        tempConn.prepareStatement("show databases");
    }

    // Test getMetaData when connection is closed
    @Test(expected = SQLException.class)
    public void testGetMetaDataAfterClose() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());
        tempConn.close();
        tempConn.getMetaData();
    }

    // Test close method called multiple times (no exception)
    @Test
    public void testCloseMultipleTimes() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());
        assertFalse("Connection should be open initially", tempConn.isClosed());

        tempConn.close();
        assertTrue("Connection should be closed after first close", tempConn.isClosed());

        // Second close should not throw exception
        tempConn.close();
        assertTrue("Connection should remain closed after second close", tempConn.isClosed());
    }

    // Test isClosed returns correct status
    @Test
    public void testIsClosedStatus() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());

        // Initial state: open
        assertFalse("New connection should not be closed", tempConn.isClosed());

        // After close: closed
        tempConn.close();
        assertTrue("Connection should be closed after close()", tempConn.isClosed());
    }

    // Test error code for closed connection operations
    @Test
    public void testClosedConnectionErrorCode() throws SQLException {
        Connection tempConn = DriverManager.getConnection(conn.getMetaData().getURL());
        tempConn.close();

        try {
            tempConn.createStatement();
            fail("Should throw SQLException for closed connection");
        } catch (SQLException e) {
            assertEquals("Incorrect error code for closed connection",
                    TSDBErrorNumbers.ERROR_CONNECTION_CLOSED, e.getErrorCode());
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);
        // create test database for test cases
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("create database if not exists " + DB_NAME);
            Thread.sleep(3000);
        }

    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.close();
            conn.close();
        }
    }
}