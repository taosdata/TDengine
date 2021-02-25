package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.management.OperationsException;
import java.sql.*;
import java.util.Properties;

public class TSDBConnectionTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;

    @Test
    public void getConnection() {
        // already test in beforeClass method
    }

    @Test
    public void createStatement() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select server_status()");
            rs.next();
            int status = rs.getInt("server_status()");
            Assert.assertEquals(1, status);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void subscribe() {
        try {
            TSDBConnection unwrap = conn.unwrap(TSDBConnection.class);
            TSDBSubscribe subscribe = unwrap.subscribe("topic1", "select * from log.log", false);
            TSDBResultSet rs = subscribe.consume();
            ResultSetMetaData metaData = rs.getMetaData();
            for (int count = 0; count < 10 && rs.next(); count++) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String value = rs.getString(i);
                    System.out.print(metaData.getColumnLabel(i) + ":" + value + "\t");
                }
                System.out.println();
            }
            Assert.assertNotNull(rs);
            subscribe.close(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void prepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select server_status()");
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("server_status()");
        Assert.assertEquals(1, status);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void prepareCall() throws SQLException {
        conn.prepareCall("select server_status()");
    }

    @Test
    public void nativeSQL() throws SQLException {
        String nativeSQL = conn.nativeSQL("select * from log.log");
        Assert.assertEquals("select * from log.log", nativeSQL);
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
        Assert.assertFalse(conn.isClosed());
    }

    @Test
    public void getMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        Assert.assertNotNull(meta);
        Assert.assertEquals("com.taosdata.jdbc.TSDBDriver", meta.getDriverName());
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
        conn.setCatalog("test");
        Assert.assertEquals("test", conn.getCatalog());
    }

    @Test
    public void getCatalog() throws SQLException {
        conn.setCatalog("log");
        Assert.assertEquals("log", conn.getCatalog());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTransactionIsolation() throws SQLException {
        conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
        Assert.assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    }

    @Test
    public void getTransactionIsolation() throws SQLException {
        Assert.assertEquals(Connection.TRANSACTION_NONE, conn.getTransactionIsolation());
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
        ResultSet rs = stmt.executeQuery("select server_status()");
        rs.next();
        int status = rs.getInt("server_status()");
        Assert.assertEquals(1, status);

        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select server_status()",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("server_status()");
        Assert.assertEquals(1, status);

        conn.prepareStatement("select server_status", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
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
        Assert.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    @Test
    public void getHoldability() throws SQLException {
        Assert.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
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
        ResultSet rs = stmt.executeQuery("select server_status()");
        rs.next();
        int status = rs.getInt("server_status()");
        Assert.assertEquals(1, status);

        conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement1() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select server_status()",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("server_status()");
        Assert.assertEquals(1, status);

        conn.prepareStatement("select server_status", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareCall1() throws SQLException {
        conn.prepareCall("", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement2() throws SQLException {
        Assert.assertNotNull("", Statement.NO_GENERATED_KEYS);
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
        Assert.assertEquals("UTF-8", charset);
        String locale = info.getProperty(TSDBDriver.PROPERTY_KEY_LOCALE);
        Assert.assertEquals("en_US.UTF-8", locale);
        String timezone = info.getProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
        Assert.assertEquals("UTC-8", timezone);
    }

    @Test
    public void testGetClientInfo() throws SQLException {
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        conn.setClientInfo(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String charset = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_CHARSET);
        Assert.assertEquals("UTF-8", charset);
        String locale = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_LOCALE);
        Assert.assertEquals("en_US.UTF-8", locale);
        String timezone = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIME_ZONE);
        Assert.assertEquals("UTC-8", timezone);
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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNetworkTimeout() throws SQLException {
        conn.setNetworkTimeout(null, 1000);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNetworkTimeout() throws SQLException {
        conn.getNetworkTimeout();
    }

    @Test
    public void unwrap() {
        try {
            TSDBConnection tsdbConnection = conn.unwrap(TSDBConnection.class);
            Assert.assertNotNull(tsdbConnection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(conn.isWrapperFor(TSDBConnection.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/log?user=root&password=taosdata", properties);
            // create test database for test cases
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("create database if not exists test");
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}