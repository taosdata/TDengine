package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class TSDBConnectionTest {
    static final String HOST = TestEnvUtil.getHost();
    private static Connection conn;
    @Test
    public void createStatement() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("show cluster alive");
            rs.next();
            int status = rs.getInt("status");
            Assert.assertTrue(status > 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void prepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive");
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        Assert.assertTrue(status > 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void prepareCall() throws SQLException {
        conn.prepareCall("show cluster alive");
    }

    @Test
    public void nativeSQL() throws SQLException {
        String nativeSQL = conn.nativeSQL("select * from test_db");
        Assert.assertEquals("select * from test_db", nativeSQL);
    }

    @Test
    public void setAutoCommit() throws SQLException {
        conn.setAutoCommit(true);
        conn.setAutoCommit(false);
        Assert.assertNotNull(conn);
    }

    @Test
    public void getAutoCommit() throws SQLException {
        Assert.assertTrue(conn.getAutoCommit());
    }

    @Test
    public void commit() throws SQLException {
        conn.commit();
        Assert.assertNotNull(conn);
    }

    @Test
    public void rollback() throws SQLException {
        conn.rollback();
        Assert.assertNotNull(conn);
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
        Assert.assertNotNull(conn);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testCreateStatement() throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("show cluster alive");
        rs.next();
        int status = rs.getInt("status");
        Assert.assertTrue(status > 0);

        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        Assert.assertTrue(status > 0);

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
        ResultSet rs = stmt.executeQuery("show cluster alive");
        rs.next();
        int status = rs.getInt("status");
        Assert.assertTrue(status > 0);

        conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrepareStatement1() throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("show cluster alive",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        int status = rs.getInt("status");
        Assert.assertTrue(status > 0);

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
        Assert.assertNotNull(conn);
    }

    @Test
    public void testSetClientInfo() throws SQLClientInfoException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        conn.setClientInfo(properties);
        Assert.assertNotNull(conn);
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
        Assert.assertNotNull(conn);
    }

    @Test
    public void getSchema() throws SQLException {
        Assert.assertNull(conn.getSchema());
    }

    @Test
    public void abort() throws SQLException {
        conn.abort(null);
        Assert.assertNotNull(conn);
    }

    @Test
    public void setNetworkTimeout() throws SQLException {
        conn.setNetworkTimeout(null, 1000);
        Assert.assertNotNull(conn);
    }

    @Test
    public void getNetworkTimeout() throws SQLException {
        int networkTimeout = conn.getNetworkTimeout();
        Assert.assertEquals(0, networkTimeout);
    }

    @Test
    public void unwrap() throws SQLException {
        TSDBConnection tsdbConnection = conn.unwrap(TSDBConnection.class);
        Assert.assertNotNull(tsdbConnection);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(conn.isWrapperFor(TSDBConnection.class));
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);
    }

}