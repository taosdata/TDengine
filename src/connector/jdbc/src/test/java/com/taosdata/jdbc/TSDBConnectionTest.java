package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class TSDBConnectionTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;

    @Test
    public void getConnection() {
    }

    @Test
    public void createStatement() {
    }

    @Test
    public void subscribe() {
    }

    @Test
    public void prepareStatement() {
    }

    @Test
    public void prepareCall() {
    }

    @Test
    public void nativeSQL() {
    }

    @Test
    public void setAutoCommit() {
    }

    @Test
    public void getAutoCommit() {
    }

    @Test
    public void commit() {
    }

    @Test
    public void rollback() {
    }

    @Test
    public void close() {
    }

    @Test
    public void isClosed() {
    }

    @Test
    public void getMetaData() {
    }

    @Test
    public void setReadOnly() {
    }

    @Test
    public void isReadOnly() {
    }

    @Test
    public void setCatalog() {
    }

    @Test
    public void getCatalog() {
    }

    @Test
    public void setTransactionIsolation() {
    }

    @Test
    public void getTransactionIsolation() {
    }

    @Test
    public void getWarnings() {
    }

    @Test
    public void clearWarnings() {
    }

    @Test
    public void testCreateStatement() {
    }

    @Test
    public void testPrepareStatement() {
    }

    @Test
    public void getBatchFetch() {
    }

    @Test
    public void setBatchFetch() {
    }

    @Test
    public void testPrepareCall() {
    }

    @Test
    public void getTypeMap() {
    }

    @Test
    public void setTypeMap() {
    }

    @Test
    public void setHoldability() {
    }

    @Test
    public void getHoldability() {
    }

    @Test
    public void setSavepoint() {
    }

    @Test
    public void testSetSavepoint() {
    }

    @Test
    public void testRollback() {
    }

    @Test
    public void releaseSavepoint() {
    }

    @Test
    public void testCreateStatement1() {
    }

    @Test
    public void testPrepareStatement1() {
    }

    @Test
    public void testPrepareCall1() {
    }

    @Test
    public void testPrepareStatement2() {
    }

    @Test
    public void testPrepareStatement3() {
    }

    @Test
    public void testPrepareStatement4() {
    }

    @Test
    public void createClob() {
    }

    @Test
    public void createBlob() {
    }

    @Test
    public void createNClob() {
    }

    @Test
    public void createSQLXML() {
    }

    @Test
    public void isValid() {
    }

    @Test
    public void setClientInfo() {
    }

    @Test
    public void testSetClientInfo() {
    }

    @Test
    public void getClientInfo() {
    }

    @Test
    public void testGetClientInfo() {
    }

    @Test
    public void createArrayOf() {
    }

    @Test
    public void createStruct() {
    }

    @Test
    public void setSchema() {
    }

    @Test
    public void getSchema() {
    }

    @Test
    public void abort() {
    }

    @Test
    public void setNetworkTimeout() {
    }

    @Test
    public void getNetworkTimeout() {
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
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata", properties);
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