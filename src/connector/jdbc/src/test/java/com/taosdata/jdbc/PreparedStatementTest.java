package com.taosdata.jdbc;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class PreparedStatementTest {
    static Connection connection;
    static TSDBPreparedStatement statement;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";

    @BeforeClass
    public static void createConnection() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
        String sql = "drop database if exists " + dbName;
        statement = (TSDBPreparedStatement) connection.prepareStatement(sql);
    }

    @Test
    public void case001_createTableAndQuery() throws SQLException {
        long ts = System.currentTimeMillis();

        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + "(ts timestamp, k1 int)");
        statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", 1)");

        PreparedStatement selectStatement = connection.prepareStatement("select * from " + dbName + "." + tName);
        ResultSet resultSet = selectStatement.executeQuery();
        assertTrue(null != resultSet);

        boolean isClosed = statement.isClosed();
        assertEquals(false, isClosed);
        selectStatement.close();
    }

    @Test
    public void case002_testPreparedStatement() throws SQLException {
        long ts = System.currentTimeMillis() + 20000;

        PreparedStatement saveStatement = connection.prepareStatement("insert into " + dbName + "." + tName + " values (" + ts + ", 1)");
        int affectedRows = saveStatement.executeUpdate();
        assertTrue(1 == affectedRows);
        saveStatement.close();
    }

    @Test
    public void case003_testSavedPreparedStatement() throws SQLException {
        long ts = System.currentTimeMillis();
        TSDBPreparedStatement saveStatement = (TSDBPreparedStatement) connection.prepareStatement("insert into  " + dbName + "." + tName + " values (?, ?)");
        saveStatement.setObject(1, ts + 10000);
        saveStatement.setObject(2, 3);
        int rows = saveStatement.executeUpdate();
        assertEquals(1, rows);
        saveStatement.close();
    }

    @Test
    public void case004_testUnsupport() throws SQLException {

        Assert.assertNotNull(statement.unwrap(TSDBPreparedStatement.class));
        Assert.assertTrue(statement.isWrapperFor(TSDBPreparedStatement.class));

        try {
            statement.getMaxFieldSize();
        } catch (SQLException e) {
        }
        try {
            statement.setMaxFieldSize(0);
        } catch (SQLException e) {
        }
        try {
            statement.setEscapeProcessing(true);
        } catch (SQLException e) {
        }
        try {
            statement.cancel();
        } catch (SQLException e) {
        }
        try {
            statement.getWarnings();
        } catch (SQLException e) {
        }
        try {
            statement.clearWarnings();
        } catch (SQLException e) {
        }
        try {
            statement.setCursorName(null);
        } catch (SQLException e) {
        }
        try {
            statement.getMoreResults();
        } catch (SQLException e) {
        }
        try {
            statement.setFetchDirection(0);
        } catch (SQLException e) {
        }
        try {
            statement.getFetchDirection();
        } catch (SQLException e) {
        }
        try {
            statement.getResultSetConcurrency();
        } catch (SQLException e) {
        }
        try {
            statement.getResultSetType();
        } catch (SQLException e) {
        }
        try {
            statement.getConnection();
        } catch (SQLException e) {
        }
        try {
            statement.getMoreResults();
        } catch (SQLException e) {
        }
        try {
            statement.getGeneratedKeys();
        } catch (SQLException e) {
        }
        try {
            statement.executeUpdate(null, 0);
        } catch (SQLException e) {
        }
        try {
            statement.executeUpdate(null, new int[]{0});
        } catch (SQLException e) {
        }
        try {
            statement.executeUpdate(null, new String[]{"str1", "str2"});
        } catch (SQLException e) {
        }
        try {
            statement.getResultSetHoldability();
        } catch (SQLException e) {
        }
        try {
            statement.setPoolable(true);
        } catch (SQLException e) {
        }
        try {
            statement.isPoolable();
        } catch (SQLException e) {
        }
        try {
            statement.closeOnCompletion();
        } catch (SQLException e) {

        }
        try {
            statement.isCloseOnCompletion();
        } catch (SQLException e) {
        }
    }

    @AfterClass
    public static void close() throws Exception {
        statement.executeUpdate("drop database " + dbName);
        statement.close();
        connection.close();
        Thread.sleep(10);

    }

}
