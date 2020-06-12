package com.taosdata.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PreparedStatementTest {
    static Connection connection = null;
    static PreparedStatement statement = null;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";
    static ResultSet resSet = null;

    @BeforeClass
    public static void createConnection() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);

        String sql = "drop database if exists " + dbName;
        statement = (TSDBPreparedStatement) connection.prepareStatement(sql);

    }

    @Test
    public void createTableAndQuery() throws SQLException {
        long ts = System.currentTimeMillis();

        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + "(ts timestamp, k1 int)");
        statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", 1)");

        PreparedStatement selectStatement = connection.prepareStatement("select * from " + dbName + "." + tName);

        ResultSet resultSet = selectStatement.executeQuery();
        assertTrue(null != resultSet);

        boolean isClosed = statement.isClosed();
        assertEquals(false, isClosed);
    }

    @Test
    public void testUnsupport() {
//        if(null == resSet) {
//            return;
//        }
        TSDBPreparedStatement tsdbStatement = (TSDBPreparedStatement) statement;
        try {
            tsdbStatement.unwrap(null);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.isWrapperFor(null);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getMaxFieldSize();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.setMaxFieldSize(0);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.setEscapeProcessing(true);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.cancel();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getWarnings();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.clearWarnings();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.setCursorName(null);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getMoreResults();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.setFetchDirection(0);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getFetchDirection();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getResultSetConcurrency();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getResultSetType();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getConnection();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getMoreResults();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getGeneratedKeys();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.executeUpdate(null, 0);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.executeUpdate(null, new int[]{0});
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.executeUpdate(null, new String[]{"str1", "str2"});
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.getResultSetHoldability();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.setPoolable(true);
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.isPoolable();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.closeOnCompletion();
        } catch (SQLException e) {
        }
        try {
            tsdbStatement.isCloseOnCompletion();
        } catch (SQLException e) {
        }
    }
}
