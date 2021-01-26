package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StatementTest {
    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";

    @BeforeClass
    public static void createConnection() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata", properties);
            statement = connection.createStatement();
            statement.executeUpdate("drop database if exists " + dbName);

        } catch (ClassNotFoundException e) {
            return;
        }
    }

    @Test
    public void testCase() {
        try {
            ResultSet rs = statement.executeQuery("show databases");
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    System.out.print(metaData.getColumnLabel(i) + ":" + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTableAndQuery() throws SQLException {
        long ts = System.currentTimeMillis();

        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + "(ts timestamp, k1 int)");
        statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", 1)");
        statement.executeQuery("select * from " + dbName + "." + tName);
        ResultSet resultSet = statement.getResultSet();
        assertTrue(null != resultSet);

        boolean isClosed = statement.isClosed();
        assertEquals(false, isClosed);
    }

    @Test
    public void testUnsupport() {
        try {
            Assert.assertNotNull(statement.unwrap(TSDBStatement.class));
            Assert.assertTrue(statement.isWrapperFor(TSDBStatement.class));

            statement.getMaxFieldSize();
            statement.setMaxFieldSize(0);
            statement.setEscapeProcessing(true);
            statement.cancel();
            statement.getWarnings();
            statement.clearWarnings();
            statement.setCursorName(null);
            statement.getMoreResults();
            statement.setFetchDirection(0);
            statement.getFetchDirection();
            statement.getResultSetConcurrency();
            statement.getResultSetType();
            statement.getConnection();
            statement.getMoreResults();
            statement.getGeneratedKeys();
            statement.executeUpdate(null, 0);
            statement.executeUpdate(null, new int[]{0});
            statement.executeUpdate(null, new String[]{"str1", "str2"});
            statement.getResultSetHoldability();
            statement.setPoolable(true);
            statement.isPoolable();
            statement.closeOnCompletion();
            statement.isCloseOnCompletion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void close() {
        try {
            statement.execute("drop database if exists " + dbName);
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
