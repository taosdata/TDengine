package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class ConnectionTest extends BaseTest {
    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String stbName = "st";
    static String host = "localhost";

    @Test
    public void testConnection() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER,"root");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD,"taosdata");

        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

        assertTrue(null != connection);
        statement = connection.createStatement();
        assertTrue(null != statement);

        // try reconnect
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

        try {
            statement.execute("create database if not exists " + dbName);
        } catch (SQLException e) {
            assert false : "create database error: " + e.getMessage();
        }

        try {
            if (!connection.isClosed()) {
                if (!statement.isClosed()) {
                    statement.executeUpdate("drop database " + dbName);
                    statement.close();
                }
                connection.close();
                Thread.sleep(10);
            }
        } catch (Exception e) {
            assert false : "close connection error: " + e.getMessage();
        }
    }
}
