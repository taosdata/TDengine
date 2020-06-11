package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class ConnectionTest {
    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String stbName = "st";
    static String host = "localhost";

    @Test
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

        assertTrue(null != connection);
    }

    @Test
    public void createDatabase() {
        try {
            statement.executeUpdate("create database if not exists " + dbName);
        } catch (SQLException e) {
            assert false : "create database error: " + e.getMessage();
        }
    }

    @Test
    public void close() {
        try {
            if (!statement.isClosed()) {
                statement.executeUpdate("drop database " + dbName);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            assert false : "close connection error: " + e.getMessage();
        }
    }
}
