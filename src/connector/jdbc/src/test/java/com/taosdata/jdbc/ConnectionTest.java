package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class ConnectionTest {
    private Connection connection;
    private Statement statement;
    private static String host = "127.0.0.1";

    @Test
    public void testConnection() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
            Assert.assertTrue(null != connection);
            statement = connection.createStatement();
            Assert.assertTrue(null != statement);
            statement.close();
            connection.close();
        } catch (ClassNotFoundException e) {
            return;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
