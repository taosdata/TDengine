package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SelectTest extends BaseTest {
    Connection connection = null;
    Statement statement = null;
    String dbName = "test";
    String tName = "t0";
    String host = "localhost";

    @Before
    public void createDatabaseAndTable() throws SQLException {
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

        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" , properties);

        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");

    }

    @Test
    public void selectData() throws SQLException {
        long ts = 1496732686000l;

        for (int i = 0; i < 50; i++) {
            ts++;
            int row = statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")");
            System.out.println("insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")\t" + row);
            assertEquals(1, row);
        }

        String sql = "select * from " + dbName + "." + tName;
        ResultSet resSet = statement.executeQuery(sql);

        int num = 0;
        while (resSet.next()) {
            num++;
        }
        resSet.close();

        assertEquals(num, 50);
    }

    @After
    public void close() throws Exception {
        statement.executeUpdate("drop database " + dbName);
        statement.close();
        connection.close();
        Thread.sleep(10);

    }
}
