package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SelectTest {
    Connection connection;
    String dbName = "test";
    String tName = "t0";
    String host = "127.0.0.1";

    @Before
    public void createDatabaseAndTable() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
            stmt.close();
        } catch (ClassNotFoundException e) {
            return;
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void selectData() {
        long ts = 1496732686000l;

        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < 50; i++) {
                ts++;
                int row = stmt.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")");
                System.out.println("insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")\t" + row);
                assertEquals(1, row);
            }

            String sql = "select * from " + dbName + "." + tName;
            ResultSet resSet = stmt.executeQuery(sql);

            int num = 0;
            while (resSet.next()) {
                num++;
            }
            resSet.close();
            assertEquals(num, 50);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @After
    public void close() {
        try {
            if (connection != null) {
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("drop database " + dbName);
                stmt.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
