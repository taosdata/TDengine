package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ImportTest {
    Connection connection;
    String dbName = "test";
    String tName = "t0";
    String host = "127.0.0.1";
    private static long ts;

    @Before
    public void createDatabase() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists " + dbName);
            stmt.executeUpdate("create database if not exists " + dbName);
            stmt.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
            ts = System.currentTimeMillis();
            stmt.close();
        } catch (ClassNotFoundException e) {
            return;
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void case001_insertData() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < 50; i++) {
                ts++;
                int row = stmt.executeUpdate("import into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")");
                System.out.println("import into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")\t" + row);
                assertEquals(1, row);
            }
        }
    }

    @Test
    public void case002_checkSum() {
        Assert.assertEquals(50, select());
    }

    private int select() {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {

            String sql = "select * from " + dbName + "." + tName;
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.printf(i + ": " + rs.getString(i) + "\t");
                }
                count++;
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    @Test
    public void case003_importData() {
        // 避免时间重复
        try (Statement stmt = connection.createStatement()) {
            StringBuilder sqlBuilder = new StringBuilder("import into ").append(dbName).append(".").append(tName).append(" values ");
            for (int i = 0; i < 50; i++) {
                int a = i / 5;
                long t = ts + a;
                sqlBuilder.append("(").append(t).append(",").append((100 + i)).append(",").append(i).append(") ");
            }
            System.out.println(sqlBuilder.toString());
            int rows = stmt.executeUpdate(sqlBuilder.toString());
            assertEquals(10, rows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void case004_checkSum() {
        Assert.assertEquals(100, select());
    }

    @After
    public void close() {
        try {
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.executeUpdate("drop database " + dbName);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
