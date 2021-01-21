package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StableTest {

    private static Connection connection;
    private static String dbName = "test";
    private static String stbName = "st";
    private static String host = "127.0.0.1";

    @BeforeClass
    public static void createDatabase() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
            Statement statement = connection.createStatement();
            statement.execute("create database if not exists " + dbName);
            statement.execute("use " + dbName);
            statement.close();
        } catch (ClassNotFoundException e) {
            return;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void case001_createSuperTable() {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "create table " + stbName + " (ts timestamp, v1 int, v2 int) tags (tg nchar(20)) ";
            stmt.execute(sql);
        } catch (SQLException e) {
            assert false : "error create stable" + e.getMessage();
        }
    }

    @Test
    public void case002_createTable() {
        try (Statement stmt = connection.createStatement()) {
            final String sql = "create table t1 using " + stbName + " tags (\"beijing\")";
            stmt.execute(sql);
        } catch (SQLException e) {
            assert false : "error create table" + e.getMessage();
        }
    }

    @Test
    public void case003_describeSTable() {
        int num = 0;
        try (Statement stmt = connection.createStatement()) {
            String sql = "describe " + stbName;
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.println(i + ":" + rs.getString(i));
                }
                num++;
            }
            rs.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    @Test
    public void case004_describeTable() {
        int num = 0;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("describe t1");
            while (rs.next()) {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.printf("%d: %s\n", i, rs.getString(i));
                }
                num++;
            }
            rs.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    @AfterClass
    public static void close() {
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
