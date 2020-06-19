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
    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String stbName = "st";
    static String host = "localhost";

    @BeforeClass
    public static void createDatabase() throws SQLException {
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
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);

        statement = connection.createStatement();
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeQuery("use " + dbName);
    }

//    @Test
    public void createStable() {
        String sql = "create table " + stbName + " (ts timestamp, v1 int, v2 int) tags (tg nchar(20)) ";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            assert false : "error create stable" + e.getMessage();
        }
    }

//    @Test
    public void createTable() {
        String sql = "create table t1 using " + stbName + " tags (\"beijing\")";

        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            assert false : "error create table" + e.getMessage();
        }
    }

    @Test
    public void describeSTable() {
        createStable();
        String sql = "describe " + stbName;
        int num = 0;
        System.out.println("describe stable");
        try {
            ResultSet res = statement.executeQuery(sql);
            while (res.next()) {
                for (int i = 1; i <= res.getMetaData().getColumnCount(); i++) {
                    System.out.printf("%d: %s\n", i, res.getString(i));
                }
                num++;
            }
            res.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    @Test
    public void describeTable() {
        createTable();
        String sql = "describe t1";
        int num = 0;
        System.out.println("describe table");
        try {
            ResultSet res = statement.executeQuery(sql);
            while (res.next()) {
                for (int i = 1; i <= res.getMetaData().getColumnCount(); i++) {
                    System.out.printf("%d: %s\n", i, res.getString(i));
                }
                num++;
            }
            res.close();
            assertEquals(4, num);
        } catch (SQLException e) {
            assert false : "error describe stable" + e.getMessage();
        }
    }

    //    @Test
    public void validCreateSql() {
        String sql = "create table t2 using " + stbName + " tags (\"beijing\")";
        boolean valid = ((TSDBConnection) connection).getConnection().validateCreateTableSql(sql);
        assertEquals(true, valid);
    }

    @AfterClass
    public static void close() throws Exception {
        if (!statement.isClosed()) {
            statement.executeUpdate("drop database " + dbName);
            statement.close();
            connection.close();
            Thread.sleep(10);

        }
    }
}
