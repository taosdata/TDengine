package com.taosdata.jdbc.rs;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class WasNullTest {

    private static final String host = "127.0.0.1";
    private Connection conn;

    @Test
    public void testGetTimestamp() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(f1 timestamp, f2 timestamp, f3 int)");
            stmt.execute("insert into restful_test.weather values('2021-01-01 00:00:00.000', NULL, 100)");

            ResultSet rs = stmt.executeQuery("select * from restful_test.weather");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if (i == 2) {
                        Object value = rs.getTimestamp(i);
                        boolean wasNull = rs.wasNull();
                        Assert.assertNull(value);
                        Assert.assertTrue(wasNull);
                    } else {
                        Object value = rs.getObject(i);
                        boolean wasNull = rs.wasNull();
                        Assert.assertNotNull(value);
                        Assert.assertFalse(wasNull);
                    }
                }
            }
        }
    }

    @Test
    public void testGetObject() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 binary(64), f7 smallint, f8 tinyint, f9 bool, f10 nchar(64))");
            stmt.execute("insert into restful_test.weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, NULL, 10, 10, true, '涛思数据')");

            ResultSet rs = stmt.executeQuery("select * from restful_test.weather");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    Object value = rs.getObject(i);
                    boolean wasNull = rs.wasNull();
                    if (i == 6) {
                        Assert.assertNull(value);
                        Assert.assertTrue(wasNull);
                    } else {
                        Assert.assertNotNull(value);
                        Assert.assertFalse(wasNull);
                    }
                }
            }

        }
    }

    @Before
    public void before() throws SQLException {
        conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists restful_test");
            stmt.execute("create database if not exists restful_test");
            stmt.execute("use restful_test");
        }
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists restful_test");
            conn.close();
        }
    }
}
