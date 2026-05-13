package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class WasNullTest {

            private Connection conn;

    static final String HOST = TestEnvUtil.getHost();
    private final String dbName = TestUtils.camelToSnake(WasNullTest.class);

    @Test
    public void testGetTimestamp() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(f1 timestamp, f2 timestamp, f3 int)");
            stmt.execute("insert into " + dbName + ".weather values('2021-01-01 00:00:00.000', NULL, 100)");

            ResultSet rs = stmt.executeQuery("select * from " + dbName + ".weather");
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
            stmt.execute("insert into " + dbName + ".weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, NULL, 10, 10, true, '涛思数据')");

            ResultSet rs = stmt.executeQuery("select * from " + dbName + ".weather");
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
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("use " + dbName);
        }
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists " + dbName);
            conn.close();
        }
    }
}

