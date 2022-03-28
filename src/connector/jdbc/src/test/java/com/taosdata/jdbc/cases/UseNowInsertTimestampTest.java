package com.taosdata.jdbc.cases;

import org.junit.AfterClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UseNowInsertTimestampTest {
    private static String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

    @Test
    public void millisec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'ms'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            assertEquals(13, Long.toString(ts.getTime()).length());

            int nanos = ts.getNanos();
            assertEquals(0, nanos % 1000_000);

            stmt.execute("drop database if exists test");
        }
    }

    @Test
    public void microsec() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'us'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            int nanos = ts.getNanos();

            assertEquals(0, nanos % 1000);

            stmt.execute("drop database if exists test");
        }
    }

    @Test
    public void nanosec() throws SQLException {
        long now_time = System.currentTimeMillis() * 1000_000L + System.nanoTime() % 1000_000L;
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'ns'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(" + now_time + ", 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Timestamp ts = rs.getTimestamp("ts");

            int nanos = ts.getNanos();
            assertTrue(nanos % 1000 != 0);

            stmt.execute("drop database if exists test");
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
