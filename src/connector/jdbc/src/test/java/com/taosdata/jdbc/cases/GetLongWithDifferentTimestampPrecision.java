package com.taosdata.jdbc.cases;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;

public class GetLongWithDifferentTimestampPrecision {

    private final String host = "127.0.0.1";

    @Test
    public void testRestful() throws SQLException {
        // given
        String url = "jdbc:TAOS-RS://" + host + ":6041/";
        Connection conn = DriverManager.getConnection(url, "root", "taosdata");
        long ts = System.currentTimeMillis();

        // when and then
        assertResultSet(conn, "ms", ts, ts);
        assertResultSet(conn, "us", ts, ts * 1000);
        assertResultSet(conn, "ns", ts, ts * 1000_000);
    }

    @Test
    public void testJni() throws SQLException {
        // given
        String url = "jdbc:TAOS://" + host + ":6030/";
        Connection conn = DriverManager.getConnection(url, "root", "taosdata");
        long ts = System.currentTimeMillis();

        // when and then
        assertResultSet(conn, "ms", ts, ts);
        assertResultSet(conn, "us", ts, ts * 1000);
        assertResultSet(conn, "ns", ts, ts * 1000_000);
    }

    private void assertResultSet(Connection conn, String precision, long timestamp, long expect) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision '" + precision + "'");
            stmt.execute("create table test.weather(ts timestamp, f1 int)");

            String dateTimeStr = sdf.format(new Date(timestamp));
            stmt.execute("insert into test.weather values('" + dateTimeStr + "', 1)");

            ResultSet rs = stmt.executeQuery("select * from test.weather");
            rs.next();
            long actual = rs.getLong("ts");
            Assert.assertEquals(expect, actual);
            stmt.execute("drop database if exists test");
        }
    }


}
