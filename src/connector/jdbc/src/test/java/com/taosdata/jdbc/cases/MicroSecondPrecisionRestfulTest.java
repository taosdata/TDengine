package com.taosdata.jdbc.cases;


import com.taosdata.jdbc.TSDBDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class MicroSecondPrecisionRestfulTest {

    private static final String host = "127.0.0.1";
    private static final String ms_timestamp_db = "ms_precision_test";
    private static final String us_timestamp_db = "us_precision_test";
    private static final long timestamp1 = System.currentTimeMillis();
    private static final long timestamp2 = timestamp1 * 1000 + 123;

    private static Connection conn1;
    private static Connection conn2;
    private static Connection conn3;

    @Test
    public void testCase1() throws SQLException {
        try (Statement stmt = conn1.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + ms_timestamp_db + ".weather");
            rs.next();
            long ts = rs.getTimestamp(1).getTime();
            Assert.assertEquals(timestamp1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @Test
    public void testCase2() throws SQLException {
        try (Statement stmt = conn1.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + us_timestamp_db + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp.getTime();
            Assert.assertEquals(timestamp1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(timestamp2 % 1000_000l * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @Test
    public void testCase3() throws SQLException {
        try (Statement stmt = conn2.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + ms_timestamp_db + ".weather");
            rs.next();
            Timestamp rsTimestamp = rs.getTimestamp(1);
            long ts = rsTimestamp.getTime();
            Assert.assertEquals(timestamp1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @Test
    public void testCase4() throws SQLException {
        try (Statement stmt = conn2.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + us_timestamp_db + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp.getTime();
            Assert.assertEquals(timestamp1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(timestamp2 % 1000_000l * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @Test
    public void testCase5() throws SQLException {
        try (Statement stmt = conn3.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + ms_timestamp_db + ".weather");
            rs.next();
            Timestamp actual = rs.getTimestamp(1);
            long ts = actual == null ? 0 : actual.getTime();
            Assert.assertEquals(timestamp1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @Test
    public void testCase6() throws SQLException {
        try (Statement stmt = conn3.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + us_timestamp_db + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp == null ? 0 : timestamp.getTime();
            Assert.assertEquals(timestamp1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(timestamp2 % 1000_000l * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(timestamp1, ts);
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
//        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, "TIMESTAMP");

        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        conn1 = DriverManager.getConnection(url, properties);

        url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&timestampFormat=timestamp";
        conn2 = DriverManager.getConnection(url, properties);

        url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&timestampFormat=utc";
        conn3 = DriverManager.getConnection(url, properties);

        Statement stmt = conn1.createStatement();
        stmt.execute("drop database if exists " + ms_timestamp_db);
        stmt.execute("create database if not exists " + ms_timestamp_db + " precision 'ms'");
        stmt.execute("create table " + ms_timestamp_db + ".weather(ts timestamp, f1 int)");
        stmt.executeUpdate("insert into " + ms_timestamp_db + ".weather(ts,f1) values(" + timestamp1 + ", 127)");

        stmt.execute("drop database if exists " + us_timestamp_db);
        stmt.execute("create database if not exists " + us_timestamp_db + " precision 'us'");
        stmt.execute("create table " + us_timestamp_db + ".weather(ts timestamp, f1 int)");
        stmt.executeUpdate("insert into " + us_timestamp_db + ".weather(ts,f1) values(" + timestamp2 + ", 127)");
        stmt.close();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn1 != null)
            conn1.close();
        if (conn2 != null)
            conn2.close();
        if (conn3 != null)
            conn3.close();
    }
}
