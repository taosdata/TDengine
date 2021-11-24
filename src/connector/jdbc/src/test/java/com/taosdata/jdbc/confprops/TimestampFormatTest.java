package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

public class TimestampFormatTest {
    private static final String host = "127.0.0.1";
    private long ts = Instant.now().toEpochMilli();

    @Test
    public void string() throws SQLException {
        // given
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";

        // when
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            // then
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals("STRING", actual);

            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                String value = rs.getString("ts");
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value);
            }
        }
    }

    @Test
    public void stringInProperties() throws SQLException {
        // given
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";

        // when
        String timestampFormat = "STRING";
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, timestampFormat);
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            // then
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals(timestampFormat, actual);

            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                String value = rs.getString("ts");
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value);
            }
        }
    }

    @Test
    public void timestampInUrl() throws SQLException {
        // given
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&timestampFormat=";
        String timestampFormat = "TIMESTAMP";

        // when
        try (Connection conn = DriverManager.getConnection(url + timestampFormat);
             Statement stmt = conn.createStatement()) {
            // then
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals(timestampFormat, actual);

            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Test
    public void timestampInProperties() throws SQLException {
        // given
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        String timestampFormat = "TIMESTAMP";

        // when
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, timestampFormat);
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {
            // then
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals(timestampFormat, actual);

            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Test
    public void utcInUrl() throws SQLException {
        // given
        String timestampFormat = "UTC";
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata&timestampFormat=" + timestampFormat;

        // when & then
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals(timestampFormat, actual);
            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                Assert.assertTrue(value instanceof Timestamp);
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Test
    public void utcInProperties() throws SQLException {
        // given
        String timestampFormat = "UTC";
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";

        // when
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT, timestampFormat);
        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            // then
            String actual = conn.getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
            Assert.assertEquals(timestampFormat, actual);
            ResultSet rs = stmt.executeQuery("select * from test.weather");
            while (rs.next()) {
                Object value = rs.getObject("ts");
                Assert.assertTrue(value instanceof Timestamp);
                String expect = new Timestamp(ts).toString();
                Assert.assertEquals(expect, value.toString());
            }
        }
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, temperature nchar(10))");
            stmt.execute("insert into weather values(" + ts + ", '北京')");
        }
    }

}
