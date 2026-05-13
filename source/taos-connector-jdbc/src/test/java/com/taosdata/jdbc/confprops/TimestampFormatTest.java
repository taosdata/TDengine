package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

public class TimestampFormatTest {
    private static final String DB_NAME = TestUtils.camelToSnake(TimestampFormatTest.class);

    static final String HOST = TestEnvUtil.getHost();
            private final long ts = Instant.now().toEpochMilli();
    private Connection conn;

    @Test
    public void utcInUrl() throws SQLException {
        // given
        String timestampFormat = "UTC";
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/" + DB_NAME +  "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&timestampFormat=" + timestampFormat;
        } else {
            url = url + "&timestampFormat=" + timestampFormat;
        }
        // when & then
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from weather");
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
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/" + DB_NAME +  "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        // when
        Properties props = new Properties();
        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {

            // then
            ResultSet rs = stmt.executeQuery("select * from weather");
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
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("create table weather(ts timestamp, temperature nchar(10))");
        stmt.execute("insert into weather values(" + ts + ", '北京')");
        stmt.close();
    }

    @After
    public void after() {
        try {
            if (null != conn) {
                Statement stmt = conn.createStatement();
                stmt.execute("drop database if exists " + DB_NAME);
                stmt.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}

