package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class MicroSecondPrecisionRestfulTest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String MS_TIMESTAMP_DB = "ms_precision_test1";
    private static final String US_TIMESTAMP_DB = "us_precision_test1";
    private static final long TIMESTAMP_1 = System.currentTimeMillis();
    private static final long TIMESTAMP_2 = TIMESTAMP_1 * 1000 + 123;

    private static Connection conn1;
    private static Connection conn2;
    private static Connection conn3;

    @Test
    public void testCase1() throws SQLException {
        try (Statement stmt = conn1.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + MS_TIMESTAMP_DB + ".weather");
            rs.next();
            long ts = rs.getTimestamp(1).getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_1, ts);
        }
    }

    @Test
    public void testCase2() throws SQLException {
        try (Statement stmt = conn1.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + US_TIMESTAMP_DB + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(TIMESTAMP_2 % 1000_000L * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_2, ts);
        }
    }

    @Test
    public void testCase3() throws SQLException {
        try (Statement stmt = conn2.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + MS_TIMESTAMP_DB + ".weather");
            rs.next();
            Timestamp rsTimestamp = rs.getTimestamp(1);
            long ts = rsTimestamp.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_1, ts);
        }
    }

    @Test
    public void testCase4() throws SQLException {
        try (Statement stmt = conn2.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + US_TIMESTAMP_DB + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(TIMESTAMP_2 % 1000_000L * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_2, ts);
        }
    }

    @Test
    public void testCase5() throws SQLException {
        try (Statement stmt = conn3.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + MS_TIMESTAMP_DB + ".weather");
            rs.next();
            Timestamp actual = rs.getTimestamp(1);
            long ts = actual == null ? 0 : actual.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_1, ts);
        }
    }

    @Test
    public void testCase6() throws SQLException {
        try (Statement stmt = conn3.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + US_TIMESTAMP_DB + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp == null ? 0 : timestamp.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(TIMESTAMP_2 % 1000_000L * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_2, ts);
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn1 = DriverManager.getConnection(url, properties);

        String url1 = url + "&timestampFormat=timestamp";
        conn2 = DriverManager.getConnection(url1, properties);

        String url2 = url + "&timestampFormat=utc";
        conn3 = DriverManager.getConnection(url2, properties);

        Statement stmt = conn1.createStatement();
        stmt.execute("drop database if exists " + MS_TIMESTAMP_DB);
        stmt.execute("create database if not exists " + MS_TIMESTAMP_DB + " precision 'ms'");
        stmt.execute("create table " + MS_TIMESTAMP_DB + ".weather(ts timestamp, f1 int)");
        stmt.executeUpdate("insert into " + MS_TIMESTAMP_DB + ".weather(ts,f1) values(" + TIMESTAMP_1 + ", 127)");

        stmt.execute("drop database if exists " + US_TIMESTAMP_DB);
        stmt.execute("create database if not exists " + US_TIMESTAMP_DB + " precision 'us'");
        stmt.execute("create table " + US_TIMESTAMP_DB + ".weather(ts timestamp, f1 int)");
        stmt.executeUpdate("insert into " + US_TIMESTAMP_DB + ".weather(ts,f1) values(" + TIMESTAMP_2 + ", 127)");
        stmt.close();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn1 != null) {
                Statement statement = conn1.createStatement();
                statement.execute("drop database if exists " + MS_TIMESTAMP_DB);
                statement.execute("drop database if exists " + US_TIMESTAMP_DB);
                statement.close();
                conn1.close();
            }
            if (conn2 != null)
                conn2.close();
            if (conn3 != null)
                conn3.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }
}

