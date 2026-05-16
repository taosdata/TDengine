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

public class MicroSecondPrecisionJNITest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String MS_TIMESTAMP_DB = "ms_precision_test";
    private static final String US_TIMESTAMP_DB = "us_precision_test";
    private static final long TIMESTAMP_1 = System.currentTimeMillis();
    private static final long TIMESTAMP_2 = TIMESTAMP_1 * 1000 + 123;

    private static Connection conn;

    @Test
    public void testCase1() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + MS_TIMESTAMP_DB + ".weather");
            rs.next();
            long ts = rs.getTimestamp(1).getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_1, ts);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCase2() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select last_row(ts) from " + US_TIMESTAMP_DB + ".weather");
            rs.next();

            Timestamp timestamp = rs.getTimestamp(1);
            long ts = timestamp.getTime();
            Assert.assertEquals(TIMESTAMP_1, ts);
            int nanos = timestamp.getNanos();
            Assert.assertEquals(TIMESTAMP_2 % 1000_000L * 1000, nanos);

            ts = rs.getLong(1);
            Assert.assertEquals(TIMESTAMP_2, ts);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);

        Statement stmt = conn.createStatement();
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
            if (conn != null) {
                Statement statement = conn.createStatement();
                statement.execute("drop database " + MS_TIMESTAMP_DB);
                statement.execute("drop database " + US_TIMESTAMP_DB);
                statement.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

