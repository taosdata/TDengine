package com.taosdata.jdbc.cases;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.util.Random;

public class NanoSecondTimestampJNITest {

    private static final String host = "127.0.0.1";
    private static final String dbname = "nano_sec_test";
    private static final Random random = new Random(System.currentTimeMillis());
    private static Connection conn;

    @Test
    public void insertUsingLongValue() {
        // given
        long ms = System.currentTimeMillis();
        long ns = ms * 1000_000 + random.nextInt(1000_000);

        // when
        int ret = 0;
        try (Statement stmt = conn.createStatement()) {
            ret = stmt.executeUpdate("insert into weather(ts, temperature, humidity) values(" + ns + ", 12.3, 4)");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(1, ret);
    }

    @Test
    public void insertUsingStringValue() {
        // given

        // when
        int ret = 0;
        try (Statement stmt = conn.createStatement()) {
            ret = stmt.executeUpdate("insert into weather(ts, temperature, humidity) values('2021-01-01 12:00:00.123456789', 12.3, 4)");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(1, ret);
    }

    @Test
    public void insertUsingTimestampValue() {
        // given
        long epochSec = System.currentTimeMillis() / 1000;
        long nanoAdjustment = random.nextInt(1000_000_000);
        Timestamp ts = Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));

        // when
        int ret = 0;
        String sql = "insert into weather(ts, temperature, humidity) values( ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, ts);
            pstmt.setFloat(2, 12.34f);
            pstmt.setInt(3, 55);
            ret = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(1, ret);
    }

    @Test
    public void selectUsingLongValue() throws SQLException {
        // given
        long ms = System.currentTimeMillis();
        long ns = ms * 1000_000L + random.nextInt(1000_000);

        // when
        ResultSet rs = null;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into weather(ts, temperature, humidity) values(" + ns + ", 12.3, 4)");
            rs = stmt.executeQuery("select * from weather");
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        long actual = rs.getLong(1);
        Assert.assertEquals(ms, actual);
        actual = rs.getLong("ts");
        Assert.assertEquals(ms, actual);
    }

    @Test
    public void selectUsingStringValue() throws SQLException {
        // given
        String timestampStr = "2021-01-01 12:00:00.123456789";

        // when
        ResultSet rs = null;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into weather(ts, temperature, humidity) values('" + timestampStr + "', 12.3, 4)");
            rs = stmt.executeQuery("select * from weather");
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        String actual = rs.getString(1);
        Assert.assertEquals(timestampStr, actual);
        actual = rs.getString("ts");
        Assert.assertEquals(timestampStr, actual);
    }

    @Test
    public void selectUsingTimestampValue() throws SQLException {
        // given
        long timeMillis = System.currentTimeMillis();
        long epochSec = timeMillis / 1000;
        long nanoAdjustment = (timeMillis % 1000) * 1000_000L + random.nextInt(1000_000);
        Timestamp ts = Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));

        // insert one row
        String sql = "insert into weather(ts, temperature, humidity) values( ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, ts);
            pstmt.setFloat(2, 12.34f);
            pstmt.setInt(3, 55);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // when
        ResultSet rs = null;
        try (Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from weather");
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Timestamp actual = rs.getTimestamp(1);
        Assert.assertEquals(ts, actual);
        actual = rs.getTimestamp("ts");
        Assert.assertEquals(ts, actual);
        Assert.assertEquals(timeMillis, actual.getTime());
        Assert.assertEquals(nanoAdjustment, actual.getNanos());
    }

    @Before
    public void before() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather(ts timestamp, temperature float, humidity int)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision 'ns'");
            stmt.execute("use " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
