package com.taosdata.jdbc.cases;

import org.junit.*;

import java.sql.*;
import java.time.Instant;
import java.util.Random;

public class NanoSecondTimestampRestfulTest {

    private static final String host = "127.0.0.1";
    private static final String dbname = "nano_sec_test";
    private static final Random random = new Random(System.currentTimeMillis());
    private static Connection conn;

    @Test
    public void insertUsingLongValue() throws SQLException {
        // given
        long ms = System.currentTimeMillis();
        long ns = ms * 1000_000 + random.nextInt(1000_000);

        // when
        int ret = 0;
        try (Statement stmt = conn.createStatement()) {
            ret = stmt.executeUpdate("insert into weather(ts, temperature, humidity) values(" + ns + ", 12.3, 4)");
        }

        // then
        Assert.assertEquals(1, ret);
    }

    @Test
    public void insertUsingStringValue() throws SQLException {
        // given

        // when
        int ret = 0;
        try (Statement stmt = conn.createStatement()) {
            ret = stmt.executeUpdate("insert into weather(ts, temperature, humidity) values('2021-01-01 12:00:00.123456789', 12.3, 4)");
        }

        // then
        Assert.assertEquals(1, ret);
    }

    @Test
    public void insertUsingTimestampValue() throws SQLException {
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
        ResultSet rs;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into weather(ts, temperature, humidity) values(" + ns + ", 12.3, 4)");
            rs = stmt.executeQuery("select * from weather");
            rs.next();
        }

        // then
        long actual = rs.getLong(1);
        Assert.assertEquals(ns, actual);
        actual = rs.getLong("ts");
        Assert.assertEquals(ns, actual);
    }

    @Test
    public void selectUsingStringValue() throws SQLException {
        // given
        String timestampStr = "2021-01-01 12:00:00.123456789";

        // when
        ResultSet rs;
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into weather(ts, temperature, humidity) values('" + timestampStr + "', 12.3, 4)");
            rs = stmt.executeQuery("select * from weather");
            rs.next();
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
        }

        // when
        ResultSet rs = null;
        try (Statement stmt = conn.createStatement()) {
            rs = stmt.executeQuery("select * from weather");
            rs.next();
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
    public void before() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather(ts timestamp, temperature float, humidity int)");
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision 'ns'");
            stmt.execute("use " + dbname);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null){
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists " + dbname);
            }
            conn.close();
        }
    }

}
