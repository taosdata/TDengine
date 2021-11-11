package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedNumberJniTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static long ts;

    @Test
    public void testCase001() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from us_table");
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);
            while (rs.next()) {
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals("127", rs.getString(2));
                Assert.assertEquals("32767", rs.getString(3));
                Assert.assertEquals("2147483647", rs.getString(4));
                Assert.assertEquals("9223372036854775807", rs.getString(5));
            }
        }
    }

    @Test
    public void testCase002() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from us_table");
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);

            while (rs.next()) {
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(127, rs.getByte(2));
                Assert.assertEquals(32767, rs.getShort(3));
                Assert.assertEquals(2147483647, rs.getInt(4));
                Assert.assertEquals(9223372036854775807L, rs.getLong(5));
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testCase003() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 127, 32767,2147483647, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);
            while (rs.next()) {
                Assert.assertEquals(now, rs.getTimestamp(1).getTime());
                Assert.assertEquals(127, rs.getByte(2));
                Assert.assertEquals(32767, rs.getShort(3));
                Assert.assertEquals(2147483647, rs.getInt(4));
                Assert.assertEquals("18446744073709551614", rs.getString(5));
                rs.getLong(5);
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testCase004() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 127, 32767,4294967294, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);

            while (rs.next()) {
                Assert.assertEquals(now, rs.getTimestamp(1).getTime());
                Assert.assertEquals(127, rs.getByte(2));
                Assert.assertEquals(32767, rs.getShort(3));
                Assert.assertEquals("4294967294", rs.getString(4));
                Assert.assertEquals("18446744073709551614", rs.getString(5));
                rs.getInt(4);
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testCase005() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 127, 65534,4294967294, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);

            while (rs.next()) {
                Assert.assertEquals(now, rs.getTimestamp(1).getTime());
                Assert.assertEquals(127, rs.getByte(2));
                Assert.assertEquals("65534", rs.getString(3));
                Assert.assertEquals("4294967294", rs.getString(4));
                Assert.assertEquals("18446744073709551614", rs.getString(5));
                rs.getShort(3);
            }
        }
    }

    @Test(expected = SQLException.class)
    public void testCase006() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 254, 65534,4294967294, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            assertResultSetMetaData(meta);

            while (rs.next()) {
                Assert.assertEquals(now, rs.getTimestamp(1).getTime());
                Assert.assertEquals("254", rs.getString(2));
                Assert.assertEquals("65534", rs.getString(3));
                Assert.assertEquals("4294967294", rs.getString(4));
                Assert.assertEquals("18446744073709551614", rs.getString(5));
                rs.getByte(2);
            }
        }
    }

    private void assertResultSetMetaData(ResultSetMetaData meta) throws SQLException {
        Assert.assertEquals(5, meta.getColumnCount());
        Assert.assertEquals("ts", meta.getColumnLabel(1));
        Assert.assertEquals("f1", meta.getColumnLabel(2));
        Assert.assertEquals("f2", meta.getColumnLabel(3));
        Assert.assertEquals("f3", meta.getColumnLabel(4));
        Assert.assertEquals("f4", meta.getColumnLabel(5));
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        ts = System.currentTimeMillis();

        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists unsign_jni");
        stmt.execute("create database if not exists unsign_jni");
        stmt.execute("use unsign_jni");
        stmt.execute("create table us_table(ts timestamp, f1 tinyint unsigned, f2 smallint unsigned, f3 int unsigned, f4 bigint unsigned)");
        stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + ts + ", 127, 32767,2147483647, 9223372036854775807)");
        stmt.close();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null)
            conn.close();
    }

}
