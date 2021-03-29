package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;


@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class UnsignedNumberRestfulTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;

    @Test
    public void testCase001() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from us_table");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCase002() {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from us_table");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                System.out.print(meta.getColumnLabel(1) + ": " + rs.getTimestamp(1) + "\t");
                System.out.print(meta.getColumnLabel(2) + ": " + rs.getByte(2) + "\t");
                System.out.print(meta.getColumnLabel(3) + ": " + rs.getShort(3) + "\t");
                System.out.print(meta.getColumnLabel(4) + ": " + rs.getInt(4) + "\t");
                System.out.print(meta.getColumnLabel(5) + ": " + rs.getLong(5) + "\t");
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test(expected = SQLException.class)
    public void testCase003() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 127, 32767,2147483647, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                System.out.print(meta.getColumnLabel(1) + ": " + rs.getTimestamp(1) + "\t");
                System.out.print(meta.getColumnLabel(2) + ": " + rs.getByte(2) + "\t");
                System.out.print(meta.getColumnLabel(3) + ": " + rs.getShort(3) + "\t");
                System.out.print(meta.getColumnLabel(4) + ": " + rs.getInt(4) + "\t");
                System.out.print(meta.getColumnLabel(5) + ": " + rs.getLong(5) + "\t");
                System.out.println();
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
            while (rs.next()) {
                System.out.print(meta.getColumnLabel(1) + ": " + rs.getTimestamp(1) + "\t");
                System.out.print(meta.getColumnLabel(2) + ": " + rs.getByte(2) + "\t");
                System.out.print(meta.getColumnLabel(3) + ": " + rs.getShort(3) + "\t");
                System.out.print(meta.getColumnLabel(4) + ": " + rs.getInt(4) + "\t");
                System.out.print(meta.getColumnLabel(5) + ": " + rs.getLong(5) + "\t");
                System.out.println();
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
            while (rs.next()) {
                System.out.print(meta.getColumnLabel(1) + ": " + rs.getTimestamp(1) + "\t");
                System.out.print(meta.getColumnLabel(2) + ": " + rs.getByte(2) + "\t");
                System.out.print(meta.getColumnLabel(3) + ": " + rs.getShort(3) + "\t");
                System.out.print(meta.getColumnLabel(4) + ": " + rs.getInt(4) + "\t");
                System.out.print(meta.getColumnLabel(5) + ": " + rs.getLong(5) + "\t");
                System.out.println();
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
            while (rs.next()) {
                System.out.print(meta.getColumnLabel(1) + ": " + rs.getTimestamp(1) + "\t");
                System.out.print(meta.getColumnLabel(2) + ": " + rs.getByte(2) + "\t");
                System.out.print(meta.getColumnLabel(3) + ": " + rs.getShort(3) + "\t");
                System.out.print(meta.getColumnLabel(4) + ": " + rs.getInt(4) + "\t");
                System.out.print(meta.getColumnLabel(5) + ": " + rs.getLong(5) + "\t");
                System.out.println();
            }
        }
    }

    @Test
    public void testCase007() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            long now = System.currentTimeMillis();
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(" + now + ", 254, 65534,4294967294, 18446744073709551614)");
            ResultSet rs = stmt.executeQuery("select * from us_table where ts = " + now);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
                Assert.assertEquals("254", rs.getString(2));
                Assert.assertEquals("65534", rs.getString(3));
                Assert.assertEquals("4294967294", rs.getString(4));
                Assert.assertEquals("18446744073709551614", rs.getString(5));
            }
        }
    }

    @BeforeClass
    public static void beforeClass() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
            conn = DriverManager.getConnection(url, properties);

            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists unsign_restful");
            stmt.execute("create database if not exists unsign_restful");
            stmt.execute("use unsign_restful");
            stmt.execute("create table us_table(ts timestamp, f1 tinyint unsigned, f2 smallint unsigned, f3 int unsigned, f4 bigint unsigned)");
            stmt.executeUpdate("insert into us_table(ts,f1,f2,f3,f4) values(now, 127, 32767,2147483647, 9223372036854775807)");
            stmt.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
