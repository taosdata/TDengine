package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.TimestampUtil;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class NullValueInResultSetTest {
    private static final String host = "127.0.0.1";
    private static Properties properties;
    private static Connection conn_restful;
    private static Connection conn_jni;

    @Test
    public void testRestful() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        conn_restful = DriverManager.getConnection(url, properties);

        try (Statement stmt = conn_restful.createStatement()) {
            stmt.execute("drop database if exists test_null");
            stmt.execute("create database if not exists test_null");
            stmt.execute("use test_null");
            stmt.execute("create table weather(ts timestamp, f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 smallint, f7 tinyint, f8 bool, f9 binary(64), f10 nchar(64))");
            stmt.executeUpdate("insert into weather(ts, f1) values(now+1s, " + TimestampUtil.datetimeToLong("2021-04-21 12:00:00.000") + ")");
            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Assert.assertEquals("2021-04-21 12:00:00.000", TimestampUtil.longToDatetime(rs.getTimestamp(2).getTime()));
            Assert.assertEquals(true, rs.getInt(3) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getLong(4) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getFloat(5) == 0.0f && rs.wasNull());
            Assert.assertEquals(true, rs.getDouble(6) == 0.0f && rs.wasNull());
            Assert.assertEquals(true, rs.getByte(7) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getShort(8) == 0 && rs.wasNull());
            Assert.assertEquals(null, rs.getBytes(9));
            Assert.assertEquals(null, rs.getString(10));

            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJNI() throws SQLException {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        conn_jni = DriverManager.getConnection(url, properties);

        try (Statement stmt = conn_jni.createStatement()) {
            stmt.execute("drop database if exists test_null");
            stmt.execute("create database if not exists test_null");
            stmt.execute("use test_null");
            stmt.execute("create table weather(ts timestamp, f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 smallint, f7 tinyint, f8 bool, f9 binary(64), f10 nchar(64))");
            stmt.executeUpdate("insert into weather(ts, f1) values(now+1s, " + TimestampUtil.datetimeToLong("2021-04-21 12:00:00.000") + ")");
            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Assert.assertEquals("2021-04-21 12:00:00.000", TimestampUtil.longToDatetime(rs.getTimestamp(2).getTime()));
            Assert.assertEquals(true, rs.getInt(3) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getLong(4) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getFloat(5) == 0.0f && rs.wasNull());
            Assert.assertEquals(true, rs.getDouble(6) == 0.0f && rs.wasNull());
            Assert.assertEquals(true, rs.getByte(7) == 0 && rs.wasNull());
            Assert.assertEquals(true, rs.getShort(8) == 0 && rs.wasNull());
            Assert.assertEquals(null, rs.getBytes(9));
            Assert.assertEquals(null, rs.getString(10));

            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn_restful != null)
            conn_restful.close();
        if (conn_jni != null) {
            Statement statement = conn_jni.createStatement();
            statement.execute("drop database if exists test_null");
            statement.close();
            conn_jni.close();
        }
    }
}
