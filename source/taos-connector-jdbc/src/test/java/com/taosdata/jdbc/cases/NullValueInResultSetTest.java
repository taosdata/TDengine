package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.TimestampUtil;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class NullValueInResultSetTest {

        static final String HOST = TestEnvUtil.getHost();


    private static Properties properties;
    private Connection conn;
    private final String dbName = TestUtils.camelToSnake(NullValueInResultSetTest.class);

    @Test
    public void testRestful() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("use " + dbName);
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
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("use " + dbName);
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

    @After
    public void after() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists " + dbName);
            statement.close();
            conn.close();
        }
    }
}

