package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.TimestampUtil;
import org.junit.*;

import java.sql.*;

public class DatetimeBefore1970Test {

    private static final String host = "127.0.0.1";
    private Connection conn;

    @Test
    public void test() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // given
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 23:59:59.999')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 08:00:00.000')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 07:59:59.999')");
            ResultSet rs = stmt.executeQuery("select * from weather order by ts asc");
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(2, metaData.getColumnCount());

            // when
            rs.next();
            // then
            Timestamp ts = rs.getTimestamp("ts");
            Assert.assertEquals("1969-12-31 23:59:59.999", TimestampUtil.longToDatetime(ts.getTime()));

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 00:00:00.000", TimestampUtil.longToDatetime(ts.getTime()));

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 08:00:00.000", TimestampUtil.longToDatetime(ts.getTime()));

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 07:59:59.999", TimestampUtil.longToDatetime(ts.getTime()));
        }
    }

    @Before
    public void before() throws SQLException {
        conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists test_timestamp");
        stmt.execute("create database if not exists test_timestamp keep 36500");
        stmt.execute("use test_timestamp");
        stmt.execute("create table weather(ts timestamp,f1 float)");
        stmt.close();
    }

    @After
    public void after() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists test_timestamp");
        if (conn != null)
            conn.close();
    }
}
