package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.utils.TimestampUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.*;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "negative value convert to timestamp", author = "huolibo", version = "2.0.37")
@Ignore // TODO 3.0 timestamp
public class DatetimeBefore1970Test {
    private static final String DB_NAME = TestUtils.camelToSnake(DatetimeBefore1970Test.class);

                    private Connection conn;

    @Test
    @Description("millisecond")
    public void msTest() throws SQLException {
        conn = createEnvironment("ms");
        long now = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement()) {
            // given
            // before
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 00:00:00.001')");
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 23:59:59.999')");
            // zero
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000')");
            //after
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.001')");
            stmt.executeUpdate("insert into weather(ts) values(" + now + ")");
            ResultSet rs = stmt.executeQuery("select * from weather order by ts asc");
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(2, metaData.getColumnCount());

            // when
            rs.next();
            // then
            Timestamp ts = rs.getTimestamp("ts");
            Assert.assertEquals(-24 * 60 * 60 * 1000 + 1, ts.getTime());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(-1, ts.getTime());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(0, ts.getTime());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(1, ts.getTime());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(now, ts.getTime());
        }
    }

    @Test
    @Description("microsecond")
    public void usTest() throws SQLException {
        conn = createEnvironment("us");
        long now = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement()) {
            // given
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 00:00:00.000001')");
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 23:59:59.999999')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000000')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000001')");
            stmt.executeUpdate("insert into weather(ts) values(" + now + ")");
            ResultSet rs = stmt.executeQuery("select * from weather order by ts asc");
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(2, metaData.getColumnCount());

            // when
            rs.next();
            // then
            Timestamp ts = rs.getTimestamp("ts");
            Assert.assertEquals(-24 * 60 * 60 * 1000, ts.getTime());
            Assert.assertEquals(1_000, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(-1, ts.getTime());
            Assert.assertEquals(999_999_000, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(0, ts.getTime());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(0, ts.getTime());
            Assert.assertEquals(1_000, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            String s = String.valueOf(now);
            Assert.assertEquals(Long.parseLong(s.substring(0, s.length() - 3)), ts.getTime());
            Assert.assertEquals(Long.parseLong(s.substring(s.length() - 6) + "000"), ts.getNanos());
        }
    }

    @Test
    @Description("nanosecond")
    public void nanoTest() throws SQLException {
        conn = createEnvironment("ns");
        long now = System.currentTimeMillis() * 1000_000L + System.nanoTime() % 1000_000L;
        try (Statement stmt = conn.createStatement()) {
            // given
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 00:00:00.000000123')");
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 23:59:59.999999999')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000000001')");
            stmt.executeUpdate("insert into weather(ts) values(" + now + ")");
            ResultSet rs = stmt.executeQuery("select * from weather order by ts asc");
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(2, metaData.getColumnCount());

            // when
            rs.next();
            // then
            Timestamp ts = rs.getTimestamp("ts");
            Assert.assertEquals(-24 * 60 * 60 * 1_000, ts.getTime());
            Assert.assertEquals(123, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(-1, ts.getTime());
            Assert.assertEquals(999999999, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(0, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals(1, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            String s = String.valueOf(now);
            Assert.assertEquals(Long.parseLong(s.substring(0, s.length() - 6)), ts.getTime());
            Assert.assertEquals(Long.parseLong(s.substring(s.length() - 9)), ts.getNanos());
        }
    }

    @Test
    @Ignore
    @Description("nanosecond convert timestamp when timezone is asia shanghai")
    public void asiaShanghaiTest() throws SQLException {
        conn = createEnvironment("ns");
        long now = System.currentTimeMillis() * 1000_000L + System.nanoTime() % 1000_000L;
        try (Statement stmt = conn.createStatement()) {
            // given
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 00:00:00.000000123')");
            stmt.executeUpdate("insert into weather(ts) values('1969-12-31 23:59:59.999999999')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000')");
            stmt.executeUpdate("insert into weather(ts) values('1970-01-01 00:00:00.000000001')");
            stmt.executeUpdate("insert into weather(ts) values(" + now + ")");
            ResultSet rs = stmt.executeQuery("select * from weather order by ts asc");
            ResultSetMetaData metaData = rs.getMetaData();
            Assert.assertEquals(2, metaData.getColumnCount());

            // when
            rs.next();
            // then
            Timestamp ts = rs.getTimestamp("ts");
            Assert.assertEquals("1969-12-31 08:00:00.000", TimestampUtil.longToDatetime(ts.getTime()));
            Assert.assertEquals(123, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 07:59:59.999", TimestampUtil.longToDatetime(ts.getTime()));
            Assert.assertEquals(999999999, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 08:00:00.000", TimestampUtil.longToDatetime(ts.getTime()));
            Assert.assertEquals(0, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            Assert.assertEquals("1970-01-01 08:00:00.000", TimestampUtil.longToDatetime(ts.getTime()));
            Assert.assertEquals(1, ts.getNanos());

            // when
            rs.next();
            // then
            ts = rs.getTimestamp("ts");
            String s = String.valueOf(now);
            Assert.assertEquals(Long.parseLong(s.substring(0, s.length() - 6)), ts.getTime());
            Assert.assertEquals(Long.parseLong(s.substring(s.length() - 9)), ts.getNanos());
        }
    }

    private Connection createEnvironment(String precision) throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&timezone=UTC";
        } else {
            url += "&timezone=UTC";
        }
        String createSql = "create database if not exists " + DB_NAME + " keep 36500";
        if (!isEmpty(precision)) {
            createSql += " precision '" + precision + "'";
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute(createSql);
        stmt.execute("use " + DB_NAME);
        stmt.execute("create table weather(ts timestamp,f1 float)");
        stmt.close();
        return conn;
    }

    private boolean isEmpty(String string) {
        return null == string || string.trim().equals("");
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.close();
            conn.close();
        }
    }
}

