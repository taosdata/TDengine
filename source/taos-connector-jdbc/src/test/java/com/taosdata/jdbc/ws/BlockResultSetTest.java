package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.Calendar;
import java.util.Properties;

public class BlockResultSetTest {

        private static Connection conn;
    private static Statement stmt;
    private static ResultSet rs;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(BlockResultSetTest.class);

    @Test
    public void testIsBeforeFirst() throws SQLException {
        Assert.assertFalse(rs.isBeforeFirst());
    }

    @Test
    public void testIsAfterLast() throws SQLException {
        Assert.assertFalse(rs.isAfterLast());
    }

    @Test
    public void testIsFirst() throws SQLException {
        Assert.assertTrue(rs.isFirst());
    }

    @Test
    public void testIsLast() throws SQLException {
        Assert.assertFalse(rs.isLast());
    }

    @Test
    public void testBeforeFirst() throws SQLException {
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
    }

    @Test
    public void testAfterLast() throws SQLException {
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
    }

    @Test
    public void testFirst() throws SQLException {
        Assert.assertTrue(rs.first());
        Assert.assertTrue(rs.isFirst());
    }

    @Test
    public void testLast() throws SQLException {
        Assert.assertTrue(rs.last());
        Assert.assertTrue(rs.isLast());
    }

    @Test
    public void testGetRow() throws SQLException {
        int row = rs.getRow();
        Assert.assertEquals(1, row);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testAbsolute() throws SQLException {
        rs.absolute(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testRelative() throws SQLException {
        rs.relative(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testPrevious() throws SQLException {
        rs.previous();
    }

    @Test
    public void testGetNString() throws SQLException {
        String value = rs.getNString(10);
        Assert.assertEquals("涛思数据", value);
    }

    @Test
    public void testGetStatement() throws SQLException {
        Statement statement = rs.getStatement();
        Assert.assertNotNull(statement);
    }

    @Test
    public void testGetTimestampWithCalendar() throws SQLException {
        Calendar calendar = Calendar.getInstance();
        rs.first();
        Timestamp timestamp = rs.getTimestamp(1, calendar);
        Assert.assertEquals("2021-01-01 00:00:00.0", timestamp.toString());
    }
    @Test(expected = SQLException.class)
    public void testIsBeforeFirstWhenClosed() throws SQLException {
        rs.close();
        rs.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testAbsoluteWhenClosed() throws SQLException {
        rs.close();
        rs.absolute(1);
    }

    @Test(expected = SQLException.class)
    public void testRelativeWhenClosed() throws SQLException {
        rs.close();
        rs.relative(1);
    }

    @Test(expected = SQLException.class)
    public void testPreviousWhenClosed() throws SQLException {
        rs.close();
        rs.previous();
    }

    @Test(expected = SQLException.class)
    public void testIsAfterLastWhenClosed() throws SQLException {
        rs.close();
        rs.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void testIsFirstWhenClosed() throws SQLException {
        rs.close();
        rs.isFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsLastWhenClosed() throws SQLException {
        rs.close();
        rs.isLast();
    }

    @Test(expected = SQLException.class)
    public void testBeforeFirstWhenClosed() throws SQLException {
        rs.close();
        rs.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testAfterLastWhenClosed() throws SQLException {
        rs.close();
        rs.afterLast();
    }

    @Test(expected = SQLException.class)
    public void testFirstWhenClosed() throws SQLException {
        rs.close();
        rs.first();
    }

    @Test(expected = SQLException.class)
    public void testLastWhenClosed() throws SQLException {
        rs.close();
        rs.last();
    }

    @Test(expected = SQLException.class)
    public void testGetRowWhenClosed() throws SQLException {
        rs.close();
        rs.getRow();
    }

    @Test(expected = SQLException.class)
    public void testFindColumnWhenClosed() throws SQLException {
        rs.close();
        rs.findColumn("f1");
    }

    @Test(expected = SQLException.class)
    public void testGetStatementWhenClosed() throws SQLException {
        rs.close();
        rs.getStatement();
    }

    @Before
    public void before() throws SQLException {
        rs = stmt.executeQuery("select * from weather");
        rs.next();
    }
    @After
    public void after() throws SQLException {
        if (rs != null) {
            rs.close();
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestUtils.runInMain();

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url, properties);
        stmt = conn.createStatement();
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("drop table if exists weather");
        stmt.execute("create table if not exists weather(f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 binary(64), f7 smallint, f8 tinyint, f9 bool, f10 nchar(64))");
        stmt.execute("insert into weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, 'abc', 10, 10, true, '涛思数据')");
        stmt.execute("insert into weather values('2021-01-01 00:00:00.001', 1, 100, 3.1415, 3.1415926, 'abc', 10, 10, true, '涛思数据')");
        rs = stmt.executeQuery("select * from weather");
        rs.next();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (conn != null) {
                Statement statement = conn.createStatement();
                statement.execute("drop database if exists " + DB_NAME);
                statement.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}