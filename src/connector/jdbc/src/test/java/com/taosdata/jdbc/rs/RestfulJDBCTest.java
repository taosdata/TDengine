package com.taosdata.jdbc.rs;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestfulJDBCTest {

    private static final String host = "127.0.0.1";
    private final Random random = new Random(System.currentTimeMillis());
    private Connection connection;

    @Test
    public void testCase001() throws SQLException {
        // given
        String sql = "drop database if exists restful_test";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);

        // given
        sql = "create database if not exists restful_test";
        // when
        execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);

        // given
        sql = "use restful_test";
        // when
        execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase002() throws SQLException {
        // given
        String sql = "create table weather(ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase004() throws SQLException {
        for (int i = 1; i <= 100; i++) {
            // given
            String sql = "create table t" + i + " using weather tags('beijing', '" + i + "')";
            // when
            boolean execute = execute(connection, sql);
            // then
            Assert.assertFalse(execute);
        }
    }

    @Test
    public void testCase005() throws SQLException {
        int rows = 0;
        for (int i = 0; i < 10; i++) {
            for (int j = 1; j <= 100; j++) {

                // given
                long currentTimeMillis = System.currentTimeMillis();
                String sql = "insert into t" + j + " values(" + currentTimeMillis + "," + (random.nextFloat() * 50) + "," + random.nextInt(100) + ")";
                // when
                int affectRows = executeUpdate(connection, sql);
                // then
                Assert.assertEquals(1, affectRows);

                rows += affectRows;
            }
        }
        Assert.assertEquals(1000, rows);
    }

    @Test
    public void testCase006() throws SQLException {
        // given
        String sql = "select * from weather";
        // when
        ResultSet rs = executeQuery(connection, sql);
        ResultSetMetaData meta = rs.getMetaData();

        // then
        Assert.assertEquals(5, meta.getColumnCount());

        while (rs.next()) {
            Assert.assertNotNull(rs.getTimestamp("ts"));
            Assert.assertNotNull(rs.getFloat("temperature"));
            Assert.assertNotNull(rs.getInt("humidity"));
            Assert.assertNotNull(rs.getString("location"));
        }
    }

    @Test
    public void testCase007() throws SQLException {
        // given
        String sql = "drop database restful_test";

        // when
        boolean execute = execute(connection, sql);

        // then
        Assert.assertFalse(execute);
    }

    private int executeUpdate(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    private boolean execute(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            return stmt.execute(sql);
        }
    }


    private ResultSet executeQuery(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            return statement.executeQuery(sql);
        }
    }

    @Before
    public void before() throws SQLException {
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=root&password=taosdata&httpKeepAlive=false");
    }

    @After
    public void after() throws SQLException {
        if (connection != null)
            connection.close();
    }

}
