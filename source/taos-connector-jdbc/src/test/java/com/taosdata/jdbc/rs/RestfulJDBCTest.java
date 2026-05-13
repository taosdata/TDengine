package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RestfulJDBCTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final Random random = new Random(System.currentTimeMillis());
    private static Connection connection;
    private static final String DB_NAME = TestUtils.camelToSnake(RestfulJDBCTest.class);

    @Test
    public void testCase001() throws SQLException {
        // given
        String sql = "drop database if exists " + DB_NAME;
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);

        // given
        sql = "create database if not exists " + DB_NAME;
        // when
        execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);

        // given
        sql = "use " + DB_NAME;
        // when
        execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase002() throws SQLException {
        // given
        String sql = "create table " + DB_NAME + ".weather(ts timestamp, temperature float, humidity int) tags(location nchar(64), groupId int)";
        // when
        boolean execute = execute(connection, sql);
        // then
        Assert.assertFalse(execute);
    }

    @Test
    public void testCase004() throws SQLException {
        for (int i = 1; i <= 100; i++) {
            // given
            String sql = "create table " + DB_NAME + ".t" + i + " using " + DB_NAME + ".weather tags('beijing', '" + i + "')";
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
                String sql = "insert into " + DB_NAME + ".t" + j + " values(" + currentTimeMillis + "," + (random.nextFloat() * 50) + "," + random.nextInt(100) + ")";
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
        String sql = "select * from " + DB_NAME + ".weather";
        // when

        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);

            ResultSetMetaData meta = rs.getMetaData();

            // then
            Assert.assertEquals(5, meta.getColumnCount());

            while (rs.next()) {
                Assert.assertNotNull(rs.getTimestamp("ts"));
                Assert.assertNotNull(rs.getString("location"));
            }
        }
    }

    @Test
    public void testCase007() throws SQLException {
        // given
        String sql = "drop database " + DB_NAME;

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

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getRestUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.close();
            connection.close();
        }
    }

}

