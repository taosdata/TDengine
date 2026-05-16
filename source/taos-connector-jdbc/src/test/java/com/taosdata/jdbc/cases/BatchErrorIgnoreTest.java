package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;
import java.util.stream.IntStream;

public class BatchErrorIgnoreTest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String DB_NAME = TestUtils.camelToSnake(BatchErrorIgnoreTest.class);

    @Test
    public void batchErrorThrowException() throws SQLException {
        // given
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Connection conn = DriverManager.getConnection(url);

        // when
        try (Statement stmt = conn.createStatement()) {
            IntStream.range(1, 6).mapToObj(i -> "insert into " + DB_NAME + ".t" + i + " values(now, " + i + ")").forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException ignored) {
                    // do nothing
                }
            });
            stmt.addBatch("insert into t11 values(now, 11)");
            IntStream.range(6, 11).mapToObj(i -> "insert into " + DB_NAME + ".t" + i + " values(now, " + i + "),(now + 1s, " + (10 * i) + ")").forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException ignored) {
                    // do nothing
                }
            });
            stmt.addBatch("select count(*) from " + DB_NAME + ".weather");

            stmt.executeBatch();
        } catch (BatchUpdateException e) {
            int[] updateCounts = e.getUpdateCounts();
            Assert.assertEquals(5, updateCounts.length);
            Assert.assertEquals(1, updateCounts[0]);
            Assert.assertEquals(1, updateCounts[1]);
            Assert.assertEquals(1, updateCounts[2]);
            Assert.assertEquals(1, updateCounts[3]);
            Assert.assertEquals(1, updateCounts[4]);
        }

    }

    @Test
    public void batchErrorIgnore() throws SQLException {
        // given
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchErrorIgnore=true";
        } else {
            url += "&batchErrorIgnore=true";
        }
        Connection conn = DriverManager.getConnection(url);

        // when
        int[] results = null;
        try (Statement stmt = conn.createStatement()) {
            IntStream.range(1, 6).mapToObj(i -> "insert into " + DB_NAME + ".t" + i + " values(now, " + i + ")").forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException ignored) {
                    // do nothing
                }
            });
            stmt.addBatch("insert into t11 values(now, 11)");
            IntStream.range(6, 11).mapToObj(i -> "insert into " + DB_NAME + ".t" + i + " values(now, " + i + "),(now + 1s, " + (10 * i) + ")").forEach(sql -> {
                try {
                    stmt.addBatch(sql);
                } catch (SQLException ignored) {
                    // do nothing
                }
            });
            stmt.addBatch("select count(*) from " + DB_NAME + ".weather");

            results = stmt.executeBatch();
        }

        // then
        assert results != null;
        Assert.assertEquals(12, results.length);
        Assert.assertEquals(1, results[0]);
        Assert.assertEquals(1, results[1]);
        Assert.assertEquals(1, results[2]);
        Assert.assertEquals(1, results[3]);
        Assert.assertEquals(1, results[4]);
        Assert.assertEquals(Statement.EXECUTE_FAILED, results[5]);
        Assert.assertEquals(2, results[6]);
        Assert.assertEquals(2, results[7]);
        Assert.assertEquals(2, results[8]);
        Assert.assertEquals(2, results[9]);
        Assert.assertEquals(2, results[10]);
        Assert.assertEquals(Statement.SUCCESS_NO_INFO, results[11]);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {

            stmt.execute("use " + DB_NAME);
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather (ts timestamp, f1 float) tags(t1 int)");
            IntStream.range(1, 11).mapToObj(i -> "create table t" + i + " using weather tags(" + i + ")").forEach(sql -> {
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {

            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {

            stmt.execute("drop database if exists " + DB_NAME);
        }
    }
}

