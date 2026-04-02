package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore // performance
public class BatchFetchTest {

            private long rowFetchCost, batchFetchCost;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(BatchFetchTest.class);

    @Test
    public void case01_rowFetch() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/" + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "test?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {

            boolean batchfetch = Boolean.parseBoolean(conn.getClientInfo("batchfetch"));
            Assert.assertFalse(batchfetch);

            long start = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
                // do nothing
            }
            long end = System.currentTimeMillis();
            rowFetchCost = end - start;
        }
    }

    @Test
    public void case02_batchFetch() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/" + DB_NAME + "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        } else {
            url += "test?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {

            boolean batchfetch = Boolean.parseBoolean(conn.getClientInfo("batchfetch"));
            Assert.assertTrue(batchfetch);

            long start = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
                // do nothing
            }
            long end = System.currentTimeMillis();
            batchFetchCost = end - start;
        }
    }

    @Test
    public void case03_batchFetchFastThanRowFetch() {
        Assert.assertTrue(rowFetchCost - batchFetchCost >= 0);
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
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f int) tags(t int)");
            for (int i = 0; i < 1000; i++) {
                stmt.execute(generateSql(100, 100));
            }
        }
    }

    private static String generateSql(int tableSize, int valueSize) {
        Random random = new Random(System.currentTimeMillis());
        StringBuilder builder = new StringBuilder("insert into ");
        for (int i = 0; i < tableSize; i++) {
            builder.append("t" + i).append(" using weather tags(").append(random.nextInt(100)).append(") values");
            for (int j = 0; j < valueSize; j++) {
                builder.append(" (now + ").append(i).append("s, ").append(random.nextInt(100)).append(")");
            }
        }
        return builder.toString();
    }

    @AfterClass
    public static void afterClass() {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

