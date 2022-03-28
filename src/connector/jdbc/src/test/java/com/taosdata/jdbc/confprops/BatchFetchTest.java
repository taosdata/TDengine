package com.taosdata.jdbc.confprops;

import org.junit.*;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BatchFetchTest {

    private static String host = "127.0.0.1";
    private long rowFetchCost, batchFetchCost;

    @Test
    public void case01_rowFetch() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/test?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            boolean batchfetch = Boolean.parseBoolean(conn.getClientInfo("batchfetch"));
            Assert.assertFalse(batchfetch);

            long start = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
            }
            long end = System.currentTimeMillis();
            rowFetchCost = end - start;
        }
    }

    @Test
    public void case02_batchFetch() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/test?user=root&password=taosdata&batchfetch=true";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            boolean batchfetch = Boolean.parseBoolean(conn.getClientInfo("batchfetch"));
            Assert.assertTrue(batchfetch);

            long start = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
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
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test");
            stmt.execute("use test");
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
    public static void afterClass(){
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
