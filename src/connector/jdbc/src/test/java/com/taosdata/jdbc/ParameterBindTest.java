package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class ParameterBindTest {

    private static final String host = "127.0.0.1";
    private static final String stable = "weather";

    private Connection conn;
    private final Random random = new Random(System.currentTimeMillis());

    @Test
    public void test() {
        // given
        String[] tbnames = {"t1", "t2", "t3"};
        int rows = 10;

        // when
        insertIntoTables(tbnames, 10);

        // then
        assertRows(stable, tbnames.length * rows);
        for (String t : tbnames) {
            assertRows(t, rows);
        }
    }

    @Test
    public void testMultiThreads() {
        // given
        String[][] tables = {{"t1", "t2", "t3"}, {"t4", "t5", "t6"}, {"t7", "t8", "t9"}, {"t10"}};
        int rows = 10;

        // when
        List<Thread> threads = Arrays.stream(tables).map(tbnames -> new Thread(() -> insertIntoTables(tbnames, rows))).collect(Collectors.toList());
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // then
        for (String[] table : tables) {
            for (String t : table) {
                assertRows(t, rows);
            }
        }

    }

    private void assertRows(String tbname, int rows) {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select count(*) from " + tbname);
            while (rs.next()) {
                int count = rs.getInt(1);
                Assert.assertEquals(rows, count);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertIntoTables(String[] tbnames, int rowsEachTable) {
        long current = System.currentTimeMillis();
        String sql = "insert into ? using " + stable + " tags(?, ?) values(?, ?, ?)";
        try (TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class)) {
            for (int i = 0; i < tbnames.length; i++) {
                pstmt.setTableName(tbnames[i]);
                pstmt.setTagInt(0, random.nextInt(100));
                pstmt.setTagInt(1, random.nextInt(100));

                ArrayList<Long> timestampList = new ArrayList<>();
                for (int j = 0; j < rowsEachTable; j++) {
                    timestampList.add(current + i * 1000 + j);
                }
                pstmt.setTimestamp(0, timestampList);

                ArrayList<Integer> f1List = new ArrayList<>();
                for (int j = 0; j < rowsEachTable; j++) {
                    f1List.add(random.nextInt(100));
                }
                pstmt.setInt(1, f1List);

                ArrayList<Integer> f2List = new ArrayList<>();
                for (int j = 0; j < rowsEachTable; j++) {
                    f2List.add(random.nextInt(100));
                }
                pstmt.setInt(2, f2List);

                pstmt.columnDataAddBatch();
            }

            pstmt.columnDataExecuteBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test_pd");
            stmt.execute("create database if not exists test_pd");
            stmt.execute("use test_pd");
            stmt.execute("create table " + stable + "(ts timestamp, f1 int, f2 int) tags(t1 int, t2 int)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test_pd");
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
