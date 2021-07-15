package com.taosdata.jdbc.cases;

import org.junit.*;

import java.sql.*;

public class TD5286Test {

    private static final String host = "127.0.0.1";
    private static final String sql_insert = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private Connection conn;

    @Test
    public void test() {
        // given
        long ts = System.currentTimeMillis();
        System.out.println("ts : " + ts);
        byte[] f8 = "{\"name\": \"john\", \"age\": 10, \"address\": \"192.168.1.100\"}".getBytes();

        // when
        int result = 0;
        try (PreparedStatement pstmt_insert = conn.prepareStatement(sql_insert)) {
            pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt_insert.setBytes(9, f8);
            result = pstmt_insert.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertArrayEquals(f8, rs.getBytes(9));
                Assert.assertArrayEquals(f8, rs.getBytes("f8"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test");
            stmt.execute("use test");
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
            stmt.execute("create table if not exists t1 using weather tags('beijing')");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
