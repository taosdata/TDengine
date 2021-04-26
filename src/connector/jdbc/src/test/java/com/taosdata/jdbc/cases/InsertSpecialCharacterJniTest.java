package com.taosdata.jdbc.cases;

import org.junit.*;

import java.sql.*;

public class InsertSpecialCharacterJniTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;

    @Test
    public void testCase01() throws SQLException {
        final String speicalCharacterStr = "$asd$$fsfsf$";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from specCharTest";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(speicalCharacterStr, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase02() throws SQLException {
        final String speicalCharacterStr = "\\asdfsfsf\\\\";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from specCharTest";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(speicalCharacterStr, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase03() throws SQLException {
        final String speicalCharacterStr = "\\\\asdfsfsf\\";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from specCharTest";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(speicalCharacterStr, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase04() throws SQLException {
        final String speicalCharacterStr = "?asd??fsf?sf?";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from specCharTest";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(speicalCharacterStr, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase05() throws SQLException {
        final String speicalCharacterStr = "?#sd@$fsf?s%vs^a&d*jhg)(j))(f@~!?$";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from specCharTest";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(speicalCharacterStr, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Before
    public void before() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists specCharTest");
            stmt.execute("create table specCharTest(ts timestamp,f1 binary(64),f2 nchar(64))");
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists spec_char_test");
            stmt.execute("create database if not exists spec_char_test");
            stmt.execute("use spec_char_test");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null)
            conn.close();
    }

}
