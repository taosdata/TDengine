package com.taosdata.jdbc.cases;

import org.junit.*;

import java.sql.*;

public class InsertSpecialCharacterRestfulTest {
    //    private static final String host = "127.0.0.1";
    private static final String host = "master";
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

//    @Test
//    public void testCase02() throws SQLException {
//        final String speicalCharacterStr = "\\asdfsfsf\\\\";
//        final long now = System.currentTimeMillis();
//        // insert
//        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setTimestamp(1, new Timestamp(now));
//            pstmt.setBytes(2, speicalCharacterStr.getBytes());
//            int ret = pstmt.executeUpdate();
//            Assert.assertEquals(1, ret);
//        }
//        // query
//        final String query = "select * from specCharTest";
//        try (Statement stmt = conn.createStatement()) {
//            ResultSet rs = stmt.executeQuery(query);
//            rs.next();
//            long timestamp = rs.getTimestamp(1).getTime();
//            Assert.assertEquals(now, timestamp);
//            String f1 = new String(rs.getBytes(2));
//            Assert.assertEquals(speicalCharacterStr, f1);
//            String f2 = rs.getString(3);
//            Assert.assertNull(f2);
//        }
//    }
//
//    @Test
//    public void testCase03() throws SQLException {
//        final String speicalCharacterStr = "\\\\asdfsfsf\\";
//        final long now = System.currentTimeMillis();
//        // insert
//        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setTimestamp(1, new Timestamp(now));
//            pstmt.setBytes(2, speicalCharacterStr.getBytes());
//            int ret = pstmt.executeUpdate();
//            Assert.assertEquals(1, ret);
//        }
//        // query
//        final String query = "select * from specCharTest";
//        try (Statement stmt = conn.createStatement()) {
//            ResultSet rs = stmt.executeQuery(query);
//            rs.next();
//            long timestamp = rs.getTimestamp(1).getTime();
//            Assert.assertEquals(now, timestamp);
//            String f1 = new String(rs.getBytes(2));
//            Assert.assertEquals(speicalCharacterStr, f1);
//            String f2 = rs.getString(3);
//            Assert.assertNull(f2);
//        }
//    }
//
//    @Test
//    public void testCase04() throws SQLException {
//        final String speicalCharacterStr = "?asd??fsf?sf?";
//        final long now = System.currentTimeMillis();
//        // insert
//        final String sql = "insert into specCharTest(ts, f1) values(?, ?)";
//        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            pstmt.setTimestamp(1, new Timestamp(now));
//            pstmt.setBytes(2, speicalCharacterStr.getBytes());
//            int ret = pstmt.executeUpdate();
//            Assert.assertEquals(1, ret);
//        }
//        // query
//        final String query = "select * from specCharTest";
//        try (Statement stmt = conn.createStatement()) {
//            ResultSet rs = stmt.executeQuery(query);
//            rs.next();
//            long timestamp = rs.getTimestamp(1).getTime();
//            Assert.assertEquals(now, timestamp);
//            String f1 = new String(rs.getBytes(2));
//            Assert.assertEquals(speicalCharacterStr, f1);
//            String f2 = rs.getString(3);
//            Assert.assertNull(f2);
//        }
//    }

    @Test
    public void testCase05() throws SQLException {
        final String speicalCharacterStr = "?#sd@$f(((s[P)){]}f?s[]{}%vs^a&d*jhg)(j))(f@~!?$";
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
    public void testCase06() throws SQLException {
        final String speicalCharacterStr = "?asd??fsf?sf?";
        final long now = System.currentTimeMillis();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather(ts timestamp, f1 binary(64), f2 nchar(64)) tags(loc nchar(64))");
        }

        // insert
        final String sql = "insert into t? using weather tags(?) values(?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, speicalCharacterStr);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, speicalCharacterStr.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from t1";
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
    public void testCase07() throws SQLException {
        final String speicalCharacterStr = "?asd??fsf?sf?";
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into specCharTest(ts, f1, f2) values(?, ?, ?)  ; ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, speicalCharacterStr.getBytes());
            pstmt.setString(3, speicalCharacterStr);
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
            Assert.assertEquals(speicalCharacterStr, f2);
        }
    }

    @Test
    public void testCase08() throws SQLException {
        final String speicalCharacterStr = "?asd??fsf?sf?";
        final long now = System.currentTimeMillis();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather(ts timestamp, f1 binary(64), f2 nchar(64)) tags(loc nchar(64))");
        }

        // insert
        final String sql = "insert into t? using weather tags(?) values(?, ?, ?) ? ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, speicalCharacterStr);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, speicalCharacterStr.getBytes());
            pstmt.setString(6, ";");
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from t1";
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
    public void testCase09() throws SQLException {
        final String speicalCharacterStr = "?asd??fsf?sf?";
        final long now = System.currentTimeMillis();

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists weather");
            stmt.execute("create table weather(ts timestamp, f1 binary(64), f2 nchar(64)) tags(loc nchar(64))");
        }

        // insert
        final String sql = "insert into t? using weather tags(?) values(?, ?, ?) t? using weather tags(?) values(?,?,?) ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // t1
            pstmt.setInt(1, 1);
            pstmt.setString(2, speicalCharacterStr);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, speicalCharacterStr.getBytes());
            // t2
            pstmt.setInt(6, 2);
            pstmt.setString(7, speicalCharacterStr);
            pstmt.setTimestamp(8, new Timestamp(now));
            pstmt.setString(10, speicalCharacterStr);
            pstmt.setString(11, ";");

            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query t1
        String query = "select * from t1";
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
        // query t2
        query = "select * from t2";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            byte[] f1 = rs.getBytes(2);
            Assert.assertNull(f1);
            String f2 = new String(rs.getBytes(3));
            Assert.assertEquals(speicalCharacterStr, f2);
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
        String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
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
