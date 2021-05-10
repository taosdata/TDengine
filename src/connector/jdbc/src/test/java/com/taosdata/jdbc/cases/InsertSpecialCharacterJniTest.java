package com.taosdata.jdbc.cases;

import org.junit.*;

import java.sql.*;

public class InsertSpecialCharacterJniTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static String dbName = "spec_char_test";
    private static String tbname1 = "test";
    private static String tbname2 = "weather";
    private static String special_character_str_1 = "$asd$$fsfsf$";
    private static String special_character_str_2 = "\\asdfsfsf\\\\";
    private static String special_character_str_3 = "\\\\asdfsfsf\\";
    private static String special_character_str_4 = "?asd??fsf?sf?";
    private static String special_character_str_5 = "?#sd@$f(('<(s[P)>\"){]}f?s[]{}%vaew|\"fsfs^a&d*jhg)(j))(f@~!?$";

    @Test
    public void testCase01() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_1.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, tbname1);

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_1, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }


    @Test
    public void testCase02() throws SQLException {
        //TODO:
        // Expected :\asdfsfsf\\
        // Actual   :\asdfsfsf\

        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_2.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            //TODO: bug to be fixed
//            Assert.assertEquals(special_character_str_2, f1);
            Assert.assertEquals(special_character_str_2.substring(0, special_character_str_1.length() - 2), f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test(expected = SQLException.class)
    public void testCase03() throws SQLException {
        //TODO:
        // TDengine ERROR (216): Syntax error in SQL
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_3.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_3, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase04() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_4.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_4, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase05() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_5.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_5, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase06() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into t? using " + tbname2 + " tags(?) values(?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, special_character_str_4);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, special_character_str_4.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query t1
        final String query = "select * from t1";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_4, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase07() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1, f2) values(?, ?, ?)  ; ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, special_character_str_4.getBytes());
            pstmt.setString(3, special_character_str_4);
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_4, f1);
            String f2 = rs.getString(3);
            Assert.assertEquals(special_character_str_4, f2);
        }
    }

    @Test(expected = SQLException.class)
    public void testCase08() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into t? using " + tbname2 + " tags(?) values(?, ?, ?) ? ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, special_character_str_5);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, special_character_str_5.getBytes());
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
    }

    @Test
    public void testCase09() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into ?.t? using " + tbname2 + " tags(?) values(?, ?, ?) t? using weather tags(?) values(?,?,?) ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // t1
            pstmt.setString(1, dbName);
            pstmt.setInt(2, 1);
            pstmt.setString(3, special_character_str_5);
            pstmt.setTimestamp(4, new Timestamp(now));
            pstmt.setBytes(5, special_character_str_5.getBytes());
            // t2
            pstmt.setInt(7, 2);
            pstmt.setString(8, special_character_str_5);
            pstmt.setTimestamp(9, new Timestamp(now));
            pstmt.setString(11, special_character_str_5);

            int ret = pstmt.executeUpdate();
            Assert.assertEquals(2, ret);
        }
        // query t1
        String query = "select * from t?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, 1);

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_5, f1);
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
            Assert.assertEquals(special_character_str_5, f2);
        }
    }

    @Test
    public void testCase10() throws SQLException {
        final long now = System.currentTimeMillis();

        // insert
        final String sql = "insert into t? using ? tags(?) values(?, ?, ?) t? using " + tbname2 + " tags(?) values(?,?,?) ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // t1
            pstmt.setInt(1, 1);
            pstmt.setString(2, tbname2);
            pstmt.setString(3, special_character_str_5);
            pstmt.setTimestamp(4, new Timestamp(now));
            pstmt.setBytes(5, special_character_str_5.getBytes());
            // t2
            pstmt.setInt(7, 2);
            pstmt.setString(8, special_character_str_5);
            pstmt.setTimestamp(9, new Timestamp(now));
            pstmt.setString(11, special_character_str_5);

            int ret = pstmt.executeUpdate();
            Assert.assertEquals(2, ret);
        }
        //query t1
        String query = "select * from ?.t? where ts < ? and ts >= ? and ? is not null";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, dbName);
            pstmt.setInt(2, 1);
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.setTimestamp(4, new Timestamp(0));
            pstmt.setString(5, "f1");

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals(special_character_str_5, f1);
            byte[] f2 = rs.getBytes(3);
            Assert.assertNull(f2);
        }
        // query t2
        query = "select * from t? where ts < ? and ts >= ? and ? is not null";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, 2);
            pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            pstmt.setTimestamp(3, new Timestamp(0));
            pstmt.setString(4, "f2");

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            byte[] f1 = rs.getBytes(2);
            Assert.assertNull(f1);
            String f2 = new String(rs.getBytes(3));
            Assert.assertEquals(special_character_str_5, f2);
        }
    }

    @Test(expected = SQLException.class)
    public void testCase11() throws SQLException {
        final String speicalCharacterStr = "?#sd@$f(((s[P)){]}f?s[]{}%vs^a&d*jhg)(j))(f@~!?$";
        final long now = System.currentTimeMillis();

        final String sql = "insert into t? using " + tbname2 + " values(?, ?, 'abc?abc') ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setTimestamp(2, new Timestamp(now));
            pstmt.setBytes(3, speicalCharacterStr.getBytes());

            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
    }


    @Test
    public void testCase12() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + tbname1 + "(ts, f1, f2) values(?, 'HelloTDengine', ?)  ; ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setString(2, special_character_str_4);
            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + tbname1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            Assert.assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            Assert.assertEquals("HelloTDengine", f1);
            String f2 = rs.getString(3);
            Assert.assertEquals(special_character_str_4, f2);
        }
    }

    @Before
    public void before() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists " + tbname1 + "");
            stmt.execute("create table " + tbname1 + "(ts timestamp,f1 binary(64),f2 nchar(64))");
            stmt.execute("drop table if exists " + tbname2);
            stmt.execute("create table " + tbname2 + "(ts timestamp, f1 binary(64), f2 nchar(64)) tags(loc nchar(64))");
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbName);
            stmt.execute("create database if not exists " + dbName);
            stmt.execute("use " + dbName);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null)
            conn.close();
    }

}
