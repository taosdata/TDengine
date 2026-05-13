package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;

import java.sql.*;

import static org.junit.Assert.assertEquals;

public class InsertSpecialCharacterRestfulTest {

                    private static Connection conn;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(InsertSpecialCharacterRestfulTest.class);
    private static final String TB_NAME_1 = "test";
    private static final String TB_NAME_2 = "weather";
    private static final String SPECIAL_CHARACTER_STR_1 = "$asd$$fsfsf$";
    private static final String SPECIAL_CHARACTER_STR_2 = "\\\\asdfsfsf\\\\";
    private static final String SPECIAL_CHARACTER_STR_3 = "\\\\asdfsfsf\\";
    private static final String SPECIAL_CHARACTER_STR_4 = "?asd??fsf?sf?";
    private static final String SPECIAL_CHARACTER_STR_5 = "?#sd@$f(('<(s[P)>\"){]}f?s[]{}%vaew|\"fsfs^a&d*jhg)(j))(f@~!?$";

    @Test
    public void testCase01() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_1.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, TB_NAME_1);

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_1, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase02() throws SQLException {
        //TODO:
        // Expected :\asdfsfsf\
        // Actual   :\asdfsfsf\

        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_2.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_2.substring(1, SPECIAL_CHARACTER_STR_1.length() - 1), f1);
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
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_3.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_3, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase04() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_4.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_4, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase05() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1) values(?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_5.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_5, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase06() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into t? using " + TB_NAME_2 + " tags(?) values(?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, SPECIAL_CHARACTER_STR_4);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, SPECIAL_CHARACTER_STR_4.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query t1
        final String query = "select * from t1";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_4, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
    }

    @Test
    public void testCase07() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1, f2) values(?, ?, ?)  ; ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setBytes(2, SPECIAL_CHARACTER_STR_4.getBytes());
            pstmt.setString(3, SPECIAL_CHARACTER_STR_4);
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_4, f1);
            String f2 = rs.getString(3);
            assertEquals(SPECIAL_CHARACTER_STR_4, f2);
        }
    }

    @Test(expected = SQLException.class)
    public void testCase08() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into t? using " + TB_NAME_2 + " tags(?) values(?, ?, ?) ? ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, SPECIAL_CHARACTER_STR_5);
            pstmt.setTimestamp(3, new Timestamp(now));
            pstmt.setBytes(4, SPECIAL_CHARACTER_STR_5.getBytes());
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
    }

    @Test
    public void testCase09() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into ?.t? using " + TB_NAME_2 + " tags(?) values(?, ?, ?) t? using weather tags(?) values(?,?,?) ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // t1
            pstmt.setString(1, DB_NAME);
            pstmt.setInt(2, 1);
            pstmt.setString(3, SPECIAL_CHARACTER_STR_5);
            pstmt.setTimestamp(4, new Timestamp(now));
            pstmt.setBytes(5, SPECIAL_CHARACTER_STR_5.getBytes());
            // t2
            pstmt.setInt(7, 2);
            pstmt.setString(8, SPECIAL_CHARACTER_STR_5);
            pstmt.setTimestamp(9, new Timestamp(now));
            pstmt.setString(11, SPECIAL_CHARACTER_STR_5);

            int ret = pstmt.executeUpdate();
            assertEquals(2, ret);
        }
        // query t1
        String query = "select * from t?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, 1);

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_5, f1);
            String f2 = rs.getString(3);
            Assert.assertNull(f2);
        }
        // query t2
        query = "select * from t2";
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            byte[] f1 = rs.getBytes(2);
            Assert.assertNull(f1);
            String f2 = new String(rs.getBytes(3));
            assertEquals(SPECIAL_CHARACTER_STR_5, f2);
        }
    }

    @Test
    public void testCase10() throws SQLException {
        final long now = System.currentTimeMillis();

        // insert
        final String sql = "insert into t? using ? tags(?) values(?, ?, ?) t? using " + TB_NAME_2 + " tags(?) values(?,?,?) ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // t1
            pstmt.setInt(1, 1);
            pstmt.setString(2, TB_NAME_2);
            pstmt.setString(3, SPECIAL_CHARACTER_STR_5);
            pstmt.setTimestamp(4, new Timestamp(now));
            pstmt.setBytes(5, SPECIAL_CHARACTER_STR_5.getBytes());
            // t2
            pstmt.setInt(7, 2);
            pstmt.setString(8, SPECIAL_CHARACTER_STR_5);
            pstmt.setTimestamp(9, new Timestamp(now));
            pstmt.setString(11, SPECIAL_CHARACTER_STR_5);

            int ret = pstmt.executeUpdate();
            assertEquals(2, ret);
        }
        //query t1
        String query = "select * from ?.t? where ts < ? and ts >= ? and f1 is not null";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, DB_NAME);
            pstmt.setInt(2, 1);
            pstmt.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            pstmt.setTimestamp(4, new Timestamp(0));

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals(SPECIAL_CHARACTER_STR_5, f1);
            byte[] f2 = rs.getBytes(3);
            Assert.assertNull(f2);
        }
        // query t2
        query = "select * from t? where ts < ? and ts >= ? and f2 is not null";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, 2);
            pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            pstmt.setTimestamp(3, new Timestamp(0));

            ResultSet rs = pstmt.executeQuery();
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            byte[] f1 = rs.getBytes(2);
            Assert.assertNull(f1);
            String f2 = new String(rs.getBytes(3));
            assertEquals(SPECIAL_CHARACTER_STR_5, f2);
        }
    }

    @Test(expected = SQLException.class)
    public void testCase11() throws SQLException {
        final String specialCharacterStr = "?#sd@$f(((s[P)){]}f?s[]{}%vs^a&d*jhg)(j))(f@~!?$";
        final long now = System.currentTimeMillis();

        final String sql = "insert into t? using " + TB_NAME_2 + " values(?, ?, 'abc?abc') ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, 1);
            pstmt.setTimestamp(2, new Timestamp(now));
            pstmt.setBytes(3, specialCharacterStr.getBytes());

            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
    }

    @Test
    public void testCase12() throws SQLException {
        final long now = System.currentTimeMillis();
        // insert
        final String sql = "insert into " + TB_NAME_1 + "(ts, f1, f2) values(?, 'HelloTDengine', ?)  ; ";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setTimestamp(1, new Timestamp(now));
            pstmt.setString(2, SPECIAL_CHARACTER_STR_4);
            int ret = pstmt.executeUpdate();
            assertEquals(1, ret);
        }
        // query
        final String query = "select * from " + TB_NAME_1;
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long timestamp = rs.getTimestamp(1).getTime();
            assertEquals(now, timestamp);
            String f1 = new String(rs.getBytes(2));
            assertEquals("HelloTDengine", f1);
            String f2 = rs.getString(3);
            assertEquals(SPECIAL_CHARACTER_STR_4, f2);
        }
    }

    @Before
    public void before() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop table if exists " + TB_NAME_1 + "");
            stmt.execute("create table " + TB_NAME_1 + "(ts timestamp,f1 binary(64),f2 nchar(64))");
            stmt.execute("drop table if exists " + TB_NAME_2);
            stmt.execute("create table " + TB_NAME_2 + "(ts timestamp, f1 binary(64), f2 nchar(64)) tags(loc nchar(64))");
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (conn != null) {
            Statement statement = conn.createStatement();
            statement.execute("drop database if exists "+ DB_NAME);
            statement.close();
            conn.close();
        }
    }
}

