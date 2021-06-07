package com.taosdata.jdbc;

import org.junit.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalTime;

public class TSDBPreparedStatementTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String sql_select = "select * from t1 where ts >= ? and ts < ? and f1 >= ?";

    private PreparedStatement pstmt_insert;
    private PreparedStatement pstmt_select;
    //create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))

    @Test
    public void executeQuery() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setInt(2, 2);
        pstmt_insert.setLong(3, 3l);
        pstmt_insert.setFloat(4, 3.14f);
        pstmt_insert.setDouble(5, 3.1415);
        pstmt_insert.setShort(6, (short) 6);
        pstmt_insert.setByte(7, (byte) 7);
        pstmt_insert.setBoolean(8, true);
        pstmt_insert.setBytes(9, "abc".getBytes());
        pstmt_insert.setString(10, "涛思数据");
        pstmt_insert.executeUpdate();
        long start = ts - 1000 * 60 * 60;
        long end = ts + 1000 * 60 * 60;
        pstmt_select.setTimestamp(1, new Timestamp(start));
        pstmt_select.setTimestamp(2, new Timestamp(end));
        pstmt_select.setInt(3, 0);

        // when
        ResultSet rs = pstmt_select.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();
        rs.next();

        // then
        assertMetaData(meta);
        {
            Assert.assertNotNull(rs);
            Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
            Assert.assertEquals(2, rs.getInt(2));
            Assert.assertEquals(2, rs.getInt("f1"));
            Assert.assertEquals(3l, rs.getLong(3));
            Assert.assertEquals(3l, rs.getLong("f2"));
            Assert.assertEquals(3.14f, rs.getFloat(4), 0.0);
            Assert.assertEquals(3.14f, rs.getFloat("f3"), 0.0);
            Assert.assertEquals(3.1415, rs.getDouble(5), 0.0);
            Assert.assertEquals(3.1415, rs.getDouble("f4"), 0.0);
            Assert.assertEquals((short) 6, rs.getShort(6));
            Assert.assertEquals((short) 6, rs.getShort("f5"));
            Assert.assertEquals((byte) 7, rs.getByte(7));
            Assert.assertEquals((byte) 7, rs.getByte("f6"));
            Assert.assertTrue(rs.getBoolean(8));
            Assert.assertTrue(rs.getBoolean("f7"));
            Assert.assertArrayEquals("abc".getBytes(), rs.getBytes(9));
            Assert.assertArrayEquals("abc".getBytes(), rs.getBytes("f8"));
            Assert.assertEquals("涛思数据", rs.getString(10));
            Assert.assertEquals("涛思数据", rs.getString("f9"));
        }
    }

    private void assertMetaData(ResultSetMetaData meta) throws SQLException {
        Assert.assertEquals(10, meta.getColumnCount());
        Assert.assertEquals("ts", meta.getColumnLabel(1));
        Assert.assertEquals("f1", meta.getColumnLabel(2));
        Assert.assertEquals("f2", meta.getColumnLabel(3));
        Assert.assertEquals("f3", meta.getColumnLabel(4));
        Assert.assertEquals("f4", meta.getColumnLabel(5));
        Assert.assertEquals("f5", meta.getColumnLabel(6));
        Assert.assertEquals("f6", meta.getColumnLabel(7));
        Assert.assertEquals("f7", meta.getColumnLabel(8));
        Assert.assertEquals("f8", meta.getColumnLabel(9));
        Assert.assertEquals("f9", meta.getColumnLabel(10));
    }

    @Test
    public void setNullForTimestamp() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(2, Types.INTEGER);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    private void assertAllNullExceptTimestamp(ResultSet rs, long ts) throws SQLException {
        Assert.assertNotNull(rs);
        Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
        Assert.assertEquals(0, rs.getInt(2));
        Assert.assertEquals(0, rs.getInt("f1"));
        Assert.assertEquals(0, rs.getLong(3));
        Assert.assertEquals(0, rs.getLong("f2"));
        Assert.assertEquals(0, rs.getFloat(4), 0.0);
        Assert.assertEquals(0, rs.getFloat("f3"), 0.0);
        Assert.assertEquals(0, rs.getDouble(5), 0.0);
        Assert.assertEquals(0, rs.getDouble("f4"), 0.0);
        Assert.assertEquals(0, rs.getShort(6));
        Assert.assertEquals(0, rs.getShort("f5"));
        Assert.assertEquals(0, rs.getByte(7));
        Assert.assertEquals(0, rs.getByte("f6"));
        Assert.assertFalse(rs.getBoolean(8));
        Assert.assertFalse(rs.getBoolean("f7"));
        Assert.assertNull(rs.getBytes(9));
        Assert.assertNull(rs.getBytes("f8"));
        Assert.assertNull(rs.getString(10));
        Assert.assertNull(rs.getString("f9"));
    }

    @Test
    public void setNullForInteger() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(3, Types.BIGINT);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForFloat() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(4, Types.FLOAT);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForDouble() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(5, Types.DOUBLE);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForSmallInt() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(6, Types.SMALLINT);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForTinyInt() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(7, Types.TINYINT);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForBoolean() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(8, Types.BOOLEAN);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForBinary() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(9, Types.BINARY);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setNullForNchar() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setNull(10, Types.NCHAR);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            assertAllNullExceptTimestamp(rs, ts);
        }
    }

    @Test
    public void setBoolean() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setBoolean(8, true);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertTrue(rs.getBoolean(8));
                Assert.assertTrue(rs.getBoolean("f7"));
            }
        }
    }

    @Test
    public void setByte() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setByte(7, (byte) 0x001);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals((byte) 0x001, rs.getByte(7));
                Assert.assertEquals((byte) 0x001, rs.getByte("f6"));
            }
        }
    }

    @Test
    public void setShort() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setShort(6, (short) 2);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals((short) 2, rs.getByte(6));
                Assert.assertEquals((short) 2, rs.getByte("f5"));
            }
        }
    }

    @Test
    public void setInt() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setInt(2, 10086);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(10086, rs.getInt(2));
                Assert.assertEquals(10086, rs.getInt("f1"));
            }
        }
    }

    @Test
    public void setLong() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setLong(3, Long.MAX_VALUE);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(Long.MAX_VALUE, rs.getLong(3));
                Assert.assertEquals(Long.MAX_VALUE, rs.getLong("f2"));
            }
        }
    }

    @Test
    public void setFloat() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setFloat(4, 3.14f);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(3.14f, rs.getFloat(4), 0.0f);
                Assert.assertEquals(3.14f, rs.getFloat("f3"), 0.0f);
            }
        }
    }

    @Test
    public void setDouble() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setDouble(5, 3.14444);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(3.14444, rs.getDouble(5), 0.0);
                Assert.assertEquals(3.14444, rs.getDouble("f4"), 0.0);
            }
        }
    }

    @Test
    public void setBigDecimal() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        BigDecimal bigDecimal = new BigDecimal(3.14444);

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setBigDecimal(5, bigDecimal);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(3.14444, rs.getDouble(5), 0.0);
                Assert.assertEquals(3.14444, rs.getDouble("f4"), 0.0);
            }
        }
    }

    @Test
    public void setString() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        String f9 = "{\"name\": \"john\", \"age\": 10, \"address\": \"192.168.1.100\"}";

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setString(10, f9);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertEquals(f9, rs.getString(10));
                Assert.assertEquals(f9, rs.getString("f9"));
            }
        }
    }

    @Test
    public void setBytes() throws SQLException, IOException {
        // given
        long ts = System.currentTimeMillis();
        byte[] f8 = "{\"name\": \"john\", \"age\": 10, \"address\": \"192.168.1.100\"}".getBytes();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmt_insert.setBytes(9, f8);
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
                Assert.assertArrayEquals(f8, rs.getBytes(9));
                Assert.assertArrayEquals(f8, rs.getBytes("f8"));
            }
        }
    }

    @Test
    public void setDate() throws SQLException {
        // given
        long ts = new java.util.Date().getTime();

        // when
        pstmt_insert.setDate(1, new Date(ts));
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test
    public void setTime() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTime(1, new Time(ts));
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test
    public void setTimestamp() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        int result = pstmt_insert.executeUpdate();

        // then
        Assert.assertEquals(1, result);
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("select * from t1");
            ResultSetMetaData meta = rs.getMetaData();
            assertMetaData(meta);
            rs.next();
            {
                Assert.assertNotNull(rs);
                Assert.assertEquals(ts, rs.getTimestamp(1).getTime());
                Assert.assertEquals(ts, rs.getTimestamp("ts").getTime());
            }
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream() throws SQLException {
        pstmt_insert.setAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream() throws SQLException {
        pstmt_insert.setBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream() throws SQLException {
        pstmt_insert.setCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRef() throws SQLException {
        pstmt_insert.setRef(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob() throws SQLException {
        pstmt_insert.setBlob(1, (Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob() throws SQLException {
        pstmt_insert.setClob(1, (Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setArray() throws SQLException {
        pstmt_insert.setArray(1, null);
    }

    @Test
    public void getMetaData() throws SQLException {
        // given
        long ts = System.currentTimeMillis();

        // when
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        ResultSetMetaData metaData = pstmt_insert.getMetaData();

        // then
        Assert.assertNull(metaData);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setURL() throws SQLException {
        pstmt_insert.setURL(1, null);
    }

    @Test
    public void getParameterMetaData() throws SQLException {
        // given
        long ts = System.currentTimeMillis();
        pstmt_insert.setTimestamp(1, new Timestamp(ts));
        pstmt_insert.setInt(2, 2);
        pstmt_insert.setLong(3, 3l);
        pstmt_insert.setFloat(4, 3.14f);
        pstmt_insert.setDouble(5, 3.1415);
        pstmt_insert.setShort(6, (short) 6);
        pstmt_insert.setByte(7, (byte) 7);
        pstmt_insert.setBoolean(8, true);
        pstmt_insert.setBytes(9, "abc".getBytes());
        pstmt_insert.setString(10, "涛思数据");

        // when
        ParameterMetaData parameterMetaData = pstmt_insert.getParameterMetaData();

        // then
        Assert.assertNotNull(parameterMetaData);
        Assert.assertEquals(10, parameterMetaData.getParameterCount());
        Assert.assertEquals(Types.TIMESTAMP, parameterMetaData.getParameterType(1));
        Assert.assertEquals(Types.INTEGER, parameterMetaData.getParameterType(2));
        Assert.assertEquals(Types.BIGINT, parameterMetaData.getParameterType(3));
        Assert.assertEquals(Types.FLOAT, parameterMetaData.getParameterType(4));
        Assert.assertEquals(Types.DOUBLE, parameterMetaData.getParameterType(5));
        Assert.assertEquals(Types.SMALLINT, parameterMetaData.getParameterType(6));
        Assert.assertEquals(Types.TINYINT, parameterMetaData.getParameterType(7));
        Assert.assertEquals(Types.BOOLEAN, parameterMetaData.getParameterType(8));
        Assert.assertEquals(Types.BINARY, parameterMetaData.getParameterType(9));
        Assert.assertEquals(Types.NCHAR, parameterMetaData.getParameterType(10));

        Assert.assertEquals("TIMESTAMP", parameterMetaData.getParameterTypeName(1));
        Assert.assertEquals("INT", parameterMetaData.getParameterTypeName(2));
        Assert.assertEquals("BIGINT", parameterMetaData.getParameterTypeName(3));
        Assert.assertEquals("FLOAT", parameterMetaData.getParameterTypeName(4));
        Assert.assertEquals("DOUBLE", parameterMetaData.getParameterTypeName(5));
        Assert.assertEquals("SMALLINT", parameterMetaData.getParameterTypeName(6));
        Assert.assertEquals("TINYINT", parameterMetaData.getParameterTypeName(7));
        Assert.assertEquals("BOOL", parameterMetaData.getParameterTypeName(8));
        Assert.assertEquals("BINARY", parameterMetaData.getParameterTypeName(9));
        Assert.assertEquals("NCHAR", parameterMetaData.getParameterTypeName(10));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRowId() throws SQLException {
        pstmt_insert.setRowId(1, null);
    }

    @Test
    public void setNString() throws SQLException {
        setString();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream() throws SQLException {
        pstmt_insert.setNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob() throws SQLException {
        pstmt_insert.setNClob(1, (NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSQLXML() throws SQLException {
        pstmt_insert.setSQLXML(1, null);
    }

    @Before
    public void before() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
            stmt.execute("create table if not exists t1 using weather tags('beijing')");
            stmt.close();

            pstmt_insert = conn.prepareStatement(sql_insert);
            pstmt_select = conn.prepareStatement(sql_select);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (pstmt_insert != null)
                pstmt_insert.close();
            if (pstmt_select != null)
                pstmt_select.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @BeforeClass
    public static void beforeClass() {
        try {
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists test_pstmt_jni");
                stmt.execute("create database if not exists test_pstmt_jni");
                stmt.execute("use test_pstmt_jni");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}