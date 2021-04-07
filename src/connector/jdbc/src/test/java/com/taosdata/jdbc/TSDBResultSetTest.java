package com.taosdata.jdbc;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.rs.RestfulResultSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TSDBResultSetTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static Statement stmt;
    private static ResultSet rs;

    @Test
    public void wasNull() throws SQLException {
        Assert.assertFalse(rs.wasNull());
    }

    @Test
    public void getString() throws SQLException {
        String f10 = rs.getString("f10");
        Assert.assertEquals("涛思数据", f10);
        f10 = rs.getString(10);
        Assert.assertEquals("涛思数据", f10);
    }

    @Test
    public void getBoolean() throws SQLException {
        Boolean f9 = rs.getBoolean("f9");
        Assert.assertEquals(true, f9);
        f9 = rs.getBoolean(9);
        Assert.assertEquals(true, f9);
    }

    @Test
    public void getByte() throws SQLException {
        byte f8 = rs.getByte("f8");
        Assert.assertEquals(10, f8);
        f8 = rs.getByte(8);
        Assert.assertEquals(10, f8);
    }

    @Test
    public void getShort() throws SQLException {
        short f7 = rs.getShort("f7");
        Assert.assertEquals(10, f7);
        f7 = rs.getShort(7);
        Assert.assertEquals(10, f7);
    }

    @Test
    public void getInt() throws SQLException {
        int f2 = rs.getInt("f2");
        Assert.assertEquals(1, f2);
        f2 = rs.getInt(2);
        Assert.assertEquals(1, f2);
    }

    @Test
    public void getLong() throws SQLException {
        long f3 = rs.getLong("f3");
        Assert.assertEquals(100, f3);
        f3 = rs.getLong(3);
        Assert.assertEquals(100, f3);
    }

    @Test
    public void getFloat() throws SQLException {
        float f4 = rs.getFloat("f4");
        Assert.assertEquals(3.1415f, f4, 0f);
        f4 = rs.getFloat(4);
        Assert.assertEquals(3.1415f, f4, 0f);
    }

    @Test
    public void getDouble() throws SQLException {
        double f5 = rs.getDouble("f5");
        Assert.assertEquals(3.1415926, f5, 0.0);
        f5 = rs.getDouble(5);
        Assert.assertEquals(3.1415926, f5, 0.0);
    }

    @Test
    public void getBigDecimal() throws SQLException {
        BigDecimal f1 = rs.getBigDecimal("f1");
        Assert.assertEquals(1609430400000l, f1.longValue());

        BigDecimal f2 = rs.getBigDecimal("f2");
        Assert.assertEquals(1, f2.intValue());

        BigDecimal f3 = rs.getBigDecimal("f3");
        Assert.assertEquals(100l, f3.longValue());

        BigDecimal f4 = rs.getBigDecimal("f4");
        Assert.assertEquals(3.1415f, f4.floatValue(), 0.00000f);

        BigDecimal f5 = rs.getBigDecimal("f5");
        Assert.assertEquals(3.1415926, f5.doubleValue(), 0.0000000);

        BigDecimal f7 = rs.getBigDecimal("f7");
        Assert.assertEquals(10, f7.intValue());

        BigDecimal f8 = rs.getBigDecimal("f8");
        Assert.assertEquals(10, f8.intValue());
    }

    @Test
    public void getBytes() throws SQLException {
        byte[] f1 = rs.getBytes("f1");
        Assert.assertEquals("2021-01-01 00:00:00.0", new String(f1));

        byte[] f2 = rs.getBytes("f2");
        Assert.assertEquals(1, Ints.fromByteArray(f2));

        byte[] f3 = rs.getBytes("f3");
        Assert.assertEquals(100l, Longs.fromByteArray(f3));

        byte[] f4 = rs.getBytes("f4");
        Assert.assertEquals(3.1415f, Float.valueOf(new String(f4)), 0.000000f);

        byte[] f5 = rs.getBytes("f5");
        Assert.assertEquals(3.1415926, Double.valueOf(new String(f5)), 0.000000f);

        byte[] f6 = rs.getBytes("f6");
        Assert.assertEquals("abc", new String(f6));

        byte[] f7 = rs.getBytes("f7");
        Assert.assertEquals((short) 10, Shorts.fromByteArray(f7));

        byte[] f8 = rs.getBytes("f8");
        Assert.assertEquals(1, f8.length);
        Assert.assertEquals((byte) 10, f8[0]);

        byte[] f9 = rs.getBytes("f9");
        Assert.assertEquals("true", new String(f9));

        byte[] f10 = rs.getBytes("f10");
        Assert.assertEquals("涛思数据", new String(f10));
    }

    @Test
    public void getDate() throws SQLException, ParseException {
        Date f1 = rs.getDate("f1");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Assert.assertEquals(sdf.parse("2021-01-01"), f1);
    }

    @Test
    public void getTime() throws SQLException {
        Time f1 = rs.getTime("f1");
        Assert.assertEquals("00:00:00", f1.toString());
    }

    @Test
    public void getTimestamp() throws SQLException {
        Timestamp f1 = rs.getTimestamp("f1");
        Assert.assertEquals("2021-01-01 00:00:00.0", f1.toString());
        f1 = rs.getTimestamp(1);
        Assert.assertEquals("2021-01-01 00:00:00.0", f1.toString());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getAsciiStream() throws SQLException {
        rs.getAsciiStream("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getUnicodeStream() throws SQLException {
        rs.getUnicodeStream("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBinaryStream() throws SQLException {
        rs.getBinaryStream("f1");
    }

    @Test
    public void getWarnings() throws SQLException {
        Assert.assertNull(rs.getWarnings());
    }

    @Test
    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCursorName() throws SQLException {
        rs.getCursorName();
    }

    @Test
    public void getMetaData() throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertNotNull(meta);
    }

    @Test
    public void getObject() throws SQLException, ParseException {
        Object f1 = rs.getObject("f1");
        Assert.assertEquals(Timestamp.class, f1.getClass());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
        java.util.Date date = sdf.parse("2021-01-01 00:00:00.000");
        Assert.assertEquals(new Timestamp(date.getTime()), f1);

        Object f2 = rs.getObject("f2");
        Assert.assertEquals(Integer.class, f2.getClass());
        Assert.assertEquals(1, f2);

        Object f3 = rs.getObject("f3");
        Assert.assertEquals(Long.class, f3.getClass());
        Assert.assertEquals(100l, f3);

        Object f4 = rs.getObject("f4");
        Assert.assertEquals(Float.class, f4.getClass());
        Assert.assertEquals(3.1415f, f4);

        Object f5 = rs.getObject("f5");
        Assert.assertEquals(Double.class, f5.getClass());
        Assert.assertEquals(3.1415926, f5);

        Object f6 = rs.getObject("f6");
        Assert.assertEquals(byte[].class, f6.getClass());
        Assert.assertEquals("abc", new String((byte[]) f6));

        Object f7 = rs.getObject("f7");
        Assert.assertEquals(Short.class, f7.getClass());
        Assert.assertEquals((short) 10, f7);

        Object f8 = rs.getObject("f8");
        Assert.assertEquals(Byte.class, f8.getClass());
        Assert.assertEquals((byte) 10, f8);

        Object f9 = rs.getObject("f9");
        Assert.assertEquals(Boolean.class, f9.getClass());
        Assert.assertEquals(true, f9);

        Object f10 = rs.getObject("f10");
        Assert.assertEquals(String.class, f10.getClass());
        Assert.assertEquals("涛思数据", f10);
    }

    @Test(expected = SQLException.class)
    public void findColumn() throws SQLException {
        int columnIndex = rs.findColumn("f1");
        Assert.assertEquals(1, columnIndex);
        columnIndex = rs.findColumn("f2");
        Assert.assertEquals(2, columnIndex);
        columnIndex = rs.findColumn("f3");
        Assert.assertEquals(3, columnIndex);
        columnIndex = rs.findColumn("f4");
        Assert.assertEquals(4, columnIndex);
        columnIndex = rs.findColumn("f5");
        Assert.assertEquals(5, columnIndex);
        columnIndex = rs.findColumn("f6");
        Assert.assertEquals(6, columnIndex);
        columnIndex = rs.findColumn("f7");
        Assert.assertEquals(7, columnIndex);
        columnIndex = rs.findColumn("f8");
        Assert.assertEquals(8, columnIndex);
        columnIndex = rs.findColumn("f9");
        Assert.assertEquals(9, columnIndex);
        columnIndex = rs.findColumn("f10");
        Assert.assertEquals(10, columnIndex);

        rs.findColumn("f11");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getCharacterStream() throws SQLException {
        rs.getCharacterStream(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isBeforeFirst() throws SQLException {
        rs.isBeforeFirst();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isAfterLast() throws SQLException {
        rs.isAfterLast();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isFirst() throws SQLException {
        rs.isFirst();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void isLast() throws SQLException {
        rs.isLast();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void first() throws SQLException {
        rs.first();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void last() throws SQLException {
        rs.last();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRow() throws SQLException {
        int row = rs.getRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void absolute() throws SQLException {
        rs.absolute(-1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void relative() throws SQLException {
        rs.relative(-1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void previous() throws SQLException {
        rs.previous();
    }

    @Test
    public void setFetchDirection() throws SQLException {
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        Assert.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        Assert.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    public void getFetchDirection() throws SQLException {
        Assert.assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    public void setFetchSize() throws SQLException {
        rs.setFetchSize(0);
        Assert.assertEquals(0, rs.getFetchSize());
    }

    @Test
    public void getFetchSize() throws SQLException {
        Assert.assertEquals(0, rs.getFetchSize());
    }

    @Test
    public void getType() throws SQLException {
        Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    }

    @Test
    public void getConcurrency() throws SQLException {
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void rowUpdated() throws SQLException {
        rs.rowUpdated();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void rowInserted() throws SQLException {
        rs.rowInserted();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void rowDeleted() throws SQLException {
        rs.rowDeleted();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNull() throws SQLException {
        rs.updateNull("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBoolean() throws SQLException {
        rs.updateBoolean(1, false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateByte() throws SQLException {
        rs.updateByte(1, new Byte("0"));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateShort() throws SQLException {
        rs.updateShort(1, new Short("0"));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateInt() throws SQLException {
        rs.updateInt(1, 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateLong() throws SQLException {
        rs.updateLong(1, 1l);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateFloat() throws SQLException {
        rs.updateFloat(1, 1f);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDouble() throws SQLException {
        rs.updateDouble(1, 1.0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBigDecimal() throws SQLException {
        rs.updateBigDecimal(1, new BigDecimal(1));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateString() throws SQLException {
        rs.updateString(1, "abc");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBytes() throws SQLException {
        rs.updateBytes(1, new byte[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateDate() throws SQLException {
        rs.updateDate(1, new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTime() throws SQLException {
        rs.updateTime(1, new Time(System.currentTimeMillis()));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateTimestamp() throws SQLException {
        rs.updateTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream() throws SQLException {
        rs.updateAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream() throws SQLException {
        rs.updateBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream() throws SQLException {
        rs.updateCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateObject() throws SQLException {
        rs.updateObject(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    @Test
    public void getStatement() throws SQLException {
        Statement stmt = rs.getStatement();
        Assert.assertNotNull(stmt);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRef() throws SQLException {
        rs.getRef(1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBlob() throws SQLException {
        rs.getBlob("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getClob() throws SQLException {
        rs.getClob("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getArray() throws SQLException {
        rs.getArray("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getURL() throws SQLException {
        rs.getURL("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRef() throws SQLException {
        rs.updateRef("f1", null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateBlob() throws SQLException {
        rs.updateBlob(1, (InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateClob() throws SQLException {
        rs.updateClob(1, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateArray() throws SQLException {
        rs.updateArray(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getRowId() throws SQLException {
        rs.getRowId("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateRowId() throws SQLException {
        rs.updateRowId(1, null);
    }

    @Test
    public void getHoldability() throws SQLException {
        Assert.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
    }

    @Test
    public void isClosed() throws SQLException {
        Assert.assertFalse(rs.isClosed());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNString() throws SQLException {
        rs.updateNString(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNClob() throws SQLException {
        rs.updateNClob(1, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNClob() throws SQLException {
        rs.getNClob("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getSQLXML() throws SQLException {
        rs.getSQLXML("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateSQLXML() throws SQLException {
        rs.updateSQLXML(1, null);
    }

    @Test
    public void getNString() throws SQLException {
        String f10 = rs.getNString("f10");
        Assert.assertEquals("涛思数据", f10);
        f10 = rs.getNString(10);
        Assert.assertEquals("涛思数据", f10);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getNCharacterStream() throws SQLException {
        rs.getNCharacterStream("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream() throws SQLException {
        rs.updateNCharacterStream(1, null);
    }

    @Test
    public void unwrap() throws SQLException {
        TSDBResultSet unwrap = rs.unwrap(TSDBResultSet.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(rs.isWrapperFor(TSDBResultSet.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            stmt = conn.createStatement();
            stmt.execute("create database if not exists restful_test");
            stmt.execute("use restful_test");
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 binary(64), f7 smallint, f8 tinyint, f9 bool, f10 nchar(64))");
            stmt.execute("insert into restful_test.weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, 'abc', 10, 10, true, '涛思数据')");
            rs = stmt.executeQuery("select * from restful_test.weather");
            rs.next();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

    }

    @AfterClass
    public static void afterClass() {
        try {
            if (rs != null)
                rs.close();
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}