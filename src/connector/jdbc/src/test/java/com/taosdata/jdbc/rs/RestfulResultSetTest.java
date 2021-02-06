package com.taosdata.jdbc.rs;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;

public class RestfulResultSetTest {

    private static final String host = "127.0.0.1";
//    private static final String host = "master";

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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getByte() throws SQLException {
        rs.getByte(1);
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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBigDecimal() throws SQLException {
        rs.getBigDecimal("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getBytes() throws SQLException {
        rs.getBytes("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getDate() throws SQLException {
        rs.getDate("f1");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getTime() throws SQLException {
        rs.getTime("f1");
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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getObject() throws SQLException {
        rs.getObject("f1");
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

    @Test
    public void isBeforeFirst() throws SQLException {
        Assert.assertFalse(rs.isBeforeFirst());
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        rs.next();
    }

    @Test
    public void isAfterLast() throws SQLException {
        Assert.assertFalse(rs.isAfterLast());
    }

    @Test
    public void isFirst() throws SQLException {
        Assert.assertTrue(rs.isFirst());
    }

    @Test
    public void isLast() throws SQLException {
        Assert.assertTrue(rs.isLast());
    }

    @Test
    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
        Assert.assertTrue(rs.isBeforeFirst());
        rs.next();
    }

    @Test
    public void afterLast() throws SQLException {
        rs.afterLast();
        Assert.assertTrue(rs.isAfterLast());
        rs.first();
    }

    @Test
    public void first() throws SQLException {
        rs.first();
        Assert.assertEquals("2021-01-01 00:00:00.0", rs.getTimestamp("f1").toString());
    }

    @Test
    public void last() throws SQLException {
        rs.last();
        Assert.assertEquals("2021-01-01 00:00:00.0", rs.getTimestamp("f1").toString());
    }

    @Test
    public void getRow() throws SQLException {
        int row = rs.getRow();
        Assert.assertEquals(1, row);
        rs.beforeFirst();
        row = rs.getRow();
        Assert.assertEquals(0, row);
        rs.first();
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

    @Test(expected = SQLFeatureNotSupportedException.class)
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
        RestfulResultSet unwrap = rs.unwrap(RestfulResultSet.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(rs.isWrapperFor(RestfulResultSet.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=root&password=taosdata");
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