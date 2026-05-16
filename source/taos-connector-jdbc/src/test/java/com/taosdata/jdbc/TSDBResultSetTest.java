package com.taosdata.jdbc;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TSDBResultSetTest {

    private static Connection conn;
    private static Statement stmt;
    private static ResultSet rs;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(TSDBResultSetTest.class);

    @Test
    public void wasNull() throws SQLException {
        Assert.assertFalse(rs.wasNull());
    }

    @Test
    public void getString() throws SQLException {
        String f10 = rs.getString("f10");
        assertEquals("涛思数据", f10);
        f10 = rs.getString(10);
        assertEquals("涛思数据", f10);
    }

    @Test
    public void getBoolean() throws SQLException {
        Boolean f9 = rs.getBoolean("f9");
        assertEquals(true, f9);
        f9 = rs.getBoolean(9);
        assertEquals(true, f9);
    }

    @Test
    public void getByte() throws SQLException {
        byte f8 = rs.getByte("f8");
        assertEquals(10, f8);
        f8 = rs.getByte(8);
        assertEquals(10, f8);
    }

    @Test
    public void getShort() throws SQLException {
        short f7 = rs.getShort("f7");
        assertEquals(10, f7);
        f7 = rs.getShort(7);
        assertEquals(10, f7);
    }

    @Test
    public void getInt() throws SQLException {
        int f2 = rs.getInt("f2");
        assertEquals(1, f2);
        f2 = rs.getInt(2);
        assertEquals(1, f2);
    }

    @Test
    public void getLong() throws SQLException {
        long f3 = rs.getLong("f3");
        assertEquals(100, f3);
        f3 = rs.getLong(3);
        assertEquals(100, f3);
    }

    @Test
    public void getFloat() throws SQLException {
        float f4 = rs.getFloat("f4");
        assertEquals(3.1415f, f4, 0f);
        f4 = rs.getFloat(4);
        assertEquals(3.1415f, f4, 0f);
    }

    @Test
    public void getDouble() throws SQLException {
        double f5 = rs.getDouble("f5");
        assertEquals(3.1415926, f5, 0.0);
        f5 = rs.getDouble(5);
        assertEquals(3.1415926, f5, 0.0);
    }

    @Test
    public void getBigDecimal() throws SQLException, ParseException {
        BigDecimal f1 = rs.getBigDecimal("f1");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        java.util.Date parse = simpleDateFormat.parse("2021-01-01 00:00:00.000");
        assertEquals(parse.getTime(), f1.longValue());

        BigDecimal f2 = rs.getBigDecimal("f2");
        assertEquals(1, f2.intValue());

        BigDecimal f3 = rs.getBigDecimal("f3");
        assertEquals(100L, f3.longValue());

        BigDecimal f4 = rs.getBigDecimal("f4");
        assertEquals(3.1415f, f4.floatValue(), 0.00000f);

        BigDecimal f5 = rs.getBigDecimal("f5");
        assertEquals(3.1415926, f5.doubleValue(), 0.0000000);

        BigDecimal f7 = rs.getBigDecimal("f7");
        assertEquals(10, f7.intValue());

        BigDecimal f8 = rs.getBigDecimal("f8");
        assertEquals(10, f8.intValue());
    }

    @Test
    public void getBytes() throws SQLException {
        byte[] f1 = rs.getBytes("f1");
        assertEquals("2021-01-01 00:00:00.0", new String(f1));

        byte[] f2 = rs.getBytes("f2");
        assertEquals(1, Ints.fromByteArray(f2));

        byte[] f3 = rs.getBytes("f3");
        assertEquals(100L, Longs.fromByteArray(f3));

        byte[] f4 = rs.getBytes("f4");
        assertEquals(3.1415f, Float.parseFloat(new String(f4)), 0.000000f);

        byte[] f5 = rs.getBytes("f5");
        assertEquals(3.1415926, Double.parseDouble(new String(f5)), 0.000000f);

        byte[] f6 = rs.getBytes("f6");
        Assert.assertTrue(Arrays.equals("abc".getBytes(), f6));

        byte[] f7 = rs.getBytes("f7");
        assertEquals((short) 10, Shorts.fromByteArray(f7));

        byte[] f8 = rs.getBytes("f8");
        assertEquals(1, f8.length);
        assertEquals((byte) 10, f8[0]);

        byte[] f9 = rs.getBytes("f9");
        assertEquals("true", new String(f9));

        byte[] f10 = rs.getBytes("f10");
        assertEquals("涛思数据", new String(f10));
    }

    @Test
    public void getDate() throws SQLException, ParseException {
        Date f1 = rs.getDate("f1");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        assertEquals(sdf.parse("2021-01-01"), f1);
    }

    @Test
    public void getTime() throws SQLException {
        Time f1 = rs.getTime("f1");
        assertNotNull(f1);
        assertEquals("00:00:00", f1.toString());
    }

    @Test
    public void getTimestamp() throws SQLException {
        Timestamp f1 = rs.getTimestamp("f1");
        assertEquals("2021-01-01 00:00:00.0", f1.toString());
        f1 = rs.getTimestamp(1);
        assertEquals("2021-01-01 00:00:00.0", f1.toString());
    }

    @Test
    public void getGeometry() throws SQLException{
        byte[] f11 = rs.getBytes("f11");
        String result = printBytesByStringBuilder(f11);
        assertEquals(result, "0101000000000000000000F03F0000000000000040");
    }

    /**
        * 根据字节数组，输出对应的格式化字符串
        * @param bytes 字节数组
        * @return 字节数组字符串
        */
    public static String printBytesByStringBuilder(byte[] bytes){
        StringBuilder stringBuilder = new StringBuilder();

        for (byte aByte : bytes) {
            stringBuilder.append(byte2String(aByte));
        }
        return stringBuilder.toString();
    }

    public static String byte2String(byte b){
        return String.format("%02X",b);
    }

    @Test(expected = SQLException.class)
    public void getAsciiStream() throws SQLException {
        rs.getAsciiStream("f1");
    }

    @SuppressWarnings("deprecation")
    @Test(expected = SQLException.class)
    public void getUnicodeStream() throws SQLException {
        rs.getUnicodeStream("f1");
    }

    @Test(expected = SQLException.class)
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

    @Test(expected = SQLException.class)
    public void getCursorName() throws SQLException {
        rs.getCursorName();
    }

    @Test
    public void getMetaData() throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        assertNotNull(meta);
    }

    @Test
    public void getObject() throws SQLException, ParseException {
        Object f1 = rs.getObject("f1");
        assertEquals(Timestamp.class, f1.getClass());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.sss");
        java.util.Date date = sdf.parse("2021-01-01 00:00:00.000");
        assertEquals(new Timestamp(date.getTime()), f1);

        Object f2 = rs.getObject("f2");
        assertEquals(Integer.class, f2.getClass());
        assertEquals(1, f2);

        Object f3 = rs.getObject("f3");
        assertEquals(Long.class, f3.getClass());
        assertEquals(100L, f3);

        Object f4 = rs.getObject("f4");
        assertEquals(Float.class, f4.getClass());
        assertEquals(3.1415f, f4);

        Object f5 = rs.getObject("f5");
        assertEquals(Double.class, f5.getClass());
        assertEquals(3.1415926, f5);

        Object f6 = rs.getObject("f6");
        assertEquals(byte[].class, f6.getClass());
        assertEquals("abc", new String((byte[]) f6));

        Object f7 = rs.getObject("f7");
        assertEquals(Short.class, f7.getClass());
        assertEquals((short) 10, f7);

        Object f8 = rs.getObject("f8");
        assertEquals(Byte.class, f8.getClass());
        assertEquals((byte) 10, f8);

        Object f9 = rs.getObject("f9");
        assertEquals(Boolean.class, f9.getClass());
        assertEquals(true, f9);

        Object f10 = rs.getObject("f10");
        assertEquals(String.class, f10.getClass());
        assertEquals("涛思数据", f10);
    }

    @Test(expected = SQLException.class)
    public void findColumn() throws SQLException {
        int columnIndex = rs.findColumn("f1");
        assertEquals(1, columnIndex);
        columnIndex = rs.findColumn("f2");
        assertEquals(2, columnIndex);
        columnIndex = rs.findColumn("f3");
        assertEquals(3, columnIndex);
        columnIndex = rs.findColumn("f4");
        assertEquals(4, columnIndex);
        columnIndex = rs.findColumn("f5");
        assertEquals(5, columnIndex);
        columnIndex = rs.findColumn("f6");
        assertEquals(6, columnIndex);
        columnIndex = rs.findColumn("f7");
        assertEquals(7, columnIndex);
        columnIndex = rs.findColumn("f8");
        assertEquals(8, columnIndex);
        columnIndex = rs.findColumn("f9");
        assertEquals(9, columnIndex);
        columnIndex = rs.findColumn("f10");
        assertEquals(10, columnIndex);
        columnIndex = rs.findColumn("f11");
        assertEquals(11, columnIndex);

        rs.findColumn("f12");
    }

    @Test(expected = SQLException.class)
    public void getCharacterStream() throws SQLException {
        rs.getCharacterStream(1);
    }

    @Test(expected = SQLException.class)
    public void isBeforeFirst() throws SQLException {
        rs.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void isAfterLast() throws SQLException {
        rs.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void isFirst() throws SQLException {
        rs.isFirst();
    }

    @Test(expected = SQLException.class)
    public void isLast() throws SQLException {
        rs.isLast();
    }

    @Test(expected = SQLException.class)
    public void beforeFirst() throws SQLException {
        rs.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void afterLast() throws SQLException {
        rs.afterLast();
    }

    @Test(expected = SQLException.class)
    public void first() throws SQLException {
        rs.first();
    }

    @Test(expected = SQLException.class)
    public void last() throws SQLException {
        rs.last();
    }

    @Test(expected = SQLException.class)
    public void getRow() throws SQLException {
        rs.getRow();
    }

    @Test(expected = SQLException.class)
    public void absolute() throws SQLException {
        rs.absolute(-1);
    }

    @Test(expected = SQLException.class)
    public void relative() throws SQLException {
        rs.relative(-1);
    }

    @Test(expected = SQLException.class)
    public void previous() throws SQLException {
        rs.previous();
    }

    @Test
    public void setFetchDirection() throws SQLException {
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    public void getFetchDirection() throws SQLException {
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    public void setFetchSize() throws SQLException {
        rs.setFetchSize(0);
        assertEquals(0, rs.getFetchSize());
    }

    @Test
    public void getFetchSize() throws SQLException {
        assertEquals(0, rs.getFetchSize());
    }

    @Test
    public void getType() throws SQLException {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
    }

    @Test
    public void getConcurrency() throws SQLException {
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
    }

    @Test(expected = SQLException.class)
    public void rowUpdated() throws SQLException {
        rs.rowUpdated();
    }

    @Test(expected = SQLException.class)
    public void rowInserted() throws SQLException {
        rs.rowInserted();
    }

    @Test(expected = SQLException.class)
    public void rowDeleted() throws SQLException {
        rs.rowDeleted();
    }

    @Test(expected = SQLException.class)
    public void updateNull() throws SQLException {
        rs.updateNull("f1");
    }

    @Test(expected = SQLException.class)
    public void updateBoolean() throws SQLException {
        rs.updateBoolean(1, false);
    }

    @Test(expected = SQLException.class)
    public void updateByte() throws SQLException {
        rs.updateByte(1, (byte) 0);
    }

    @Test(expected = SQLException.class)
    public void updateShort() throws SQLException {
        rs.updateShort(1, (short) 0);
    }

    @Test(expected = SQLException.class)
    public void updateInt() throws SQLException {
        rs.updateInt(1, 1);
    }

    @Test(expected = SQLException.class)
    public void updateLong() throws SQLException {
        rs.updateLong(1, 1L);
    }

    @Test(expected = SQLException.class)
    public void updateFloat() throws SQLException {
        rs.updateFloat(1, 1f);
    }

    @Test(expected = SQLException.class)
    public void updateDouble() throws SQLException {
        rs.updateDouble(1, 1.0);
    }

    @Test(expected = SQLException.class)
    public void updateBigDecimal() throws SQLException {
        rs.updateBigDecimal(1, new BigDecimal(1));
    }

    @Test(expected = SQLException.class)
    public void updateString() throws SQLException {
        rs.updateString(1, "abc");
    }

    @Test(expected = SQLException.class)
    public void updateBytes() throws SQLException {
        rs.updateBytes(1, new byte[]{});
    }

    @Test(expected = SQLException.class)
    public void updateDate() throws SQLException {
        rs.updateDate(1, new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void updateTime() throws SQLException {
        rs.updateTime(1, new Time(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void updateTimestamp() throws SQLException {
        rs.updateTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void updateAsciiStream() throws SQLException {
        rs.updateAsciiStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void updateBinaryStream() throws SQLException {
        rs.updateBinaryStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void updateCharacterStream() throws SQLException {
        rs.updateCharacterStream(1, null);
    }

    @Test(expected = SQLException.class)
    public void updateObject() throws SQLException {
        rs.updateObject(1, null);
    }

    @Test(expected = SQLException.class)
    public void insertRow() throws SQLException {
        rs.insertRow();
    }

    @Test(expected = SQLException.class)
    public void updateRow() throws SQLException {
        rs.updateRow();
    }

    @Test(expected = SQLException.class)
    public void deleteRow() throws SQLException {
        rs.deleteRow();
    }

    @Test(expected = SQLException.class)
    public void refreshRow() throws SQLException {
        rs.refreshRow();
    }

    @Test(expected = SQLException.class)
    public void cancelRowUpdates() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Test(expected = SQLException.class)
    public void moveToInsertRow() throws SQLException {
        rs.moveToInsertRow();
    }

    @Test
    public void getStatement() throws SQLException {
        Statement stmt = rs.getStatement();
        assertNotNull(stmt);
    }

    @Test(expected = SQLException.class)
    public void moveToCurrentRow() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Test(expected = SQLException.class)
    public void getRef() throws SQLException {
        rs.getRef(1);
    }

    @Test(expected = SQLException.class)
    public void getBlob() throws SQLException {
        rs.getBlob("f1");
    }

    @Test(expected = SQLException.class)
    public void getClob() throws SQLException {
        rs.getClob("f1");
    }

    @Test(expected = SQLException.class)
    public void getArray() throws SQLException {
        rs.getArray("f1");
    }

    @Test(expected = SQLException.class)
    public void getURL() throws SQLException {
        rs.getURL("f1");
    }

    @Test(expected = SQLException.class)
    public void updateRef() throws SQLException {
        rs.updateRef("f1", null);
    }

    @Test(expected = SQLException.class)
    public void updateBlob() throws SQLException {
        rs.updateBlob(1, (InputStream) null);
    }

    @Test(expected = SQLException.class)
    public void updateClob() throws SQLException {
        rs.updateClob(1, (Reader) null);
    }

    @Test(expected = SQLException.class)
    public void updateArray() throws SQLException {
        rs.updateArray(1, null);
    }

    @Test(expected = SQLException.class)
    public void getRowId() throws SQLException {
        rs.getRowId("f1");
    }

    @Test(expected = SQLException.class)
    public void updateRowId() throws SQLException {
        rs.updateRowId(1, null);
    }

    @Test
    public void getHoldability() throws SQLException {
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
    }

    @Test
    public void isClosed() throws SQLException {
        Assert.assertFalse(rs.isClosed());
    }

    @Test(expected = SQLException.class)
    public void updateNString() throws SQLException {
        rs.updateNString(1, null);
    }

    @Test(expected = SQLException.class)
    public void updateNClob() throws SQLException {
        rs.updateNClob(1, (Reader) null);
    }

    @Test(expected = SQLException.class)
    public void getNClob() throws SQLException {
        rs.getNClob("f1");
    }

    @Test(expected = SQLException.class)
    public void getSQLXML() throws SQLException {
        rs.getSQLXML("f1");
    }

    @Test(expected = SQLException.class)
    public void updateSQLXML() throws SQLException {
        rs.updateSQLXML(1, null);
    }

    @Test
    public void getNString() throws SQLException {
        String f10 = rs.getNString("f10");
        assertEquals("涛思数据", f10);
        f10 = rs.getNString(10);
        assertEquals("涛思数据", f10);
    }

    @Test(expected = SQLException.class)
    public void getNCharacterStream() throws SQLException {
        rs.getNCharacterStream("f1");
    }

    @Test(expected = SQLException.class)
    public void updateNCharacterStream() throws SQLException {
        rs.updateNCharacterStream(1, null);
    }

    @Test
    public void unwrap() throws SQLException {
        TSDBResultSet unwrap = rs.unwrap(TSDBResultSet.class);
        assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(rs.isWrapperFor(TSDBResultSet.class));
    }
    @Test
    public void testCheckAvailability() throws SQLException {
        AbstractResultSet rs = (AbstractResultSet) this.rs;
        // 测试正常范围
        rs.checkAvailability(1, 11); // 因为表有11列

        // 测试列索引小于1
        try {
            rs.checkAvailability(0, 11);
            Assert.fail("应该抛出SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }

        // 测试列索引超出范围
        try {
            rs.checkAvailability(12, 11);
            Assert.fail("应该抛出SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }
    }

    @Test
    public void testSetTimestampPrecision() throws SQLException {
        AbstractResultSet rs = (AbstractResultSet) this.rs;
        rs.setTimestampPrecision(6);
        // 验证精度设置是否生效
        Timestamp ts = rs.getTimestamp(1);
        assertEquals("2021-01-01 00:00:00.0", ts.toString());
    }

    @Test
    public void testGetTimestamp() throws SQLException {
        AbstractResultSet rs = (AbstractResultSet) this.rs;
        Timestamp ts = rs.getTimestamp(3);
        assertEquals(100, ts.getTime());
    }
    @Test(expected = RuntimeException.class)
    public void testGetTimestamp3() throws SQLException {
        AbstractResultSet rs = (AbstractResultSet) this.rs;
        rs.getTimestamp(6);
    }

    @Test
    public void testGetForkJoinPool() {
        AbstractResultSet rs = (AbstractResultSet) this.rs;
        assertNotNull(rs.getForkJoinPool());
    }

    @Test
    public void testClosedOperations() throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet newRs = statement.executeQuery("select * from weather");
        newRs.close();

        // 测试关闭后的操作
        try {
            newRs.getMetaData();
            Assert.fail("Should throw SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, e.getErrorCode());
        }

        try {
            newRs.setFetchSize(100);
            Assert.fail("Should throw SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, e.getErrorCode());
        }

        try {
            newRs.getFetchSize();
            Assert.fail("Should throw SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_STATEMENT_CLOSED, e.getErrorCode());
        }

        statement.close();
    }

    @Test
    public void testGetDateTimeWithCalendar() throws SQLException {
        Calendar cal = Calendar.getInstance();
        Timestamp ts = rs.getTimestamp(1, cal);
        assertNotNull(ts);
        assertEquals("2021-01-01 00:00:00.0", ts.toString());
    }
    @Test(expected = SQLException.class)
    public void testGetDateTimeWithCalendar2() throws SQLException {
        Calendar cal = Calendar.getInstance();
        rs.getDate(1, cal);
    }
    @Test(expected = SQLException.class)
    public void testGetDateTimeWithCalendar3() throws SQLException {
        Calendar cal = Calendar.getInstance();
        rs.getTime(1, cal);
    }

    @Test
    public void testSetInvalidFetchSize() throws SQLException {
        try {
            rs.setFetchSize(-1);
            Assert.fail("Should throw SQLException");
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_INVALID_VARIABLE, e.getErrorCode());
        }
    }

    @Test
    public void testGetBigDecimalDeprecated() throws SQLException {
        BigDecimal bd = rs.getBigDecimal(2, 2); // 获取第2列(INT类型)的值,指定2位小数
        assertEquals(new BigDecimal("1.00").doubleValue(), bd.doubleValue(), 0.0000001);
    }

    @Test(expected = SQLException.class)
    public void testGetObjectByLabelAndType() throws SQLException {
        rs.getObject("f1", Timestamp.class);
    }

    @Test
    public void testUpdateMethodsWithLabel() throws SQLException {
        // 测试所有带列名的update方法
        try {
            rs.updateNull("f1");
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateBoolean("f1", true);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateByte("f1", (byte)1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateShort("f1", (short)1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateInt("f1", 1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateLong("f1", 1L);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateFloat("f1", 1.0f);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateDouble("f1", 1.0);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
    }

    @Test
    public void testStreamOperations() throws SQLException {
        try {
            rs.updateAsciiStream("f1", null, 1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateBinaryStream("f1", null, 1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateCharacterStream("f1", null, 1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateAsciiStream("f1", null, 1L);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateBinaryStream("f1", null, 1L);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateCharacterStream("f1", null, 1L);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
    }

    @Test
    public void testClobOperations() throws SQLException {
        try {
            rs.updateClob(1, (Clob)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateClob("f1", (Clob)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateNClob(1, (NClob)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.updateNClob("f1", (NClob)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
    }

    @Test
    public void testClosedResultSetMethodCalls() throws SQLException {
        Statement statement = conn.createStatement();
        ResultSet newRs = statement.executeQuery("select * from " + DB_NAME + ".weather");
        newRs.close();

        try {
            newRs.next();
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, e.getErrorCode());
        }

        try {
            newRs.getString(1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, e.getErrorCode());
        }

        try {
            newRs.getBoolean(1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, e.getErrorCode());
        }

        try {
            newRs.getByte(1);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED, e.getErrorCode());
        }
        statement.close();
    }

    @Test
    public void testInvalidColumnIndexOperations() throws SQLException {
        try {
            rs.getString(0);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }

        try {
            rs.getString(100);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }

        try {
            rs.getBoolean(0);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }

        try {
            rs.getBoolean(100);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_PARAMETER_INDEX_OUT_RANGE, e.getErrorCode());
        }
    }

    @Test
    public void testGetObjectWithMap() throws SQLException {
        try {
            rs.getObject(1, (Map<String,Class<?>>)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }

        try {
            rs.getObject("f1", (Map<String,Class<?>>)null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
    }

    @Test
    public void testGetDateTimeWithNullCalendar() throws SQLException {
        try {
            rs.getDate(1, null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
        try {
            rs.getTime(1, null);
            Assert.fail();
        } catch (SQLException e) {
            assertEquals(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD, e.getErrorCode());
        }
    }

    @Test
    public void testSetFetchDirectionInvalid() throws SQLException {
        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
        // Should still be FETCH_FORWARD as other directions are not supported
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    @Test
    public void getColumnClassName() throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals(Timestamp.class.getName(), meta.getColumnClassName(1));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getJniUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "C");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            conn = DriverManager.getConnection(url, properties);
            stmt = conn.createStatement();
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
            stmt.execute("drop table if exists weather");
            stmt.execute("create table if not exists weather(f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 binary(64), f7 smallint, f8 tinyint, f9 bool, f10 nchar(64), f11 GEOMETRY(50))");
            stmt.execute("insert into weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, 'abc', 10, 10, true, '涛思数据', 'POINT(1 2)')");
            rs = stmt.executeQuery("select * from weather");
            rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (rs != null){
                rs.close();
            }
            if (stmt != null)
                stmt.close();
            if (conn != null) {
                Statement statement = conn.createStatement();

                statement.execute("drop database if exists " + DB_NAME);
                statement.close();
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

