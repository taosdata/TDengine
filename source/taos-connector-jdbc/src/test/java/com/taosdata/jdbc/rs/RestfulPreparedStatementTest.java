package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.Calendar;

public class RestfulPreparedStatementTest {
    private static Connection conn;
    private static final String DB_NAME = TestUtils.camelToSnake(RestfulPreparedStatementTest.class);

    static final String HOST = TestEnvUtil.getHost();
    private static final String SQL_INSERT = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static PreparedStatement pstmtInsert;
    private static final String SQL_SELECT = "select * from t1 where ts > ? and ts <= ? and f1 >= ?";
    private static PreparedStatement pstmtSelect;
    private static final String SQL_WITHOUT_PARAMETERS = "select count(*) from t1";
    private static PreparedStatement pstmtWithoutParameters;

    @Test
    public void executeQuery() throws SQLException {
        long end = System.currentTimeMillis();
        long start = end - 1000 * 60 * 60;
        pstmtSelect.setTimestamp(1, new Timestamp(start));
        pstmtSelect.setTimestamp(2, new Timestamp(end));
        pstmtSelect.setInt(3, 0);

        ResultSet rs = pstmtSelect.executeQuery();
        Assert.assertNotNull(rs);
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        Assert.assertEquals(10, columnCount);
        Assert.assertNotNull(rs);
    }

    @Test
    public void executeUpdate() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setFloat(4, 3.14f);
        int result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);
    }

    @Test
    public void setNull() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(2, Types.INTEGER);
        int result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(3, Types.BIGINT);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(4, Types.FLOAT);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(5, Types.DOUBLE);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(6, Types.SMALLINT);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(7, Types.TINYINT);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(8, Types.BOOLEAN);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(9, Types.BINARY);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(10, Types.NCHAR);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setNull(10, Types.OTHER);
        result = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, result);
    }

    @Test
    public void setBoolean() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setBoolean(8, true);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setByte() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setByte(7, (byte) 0x001);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setShort() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setShort(6, (short) 2);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setInt() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setInt(2, 10086);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setLong() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setLong(3, Long.MAX_VALUE);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setFloat() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setFloat(4, 3.14f);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void setDouble() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setDouble(5, 3.14444);
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBigDecimal() throws SQLException {
        pstmtInsert.setBigDecimal(1, null);
    }

    @Test
    public void setString() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setString(10, "aaaa");
        boolean execute = pstmtInsert.execute();
        Assert.assertFalse(execute);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setString(10, new Person("john", 33, true).toString());
        Assert.assertFalse(pstmtInsert.execute());

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setString(10, new Person("john", 33, true).toString().replaceAll("'", "\""));
        Assert.assertFalse(pstmtInsert.execute());
    }

    private class Person {
        final String name;
        final int age;
        final boolean sex;

        public Person(String name, int age, boolean sex) {
            this.name = name;
            this.age = age;
            this.sex = sex;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex=" + sex +
                    '}';
        }
    }

    @Test
    public void setBytes() throws SQLException, IOException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));

//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ObjectOutputStream oos = new ObjectOutputStream(baos);
//        oos.writeObject(new Person("john", 33, true));
//        oos.flush();
//        byte[] bytes = baos.toByteArray();
//        pstmt_insert.setBytes(9, bytes);

        pstmtInsert.setBytes(9, new Person("john", 33, true).toString().getBytes());
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setDate() throws SQLException {
        pstmtInsert.setDate(1, new Date(System.currentTimeMillis()));
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTime() throws SQLException {
        pstmtInsert.setTime(1, new Time(System.currentTimeMillis()));
    }

    @Test
    public void setTimestamp() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream() throws SQLException {
        pstmtInsert.setAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream() throws SQLException {
        pstmtInsert.setBinaryStream(1, null);
    }

    @Test
    public void clearParameters() throws SQLException {
        pstmtInsert.clearParameters();
        pstmtWithoutParameters.clearParameters();
    }

    @Test
    public void setObject() throws SQLException {
        pstmtInsert.setObject(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(2, 111);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(3, Long.MAX_VALUE);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(4, 3.14159265354f);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(5, Double.MAX_VALUE);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(6, Short.MAX_VALUE);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(7, Byte.MAX_VALUE);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(8, true);
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(9, "hello".getBytes());
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.setObject(10, "Hello");
        ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);
    }

    @Test
    public void execute() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        int ret = pstmtInsert.executeUpdate();
        Assert.assertEquals(1, ret);

        executeQuery();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream() throws SQLException {
        pstmtInsert.setCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRef() throws SQLException {
        pstmtInsert.setRef(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob() throws SQLException {
        pstmtInsert.setBlob(1, (Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob() throws SQLException {
        pstmtInsert.setClob(1, (Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setArray() throws SQLException {
        pstmtInsert.setArray(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getMetaData() throws SQLException {
        pstmtInsert.getMetaData();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setURL() throws SQLException {
        pstmtInsert.setURL(1, null);
    }

    @Test
    public void getParameterMetaData() throws SQLException {
        ParameterMetaData parameterMetaData = pstmtInsert.getParameterMetaData();
        Assert.assertNotNull(parameterMetaData);
        //TODO: modify the test case
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRowId() throws SQLException {
        pstmtInsert.setRowId(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNString() throws SQLException {
        pstmtInsert.setNString(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream() throws SQLException {
        pstmtInsert.setNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob() throws SQLException {
        pstmtInsert.setNClob(1, (NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSQLXML() throws SQLException {
        pstmtInsert.setSQLXML(1, null);
    }

    // Test methods after close()
    @Test(expected = SQLException.class)
    public void testExecuteQueryAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.executeQuery();
    }

    @Test(expected = SQLException.class)
    public void testExecuteUpdateAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.executeUpdate();
    }

    @Test(expected = SQLException.class)
    public void testSetNullAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setNull(1, Types.TIMESTAMP);
    }

    @Test(expected = SQLException.class)
    public void testSetBooleanAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setBoolean(8, true);
    }

    @Test(expected = SQLException.class)
    public void testSetByteAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setByte(7, (byte) 1);
    }

    @Test(expected = SQLException.class)
    public void testSetShortAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setShort(6, (short) 2);
    }

    @Test(expected = SQLException.class)
    public void testSetIntAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setInt(2, 10086);
    }

    @Test(expected = SQLException.class)
    public void testSetLongAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setLong(3, Long.MAX_VALUE);
    }

    @Test(expected = SQLException.class)
    public void testSetFloatAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setFloat(4, 3.14f);
    }

    @Test(expected = SQLException.class)
    public void testSetDoubleAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setDouble(5, 3.14444);
    }

    @Test(expected = SQLException.class)
    public void testSetStringAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setString(10, "test");
    }

    @Test(expected = SQLException.class)
    public void testSetBytesAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setBytes(9, "test".getBytes());
    }

    @Test(expected = SQLException.class)
    public void testSetTimestampAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testClearParametersAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.clearParameters();
    }

    @Test(expected = SQLException.class)
    public void testSetObjectAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.setObject(1, new Timestamp(System.currentTimeMillis()));
    }

    @Test(expected = SQLException.class)
    public void testExecuteAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.execute();
    }

    @Test(expected = SQLException.class)
    public void testAddBatchAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.addBatch();
    }

    @Test(expected = SQLException.class)
    public void testGetParameterMetaDataAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.getParameterMetaData();
    }

    // Test invalid parameter index
    @Test(expected = SQLException.class)
    public void testSetObjectWithInvalidParameterIndex() throws SQLException {
        pstmtInsert.setObject(0, "test");
    }

    @Test(expected = SQLException.class)
    public void testSetObjectWithOutOfRangeIndex() throws SQLException {
        pstmtInsert.setObject(11, "test");
    }

    // Test more unsupported methods
    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setUnicodeStream() throws SQLException {
        pstmtInsert.setUnicodeStream(1, null, 0);
    }
    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setObjectWithTargetSqlTypeAndScale() throws SQLException {
        pstmtInsert.setObject(1, "test", Types.VARCHAR, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setDateWithCalendar() throws SQLException {
        pstmtInsert.setDate(1, new Date(System.currentTimeMillis()), Calendar.getInstance());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTimeWithCalendar() throws SQLException {
        pstmtInsert.setTime(1, new Time(System.currentTimeMillis()), Calendar.getInstance());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTimestampWithCalendar() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()), Calendar.getInstance());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNullWithTypeName() throws SQLException {
        pstmtInsert.setNull(1, Types.VARCHAR, "VARCHAR");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStreamWithLength() throws SQLException {
        pstmtInsert.setAsciiStream(1, null, 100);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStreamWithLongLength() throws SQLException {
        pstmtInsert.setAsciiStream(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStreamWithLongLength() throws SQLException {
        pstmtInsert.setBinaryStream(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStreamWithLongLength() throws SQLException {
        pstmtInsert.setCharacterStream(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStreamWithoutLength() throws SQLException {
        pstmtInsert.setAsciiStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStreamWithoutLength() throws SQLException {
        pstmtInsert.setBinaryStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStreamWithoutLength() throws SQLException {
        pstmtInsert.setCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStreamWithoutLength() throws SQLException {
        pstmtInsert.setNCharacterStream(1, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClobWithReader() throws SQLException {
        pstmtInsert.setClob(1, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlobWithInputStream() throws SQLException {
        pstmtInsert.setBlob(1, (InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClobWithReader() throws SQLException {
        pstmtInsert.setNClob(1, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClobWithReaderAndLength() throws SQLException {
        pstmtInsert.setClob(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlobWithInputStreamAndLength() throws SQLException {
        pstmtInsert.setBlob(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClobWithReaderAndLength() throws SQLException {
        pstmtInsert.setNClob(1, null, 100L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStreamWithLength() throws SQLException {
        pstmtInsert.setNCharacterStream(1, null, 100L);
    }

    // Test addBatch method
    @Test
    public void testAddBatch() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.addBatch();

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis() + 1000));
        pstmtInsert.addBatch();

        int[] results = pstmtInsert.executeBatch();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.length);
    }

    // Test executeBatch method
    @Test
    public void testExecuteBatch() throws SQLException {
        pstmtInsert.clearBatch();
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.addBatch();

        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis() + 1000));
        pstmtInsert.addBatch();

        int[] results = pstmtInsert.executeBatch();
        Assert.assertNotNull(results);
        Assert.assertEquals(2, results.length);
        Assert.assertEquals(1, results[0]);
        Assert.assertEquals(1, results[1]);
    }

    // Test executeBatch after close
    @Test(expected = SQLException.class)
    public void testExecuteBatchAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.executeBatch();
    }

    // Test clearBatch method
    @Test(expected = SQLException.class)
    public void testClearBatch() throws SQLException {
        pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        pstmtInsert.addBatch();
        pstmtInsert.clearBatch();

        pstmtInsert.executeBatch();
    }

    // Test clearBatch after close
    @Test(expected = SQLException.class)
    public void testClearBatchAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.clearBatch();
    }

    // Test for Statement interface methods
    @Test(expected = SQLException.class)
    public void testExecuteQueryStringAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.executeQuery(SQL_SELECT);
    }

    @Test(expected = SQLException.class)
    public void testExecuteUpdateStringAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.executeUpdate(SQL_INSERT);
    }

    @Test(expected = SQLException.class)
    public void testExecuteStringAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.execute(SQL_INSERT);
    }

    @Test(expected = SQLException.class)
    public void testAddBatchStringAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.addBatch(SQL_INSERT);
    }

    @Test(expected = SQLException.class)
    public void testSetFetchSizeAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setFetchSize(100);
    }

    @Test(expected = SQLException.class)
    public void testGetFetchSizeAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getFetchSize();
    }

    @Test(expected = SQLException.class)
    public void testSetMaxRowsAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setMaxRows(100);
    }

    @Test(expected = SQLException.class)
    public void testGetMaxRowsAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getMaxRows();
    }

    @Test(expected = SQLException.class)
    public void testSetQueryTimeoutAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setQueryTimeout(30);
    }

    @Test(expected = SQLException.class)
    public void testGetQueryTimeoutAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getQueryTimeout();
    }

    @Test(expected = SQLException.class)
    public void testCancelAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.cancel();
    }

    @Test(expected = SQLException.class)
    public void testGetWarningsAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getWarnings();
    }

    @Test(expected = SQLException.class)
    public void testClearWarningsAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.clearWarnings();
    }

    @Test(expected = SQLException.class)
    public void testSetCursorNameAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setCursorName("cursor");
    }

    @Test(expected = SQLException.class)
    public void testGetResultSetHoldabilityAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getResultSetHoldability();
    }

    @Test
    public void testIsClosed() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        Assert.assertFalse(stmt.isClosed());
        stmt.close();
        Assert.assertTrue(stmt.isClosed());
    }

    @Test(expected = SQLException.class)
    public void testSetPoolableAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setPoolable(true);
    }

    @Test(expected = SQLException.class)
    public void testIsPoolableAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.isPoolable();
    }

    @Test(expected = SQLException.class)
    public void testCloseOnCompletionAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.closeOnCompletion();
    }

    @Test(expected = SQLException.class)
    public void testIsCloseOnCompletionAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.isCloseOnCompletion();
    }

    @Test(expected = SQLException.class)
    public void testGetGeneratedKeysAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_INSERT);
        stmt.close();
        stmt.getGeneratedKeys();
    }

    @Test(expected = SQLException.class)
    public void testGetUpdateCountAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getUpdateCount();
    }

    @Test(expected = SQLException.class)
    public void testGetMoreResultsAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getMoreResults();
    }

    @Test(expected = SQLException.class)
    public void testGetMoreResultsWithFlagAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Test(expected = SQLException.class)
    public void testGetResultSetAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getResultSet();
    }

    @Test(expected = SQLException.class)
    public void testGetResultSetTypeAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getResultSetType();
    }

    @Test(expected = SQLException.class)
    public void testGetResultSetConcurrencyAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getResultSetConcurrency();
    }

    @Test(expected = SQLException.class)
    public void testGetFetchDirectionAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getFetchDirection();
    }

    @Test(expected = SQLException.class)
    public void testSetFetchDirectionAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Test(expected = SQLException.class)
    public void testGetConnectionAfterClose() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SQL_SELECT);
        stmt.close();
        stmt.getConnection();
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getRestUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            conn = DriverManager.getConnection(url);

            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + DB_NAME);
            stmt.execute("create database if not exists " + DB_NAME);
            stmt.execute("use " + DB_NAME);
            stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
            stmt.execute("create table t1 using weather tags('beijing')");
            stmt.close();

            pstmtInsert = conn.prepareStatement(SQL_INSERT);
            pstmtSelect = conn.prepareStatement(SQL_SELECT);
            pstmtWithoutParameters = conn.prepareStatement(SQL_WITHOUT_PARAMETERS);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (pstmtInsert != null)
                pstmtInsert.close();
            if (pstmtSelect != null)
                pstmtSelect.close();
            if (pstmtWithoutParameters != null)
                pstmtWithoutParameters.close();
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