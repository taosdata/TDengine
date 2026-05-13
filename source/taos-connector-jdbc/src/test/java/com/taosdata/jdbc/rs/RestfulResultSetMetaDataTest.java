package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class RestfulResultSetMetaDataTest {


    private static Connection conn;
    private static Statement stmt;
    private static ResultSet rs;
    private static ResultSetMetaData meta;

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(RestfulResultSetMetaDataTest.class);

    @Test
    public void getColumnCount() throws SQLException {
        Assert.assertEquals(10, meta.getColumnCount());
    }

    @Test
    public void isAutoIncrement() throws SQLException {
        Assert.assertFalse(meta.isAutoIncrement(1));
        Assert.assertFalse(meta.isAutoIncrement(2));
        Assert.assertFalse(meta.isAutoIncrement(3));
        Assert.assertFalse(meta.isAutoIncrement(4));
        Assert.assertFalse(meta.isAutoIncrement(5));
        Assert.assertFalse(meta.isAutoIncrement(6));
        Assert.assertFalse(meta.isAutoIncrement(7));
        Assert.assertFalse(meta.isAutoIncrement(8));
        Assert.assertFalse(meta.isAutoIncrement(9));
        Assert.assertFalse(meta.isAutoIncrement(10));
    }

    @Test
    public void isCaseSensitive() throws SQLException {
        Assert.assertFalse(meta.isCaseSensitive(1));
    }

    @Test
    public void isSearchable() throws SQLException {
        Assert.assertTrue(meta.isSearchable(1));
    }

    @Test
    public void isCurrency() throws SQLException {
        Assert.assertFalse(meta.isCurrency(1));
    }

    @Test
    public void isNullable() throws SQLException {
        Assert.assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(2));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(3));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(4));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(5));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(6));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(7));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(8));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(9));
        Assert.assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(10));
    }

    @Test
    public void isSigned() throws SQLException {
        Assert.assertFalse(meta.isSigned(1));
    }

    @Test
    public void getColumnDisplaySize() throws SQLException {
        Assert.assertEquals(64, meta.getColumnDisplaySize(10));
    }

    @Test
    public void getColumnLabel() throws SQLException {
        Assert.assertEquals("f1", meta.getColumnLabel(1));
    }

    @Test
    public void getColumnName() throws SQLException {
        Assert.assertEquals("f1", meta.getColumnName(1));
    }

    @Test
    public void getSchemaName() throws SQLException {
        Assert.assertEquals("", meta.getSchemaName(1));
    }

    @Test
    public void getPrecision() throws SQLException {
        Assert.assertEquals(0, meta.getPrecision(1));
    }

    @Test
    public void getScale() throws SQLException {
        Assert.assertEquals(0, meta.getScale(1));
    }

    @Test
    public void getTableName() throws SQLException {
        Assert.assertEquals("", meta.getTableName(1));
    }

    @Test
    public void getCatalogName() throws SQLException {
        Assert.assertEquals(DB_NAME, meta.getCatalogName(1));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(2));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(3));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(4));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(5));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(6));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(7));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(8));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(9));
        Assert.assertEquals(DB_NAME, meta.getCatalogName(10));
    }

    @Test
    public void getColumnType() throws SQLException {
        Assert.assertEquals(Types.TIMESTAMP, meta.getColumnType(1));
        Assert.assertEquals(Types.INTEGER, meta.getColumnType(2));
        Assert.assertEquals(Types.BIGINT, meta.getColumnType(3));
        Assert.assertEquals(Types.FLOAT, meta.getColumnType(4));
        Assert.assertEquals(Types.DOUBLE, meta.getColumnType(5));
        Assert.assertEquals(Types.VARCHAR, meta.getColumnType(6));
        Assert.assertEquals(Types.SMALLINT, meta.getColumnType(7));
        Assert.assertEquals(Types.TINYINT, meta.getColumnType(8));
        Assert.assertEquals(Types.BOOLEAN, meta.getColumnType(9));
        Assert.assertEquals(Types.NCHAR, meta.getColumnType(10));
    }

    @Test
    public void getColumnTypeName() throws SQLException {
        Assert.assertEquals("TIMESTAMP", meta.getColumnTypeName(1));
        Assert.assertEquals("INT", meta.getColumnTypeName(2));
        Assert.assertEquals("BIGINT", meta.getColumnTypeName(3));
        Assert.assertEquals("FLOAT", meta.getColumnTypeName(4));
        Assert.assertEquals("DOUBLE", meta.getColumnTypeName(5));
        Assert.assertEquals("BINARY", meta.getColumnTypeName(6));
        Assert.assertEquals("SMALLINT", meta.getColumnTypeName(7));
        Assert.assertEquals("TINYINT", meta.getColumnTypeName(8));
        Assert.assertEquals("BOOL", meta.getColumnTypeName(9));
        Assert.assertEquals("NCHAR", meta.getColumnTypeName(10));
    }

    @Test
    public void isReadOnly() throws SQLException {
        Assert.assertTrue(meta.isReadOnly(1));
    }

    @Test
    public void isWritable() throws SQLException {
        Assert.assertFalse(meta.isWritable(1));
    }

    @Test
    public void isDefinitelyWritable() throws SQLException {
        Assert.assertFalse(meta.isDefinitelyWritable(1));
    }

    @Test
    public void getColumnClassName() throws SQLException {
        Assert.assertEquals(Timestamp.class.getName(), meta.getColumnClassName(1));
        Assert.assertEquals(Integer.class.getName(), meta.getColumnClassName(2));
        Assert.assertEquals(Long.class.getName(), meta.getColumnClassName(3));
        Assert.assertEquals(Float.class.getName(), meta.getColumnClassName(4));
        Assert.assertEquals(Double.class.getName(), meta.getColumnClassName(5));
        Assert.assertEquals("[B", meta.getColumnClassName(6));
        Assert.assertEquals(Short.class.getName(), meta.getColumnClassName(7));
        Assert.assertEquals(Byte.class.getName(), meta.getColumnClassName(8));
        Assert.assertEquals(Boolean.class.getName(), meta.getColumnClassName(9));
        Assert.assertEquals(String.class.getName(), meta.getColumnClassName(10));
    }

    @Test
    public void unwrap() throws SQLException {
        Assert.assertNotNull(meta.unwrap(RestfulResultSetMetaData.class));
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(meta.isWrapperFor(RestfulResultSetMetaData.class));
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("drop table if exists weather");
        stmt.execute("create table if not exists weather(f1 timestamp, f2 int, f3 bigint, f4 float, f5 double, f6 binary(64), f7 smallint, f8 tinyint, f9 bool, f10 nchar(64))");
        stmt.execute("insert into weather values('2021-01-01 00:00:00.000', 1, 100, 3.1415, 3.1415926, 'abc', 10, 10, true, '涛思数据')");
        rs = stmt.executeQuery("select * from weather");
        rs.next();
        meta = rs.getMetaData();
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (rs != null)
                rs.close();
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