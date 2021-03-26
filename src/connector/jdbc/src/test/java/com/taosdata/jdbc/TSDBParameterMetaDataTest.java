package com.taosdata.jdbc;

import com.taosdata.jdbc.rs.RestfulParameterMetaData;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class TSDBParameterMetaDataTest {

    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static final String sql_insert = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static PreparedStatement pstmt_insert;
    private static final String sql_select = "select * from t1 where ts > ? and ts <= ? and f1 >= ?";
    private static PreparedStatement pstmt_select;
    private static ParameterMetaData parameterMetaData_insert;
    private static ParameterMetaData parameterMetaData_select;

    @Test
    public void getParameterCount() throws SQLException {
        Assert.assertEquals(10, parameterMetaData_insert.getParameterCount());
    }

    @Test
    public void isNullable() throws SQLException {
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(1));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(2));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(3));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(4));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(5));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(6));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(7));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(8));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(9));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaData_insert.isNullable(10));
    }

    @Test
    public void isSigned() throws SQLException {
        Assert.assertEquals(false, parameterMetaData_insert.isSigned(1));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(2));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(3));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(4));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(5));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(6));
        Assert.assertEquals(true, parameterMetaData_insert.isSigned(7));
        Assert.assertEquals(false, parameterMetaData_insert.isSigned(8));
        Assert.assertEquals(false, parameterMetaData_insert.isSigned(9));
        Assert.assertEquals(false, parameterMetaData_insert.isSigned(10));
    }

    @Test
    public void getPrecision() throws SQLException {
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(1));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(2));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(3));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(4));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(5));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(6));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(7));
        Assert.assertEquals(0, parameterMetaData_insert.getPrecision(8));
        Assert.assertEquals(5, parameterMetaData_insert.getPrecision(9));
        Assert.assertEquals(5, parameterMetaData_insert.getPrecision(10));
    }

    @Test
    public void getScale() throws SQLException {
        Assert.assertEquals(0, parameterMetaData_insert.getScale(1));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(2));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(3));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(4));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(5));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(6));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(7));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(8));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(9));
        Assert.assertEquals(0, parameterMetaData_insert.getScale(10));
    }

    @Test
    public void getParameterType() throws SQLException {
        Assert.assertEquals(Types.TIMESTAMP, parameterMetaData_insert.getParameterType(1));
        Assert.assertEquals(Types.INTEGER, parameterMetaData_insert.getParameterType(2));
        Assert.assertEquals(Types.BIGINT, parameterMetaData_insert.getParameterType(3));
        Assert.assertEquals(Types.FLOAT, parameterMetaData_insert.getParameterType(4));
        Assert.assertEquals(Types.DOUBLE, parameterMetaData_insert.getParameterType(5));
        Assert.assertEquals(Types.SMALLINT, parameterMetaData_insert.getParameterType(6));
        Assert.assertEquals(Types.TINYINT, parameterMetaData_insert.getParameterType(7));
        Assert.assertEquals(Types.BOOLEAN, parameterMetaData_insert.getParameterType(8));
        Assert.assertEquals(Types.BINARY, parameterMetaData_insert.getParameterType(9));
        Assert.assertEquals(Types.NCHAR, parameterMetaData_insert.getParameterType(10));
    }

    @Test
    public void getParameterTypeName() throws SQLException {
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP), parameterMetaData_insert.getParameterTypeName(1));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.INTEGER), parameterMetaData_insert.getParameterTypeName(2));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BIGINT), parameterMetaData_insert.getParameterTypeName(3));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.FLOAT), parameterMetaData_insert.getParameterTypeName(4));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.DOUBLE), parameterMetaData_insert.getParameterTypeName(5));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.SMALLINT), parameterMetaData_insert.getParameterTypeName(6));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.TINYINT), parameterMetaData_insert.getParameterTypeName(7));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BOOLEAN), parameterMetaData_insert.getParameterTypeName(8));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BINARY), parameterMetaData_insert.getParameterTypeName(9));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.NCHAR), parameterMetaData_insert.getParameterTypeName(10));
    }

    @Test
    public void getParameterClassName() throws SQLException {
        Assert.assertEquals(Timestamp.class.getName(), parameterMetaData_insert.getParameterClassName(1));
        Assert.assertEquals(Integer.class.getName(), parameterMetaData_insert.getParameterClassName(2));
        Assert.assertEquals(Long.class.getName(), parameterMetaData_insert.getParameterClassName(3));
        Assert.assertEquals(Float.class.getName(), parameterMetaData_insert.getParameterClassName(4));
        Assert.assertEquals(Double.class.getName(), parameterMetaData_insert.getParameterClassName(5));
        Assert.assertEquals(Short.class.getName(), parameterMetaData_insert.getParameterClassName(6));
        Assert.assertEquals(Byte.class.getName(), parameterMetaData_insert.getParameterClassName(7));
        Assert.assertEquals(Boolean.class.getName(), parameterMetaData_insert.getParameterClassName(8));
        Assert.assertEquals(byte[].class.getName(), parameterMetaData_insert.getParameterClassName(9));
        Assert.assertEquals(String.class.getName(), parameterMetaData_insert.getParameterClassName(10));
    }

    @Test
    public void getParameterMode() throws SQLException {
        for (int i = 1; i <= parameterMetaData_insert.getParameterCount(); i++) {
            int parameterMode = parameterMetaData_insert.getParameterMode(i);
            Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMode);
        }
    }

    @Test
    public void unwrap() throws SQLException {
        RestfulParameterMetaData unwrap = parameterMetaData_insert.unwrap(RestfulParameterMetaData.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(parameterMetaData_insert.isWrapperFor(RestfulParameterMetaData.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists test_pstmt");
                stmt.execute("create database if not exists test_pstmt");
                stmt.execute("use test_pstmt");
                stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
                stmt.execute("create table t1 using weather tags('beijing')");
            }
            pstmt_insert = conn.prepareStatement(sql_insert);

            pstmt_insert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt_insert.setObject(2, 111);
            pstmt_insert.setObject(3, Long.MAX_VALUE);
            pstmt_insert.setObject(4, 3.14159265354f);
            pstmt_insert.setObject(5, Double.MAX_VALUE);
            pstmt_insert.setObject(6, Short.MAX_VALUE);
            pstmt_insert.setObject(7, Byte.MAX_VALUE);
            pstmt_insert.setObject(8, true);
            pstmt_insert.setObject(9, "hello".getBytes());
            pstmt_insert.setObject(10, "Hello");
            parameterMetaData_insert = pstmt_insert.getParameterMetaData();

            pstmt_select = conn.prepareStatement(sql_select);
            pstmt_select.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmt_select.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 10000));
            pstmt_select.setInt(3, 0);
            parameterMetaData_select = pstmt_select.getParameterMetaData();

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (pstmt_insert != null)
                pstmt_insert.close();
            if (pstmt_select != null)
                pstmt_select.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}