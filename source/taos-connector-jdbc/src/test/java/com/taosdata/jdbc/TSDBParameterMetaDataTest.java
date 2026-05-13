package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class TSDBParameterMetaDataTest {

    private static Connection conn;

    static final String HOST = TestEnvUtil.getHost();
    private static final String SQL_INSERT = "insert into t1 values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static PreparedStatement pstmtInsert;
    private static final String SQL_SELECT = "select * from t1 where ts > ? and ts <= ? and f1 >= ?";
    private static PreparedStatement pstmtSelect;
    private static ParameterMetaData parameterMetaDataInsert;
    private static ParameterMetaData parameterMetaDataSelect;
    private static final String DB_NAME = TestUtils.camelToSnake(TSDBParameterMetaDataTest.class);

    @Test
    public void getParameterCount() throws SQLException {
        Assert.assertEquals(10, parameterMetaDataInsert.getParameterCount());
    }

    @Test
    public void isNullable() throws SQLException {
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(1));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(2));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(3));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(4));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(5));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(6));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(7));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(8));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(9));
        Assert.assertEquals(ParameterMetaData.parameterNullableUnknown, parameterMetaDataInsert.isNullable(10));
    }

    @Test
    public void isSigned() throws SQLException {
        Assert.assertEquals(false, parameterMetaDataInsert.isSigned(1));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(2));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(3));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(4));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(5));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(6));
        Assert.assertEquals(true, parameterMetaDataInsert.isSigned(7));
        Assert.assertEquals(false, parameterMetaDataInsert.isSigned(8));
        Assert.assertEquals(false, parameterMetaDataInsert.isSigned(9));
        Assert.assertEquals(false, parameterMetaDataInsert.isSigned(10));
    }

    @Test
    public void getPrecision() throws SQLException {
        //create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64))
        Assert.assertEquals(TSDBConstants.TIMESTAMP_MS_PRECISION, parameterMetaDataInsert.getPrecision(1));
        Assert.assertEquals(TSDBConstants.INT_PRECISION, parameterMetaDataInsert.getPrecision(2));
        Assert.assertEquals(TSDBConstants.BIGINT_PRECISION, parameterMetaDataInsert.getPrecision(3));
        Assert.assertEquals(TSDBConstants.FLOAT_PRECISION, parameterMetaDataInsert.getPrecision(4));
        Assert.assertEquals(TSDBConstants.DOUBLE_PRECISION, parameterMetaDataInsert.getPrecision(5));
        Assert.assertEquals(TSDBConstants.SMALLINT_PRECISION, parameterMetaDataInsert.getPrecision(6));
        Assert.assertEquals(TSDBConstants.TINYINT_PRECISION, parameterMetaDataInsert.getPrecision(7));
        Assert.assertEquals(TSDBConstants.BOOLEAN_PRECISION, parameterMetaDataInsert.getPrecision(8));
        Assert.assertEquals("hello".getBytes().length, parameterMetaDataInsert.getPrecision(9));
        Assert.assertEquals("涛思数据".length(), parameterMetaDataInsert.getPrecision(10));
    }

    @Test
    public void getScale() throws SQLException {
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(1));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(2));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(3));
        Assert.assertEquals(31, parameterMetaDataInsert.getScale(4));
        Assert.assertEquals(31, parameterMetaDataInsert.getScale(5));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(6));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(7));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(8));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(9));
        Assert.assertEquals(0, parameterMetaDataInsert.getScale(10));
    }

    @Test
    public void getParameterType() throws SQLException {
        Assert.assertEquals(Types.TIMESTAMP, parameterMetaDataInsert.getParameterType(1));
        Assert.assertEquals(Types.INTEGER, parameterMetaDataInsert.getParameterType(2));
        Assert.assertEquals(Types.BIGINT, parameterMetaDataInsert.getParameterType(3));
        Assert.assertEquals(Types.FLOAT, parameterMetaDataInsert.getParameterType(4));
        Assert.assertEquals(Types.DOUBLE, parameterMetaDataInsert.getParameterType(5));
        Assert.assertEquals(Types.SMALLINT, parameterMetaDataInsert.getParameterType(6));
        Assert.assertEquals(Types.TINYINT, parameterMetaDataInsert.getParameterType(7));
        Assert.assertEquals(Types.BOOLEAN, parameterMetaDataInsert.getParameterType(8));
        Assert.assertEquals(Types.BINARY, parameterMetaDataInsert.getParameterType(9));
        Assert.assertEquals(Types.NCHAR, parameterMetaDataInsert.getParameterType(10));
    }

    @Test
    public void getParameterTypeName() throws SQLException {
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP), parameterMetaDataInsert.getParameterTypeName(1));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.INTEGER), parameterMetaDataInsert.getParameterTypeName(2));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BIGINT), parameterMetaDataInsert.getParameterTypeName(3));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.FLOAT), parameterMetaDataInsert.getParameterTypeName(4));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.DOUBLE), parameterMetaDataInsert.getParameterTypeName(5));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.SMALLINT), parameterMetaDataInsert.getParameterTypeName(6));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.TINYINT), parameterMetaDataInsert.getParameterTypeName(7));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BOOLEAN), parameterMetaDataInsert.getParameterTypeName(8));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.BINARY), parameterMetaDataInsert.getParameterTypeName(9));
        Assert.assertEquals(TSDBConstants.jdbcType2TaosTypeName(Types.NCHAR), parameterMetaDataInsert.getParameterTypeName(10));
    }

    @Test
    public void getParameterClassName() throws SQLException {
        Assert.assertEquals(Timestamp.class.getName(), parameterMetaDataInsert.getParameterClassName(1));
        Assert.assertEquals(Integer.class.getName(), parameterMetaDataInsert.getParameterClassName(2));
        Assert.assertEquals(Long.class.getName(), parameterMetaDataInsert.getParameterClassName(3));
        Assert.assertEquals(Float.class.getName(), parameterMetaDataInsert.getParameterClassName(4));
        Assert.assertEquals(Double.class.getName(), parameterMetaDataInsert.getParameterClassName(5));
        Assert.assertEquals(Short.class.getName(), parameterMetaDataInsert.getParameterClassName(6));
        Assert.assertEquals(Byte.class.getName(), parameterMetaDataInsert.getParameterClassName(7));
        Assert.assertEquals(Boolean.class.getName(), parameterMetaDataInsert.getParameterClassName(8));
        Assert.assertEquals(byte[].class.getName(), parameterMetaDataInsert.getParameterClassName(9));
        Assert.assertEquals(String.class.getName(), parameterMetaDataInsert.getParameterClassName(10));
    }

    @Test
    public void getParameterMode() throws SQLException {
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(1));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(2));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(3));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(4));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(5));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(6));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(7));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(8));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(9));
        Assert.assertEquals(ParameterMetaData.parameterModeUnknown, parameterMetaDataInsert.getParameterMode(10));
    }

    @Test
    public void unwrap() throws SQLException {
        TSDBParameterMetaData unwrap = parameterMetaDataInsert.unwrap(TSDBParameterMetaData.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(parameterMetaDataInsert.isWrapperFor(TSDBParameterMetaData.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getJniUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            conn = DriverManager.getConnection(url);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("drop database if exists " + DB_NAME);
                stmt.execute("create database if not exists " + DB_NAME);
                stmt.execute("use " + DB_NAME);
                stmt.execute("create table weather(ts timestamp, f1 int, f2 bigint, f3 float, f4 double, f5 smallint, f6 tinyint, f7 bool, f8 binary(64), f9 nchar(64)) tags(loc nchar(64))");
                stmt.execute("create table t1 using weather tags('beijing')");
            }
            pstmtInsert = conn.prepareStatement(SQL_INSERT);

            pstmtInsert.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmtInsert.setObject(2, 111);
            pstmtInsert.setObject(3, Long.MAX_VALUE);
            pstmtInsert.setObject(4, 3.14159265354f);
            pstmtInsert.setObject(5, Double.MAX_VALUE);
            pstmtInsert.setObject(6, Short.MAX_VALUE);
            pstmtInsert.setObject(7, Byte.MAX_VALUE);
            pstmtInsert.setObject(8, true);
            pstmtInsert.setObject(9, "hello".getBytes());
            pstmtInsert.setObject(10, "涛思数据");
            parameterMetaDataInsert = pstmtInsert.getParameterMetaData();

            pstmtSelect = conn.prepareStatement(SQL_SELECT);
            pstmtSelect.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            pstmtSelect.setTimestamp(2, new Timestamp(System.currentTimeMillis() + 10000));
            pstmtSelect.setInt(3, 0);
            parameterMetaDataSelect = pstmtSelect.getParameterMetaData();
            Assert.assertNotNull(parameterMetaDataSelect);

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