package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;

public class TSDBConstantsTest {

    @Test
    public void testJdbcType2TaosTypeNameBoolean() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.BOOLEAN);
        Assert.assertEquals("BOOL", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameTinyInt() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.TINYINT);
        Assert.assertEquals("TINYINT", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameSmallInt() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.SMALLINT);
        Assert.assertEquals("SMALLINT", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameInteger() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.INTEGER);
        Assert.assertEquals("INT", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameBigInt() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.BIGINT);
        Assert.assertEquals("BIGINT", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameFloat() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.FLOAT);
        Assert.assertEquals("FLOAT", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameDouble() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.DOUBLE);
        Assert.assertEquals("DOUBLE", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameBinary() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.BINARY);
        Assert.assertEquals("BINARY", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameTimestamp() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.TIMESTAMP);
        Assert.assertEquals("TIMESTAMP", typeName);
    }

    @Test
    public void testJdbcType2TaosTypeNameNChar() throws SQLException {
        String typeName = TSDBConstants.jdbcType2TaosTypeName(Types.NCHAR);
        Assert.assertEquals("NCHAR", typeName);
    }

    @Test(expected = SQLException.class)
    public void testJdbcType2TaosTypeNameUnknown() throws SQLException {
        TSDBConstants.jdbcType2TaosTypeName(Types.DATE);
    }

    @Test(expected = SQLException.class)
    public void testJdbcType2TaosTypeNameVarchar() throws SQLException {
        TSDBConstants.jdbcType2TaosTypeName(Types.VARCHAR);
    }

    @Test(expected = SQLException.class)
    public void testJdbcType2TaosTypeNameDecimal() throws SQLException {
        TSDBConstants.jdbcType2TaosTypeName(Types.DECIMAL);
    }

    @Test(expected = SQLException.class)
    public void testJdbcType2TaosTypeNameNumeric() throws SQLException {
        TSDBConstants.jdbcType2TaosTypeName(Types.NUMERIC);
    }

    @Test(expected = SQLException.class)
    public void testJdbcType2TaosTypeNameReal() throws SQLException {
        TSDBConstants.jdbcType2TaosTypeName(Types.REAL);
    }
}
