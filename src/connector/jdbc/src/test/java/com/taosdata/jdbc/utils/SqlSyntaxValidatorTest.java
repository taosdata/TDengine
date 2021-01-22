package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

public class SqlSyntaxValidatorTest {

    @Test
    public void isSelectSQL() {
        Assert.assertTrue(SqlSyntaxValidator.isSelectSql("select * from test.weather"));
        Assert.assertTrue(SqlSyntaxValidator.isSelectSql(" select * from test.weather"));
        Assert.assertTrue(SqlSyntaxValidator.isSelectSql(" select * from test.weather "));
        Assert.assertFalse(SqlSyntaxValidator.isSelectSql("insert into test.weather values(now, 1.1, 2)"));
    }

    @Test
    public void isUseSQL() {
        Assert.assertTrue(SqlSyntaxValidator.isUseSql("use database test"));
    }

}