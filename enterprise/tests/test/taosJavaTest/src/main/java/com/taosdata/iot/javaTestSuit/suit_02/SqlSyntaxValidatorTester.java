package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.jdbc.utils.SqlSyntaxValidator;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @author Jiangyi Hou
 * @since 19-1-23
 */
public class SqlSyntaxValidatorTester {

    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String TSDB_URL = "jdbc:TAOS://192.168.0.1:0/?user=root&password=taosdata";

    public static void main(String[] args) {

        SqlSyntaxValidatorTester tester = new SqlSyntaxValidatorTester();
        tester.test();
    }

    private void test() {

        boolean res = false;
        boolean expected = false;
        try {
            Class.forName(TSDB_DRIVER);
            Connection connection = DriverManager.getConnection(TSDB_URL);
            Statement stmt = connection.createStatement();
            SqlSyntaxValidator createTableValidater = new SqlSyntaxValidator(connection);

            Assert.assertTrue(createTableValidater.validateSqlSyntax("create table tb (ts timestamp, c1 int, " +
                    "c2 bigint, c3 double, c4 float, c5 bool, c6 smallint, c7 tinyint, c8 binary(10), c9 nchar(10), " +
                    "c10 timestamp)"));
            Assert.assertFalse(createTableValidater.validateSqlSyntax("create table tb using stb tags(1)"));
            stmt.executeUpdate("drop database if exists vctst_db");
            Thread.currentThread().sleep(2000);
            stmt.executeUpdate("create database vctst_db");
            Assert.assertFalse(createTableValidater.validateSqlSyntax("create table vctst_db.tb using vctst_db.stb tags(1)"));
            stmt.executeUpdate("use vctst_db");
            stmt.executeUpdate("create table stb (ts timestamp , c1 int) tags (t1 int, t2 binary(10), t3 nchar(10))");
            Assert.assertFalse(createTableValidater.validateSqlSyntax("create table vctst_db.tb using vctst_db.stb tags(1)"));
            Assert.assertFalse(createTableValidater.validateSqlSyntax("create table vctst_db.tb using vctst_db.stb tags('a', 'b0123456789', 'nchar10')"));
            Assert.assertTrue(createTableValidater.validateSqlSyntax("create table vctst_db.tb using vctst_db.stb tags(1, 'binary10', '涛思nchar10')"));
            System.out.println("All tests passed!");
            stmt.close();
            connection.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
