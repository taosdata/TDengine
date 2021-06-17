package com.taosdata.jdbc.cases;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class ResultSetMetaShouldNotBeNullRestfulTest {

    private static final String host = "127.0.0.1";
    private static final String dbname = "td4745";

    private Connection connection;

    @Test
    public void testExecuteQuery() {
        // given
        ResultSetMetaData metaData = null;
        int columnCount = -1;

        // when
        try {
            Statement statement = connection.createStatement();
            metaData = statement.executeQuery("select * from weather").getMetaData();
            columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertNotNull(metaData);
        Assert.assertEquals(0, columnCount);
    }

    @Test
    public void testExecute() {
        // given
        ResultSetMetaData metaData = null;
        int columnCount = -1;
        boolean execute = false;
        // when
        try {
            Statement statement = connection.createStatement();
            execute = statement.execute("select * from weather");
            metaData = statement.getResultSet().getMetaData();
            columnCount = metaData.getColumnCount();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals(true, execute);
        Assert.assertNotNull(metaData);
        Assert.assertEquals(0, columnCount);
    }

    @Before
    public void before() {
        try {
            connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather (ts timestamp, temperature float)");
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
