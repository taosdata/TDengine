package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class ResultSetMetaShouldNotBeNullRestfulTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String DB_NAME = TestUtils.camelToSnake(ResultSetMetaShouldNotBeNullRestfulTest.class);

    private Connection connection;

    @Test
    public void testExecuteQuery() throws SQLException {
        // given
        ResultSetMetaData metaData;
        int columnCount;

        // when
        Statement statement = connection.createStatement();
        metaData = statement.executeQuery("select * from weather").getMetaData();
        columnCount = metaData.getColumnCount();

        // then
        Assert.assertNotNull(metaData);
        Assert.assertEquals(2, columnCount);
    }

    @Test
    public void testExecute() throws SQLException {
        // given
        ResultSetMetaData metaData;
        int columnCount;
        boolean execute;

        // when
        Statement statement = connection.createStatement();
        execute = statement.execute("select * from weather");
        metaData = statement.getResultSet().getMetaData();
        columnCount = metaData.getColumnCount();

        // then
        Assert.assertEquals(true, execute);
        Assert.assertNotNull(metaData);
        Assert.assertEquals(2, columnCount);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        connection = DriverManager.getConnection(url);
        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("create table weather (ts timestamp, temperature float)");
        stmt.close();
    }

    @After
    public void after() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.close();
        connection.close();
    }

}

