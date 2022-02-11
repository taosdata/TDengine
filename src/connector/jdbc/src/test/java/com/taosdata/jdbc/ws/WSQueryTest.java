package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@Ignore
@RunWith(CatalogRunner.class)
@TestTarget(alias = "query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSQueryTest {
    private static final String host = "192.168.1.98";
    private static final int port = 6041;
    private static final String databaseName = "ws_query";
    private static final String tableName = "wq";
    private Connection connection;
    private long now;

    @Description("query")
    @Test
    public void queryBlock() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("insert into " + databaseName + "." + tableName + " values(now+100s, 100)");
        ResultSet resultSet = statement.executeQuery("select * from " + databaseName + "." + tableName);
        resultSet.next();
        Assert.assertEquals(100, resultSet.getInt(2));
        statement.close();
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS-RS://" + host + ":" + port + "/test?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + databaseName);
        statement.execute("create database " + databaseName);
        statement.execute("use " + databaseName);
        statement.execute("create table if not exists " + databaseName + "." + tableName + "(ts timestamp, f int)");
        statement.close();
    }
}
