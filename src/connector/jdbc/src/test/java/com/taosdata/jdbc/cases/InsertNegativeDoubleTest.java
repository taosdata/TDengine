package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class InsertNegativeDoubleTest {

    static Connection connection;
    static Statement statement;
    static String dbName = "test";
    static String stbName = "meters";
    static String host = "127.0.0.1";

    @Before
    public void createDatabase() {
        try {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, "root");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, "taosdata");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

            statement = connection.createStatement();
            statement.executeUpdate("drop database if exists " + dbName);
            statement.executeUpdate("create database if not exists " + dbName);
            statement.executeUpdate("use " + dbName);

            String createTableSql = "create table " + stbName + "(ts timestamp, double_col double)";
            statement.executeUpdate(createTableSql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryBinaryData() throws SQLException {
        String insertSql = "insert into " + stbName + " values(now, -123456789.123456789)";
        statement.executeUpdate(insertSql);

        String querySql = "select * from " + stbName;
        ResultSet rs = statement.executeQuery(querySql);

        while (rs.next()) {
            double value = rs.getDouble(2);
            assert(java.lang.Double.compare(-123456789.123456789, value) == 0);
        }
        rs.close();
    }

    @After
    public void close() {
        try {
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}