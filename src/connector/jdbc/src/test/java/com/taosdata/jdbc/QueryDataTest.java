package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.*;

import static org.junit.Assert.assertTrue;

public class QueryDataTest extends BaseTest {

    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String stbName = "meters";
    static String host = "localhost";
    static int numOfTables = 30;
    final static int numOfRecordsPerTable = 1000;
    static long ts = 1496732686000l;
    final static String tablePrefix = "t";

    @Before
    public void createDatabase() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
                
        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);

        String createTableSql = "create table " + stbName + "(ts timestamp, name binary(6))";  
        statement.executeUpdate(createTableSql); 
    }
    
    @Test
    public void testQueryBinaryData() throws SQLException{                
                
        String insertSql = "insert into "  + stbName + " values(now, 'taosda')";
        System.out.println(insertSql);        

        statement.executeUpdate(insertSql);

        String querySql = "select * from " + stbName;
        ResultSet rs = statement.executeQuery(querySql); 

        while(rs.next()) {
            String name = rs.getString(2) + "001";
            System.out.println("name = " + name);
            assertEquals(name, "taosda001");
        }
        rs.close();        
    }


    @After
    public void close() throws Exception {        
        statement.close();
        connection.close();
        Thread.sleep(10);
    }
    
}