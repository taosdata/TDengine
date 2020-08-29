package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TSDBDriverTest {

    @Test
    public void urlParserTest() throws SQLException {
        TSDBDriver driver = new TSDBDriver();
        String url = "jdbc:TSDB://127.0.0.1:0/db";

        Properties properties = new Properties(); 
        driver.parseURL(url, properties);
        assertEquals(properties.get("host"), "127.0.0.1");
        assertEquals(properties.get("port"), "0");
        assertEquals(properties.get("dbname"), "db");                
        assertEquals(properties.get("user"), "root");   
        assertEquals(properties.get("password"), "your_password");

        url = "jdbc:TSDB://127.0.0.1:0/log?charset=UTF-8";
        properties = new Properties(); 
        driver.parseURL(url, properties);
        assertEquals(properties.get("host"), "127.0.0.1");
        assertEquals(properties.get("port"), "0");
        assertEquals(properties.get("dbname"), "log");        
        assertEquals(properties.get("charset"), "UTF-8"); 
        
        url = "jdbc:TSDB://127.0.0.1:0/";
        properties = new Properties(); 
        driver.parseURL(url, properties);
        assertEquals(properties.get("host"), "127.0.0.1");
        assertEquals(properties.get("port"), "0"); 
        assertEquals(properties.get("dbname"), null);
        
        url = "jdbc:TSDB://127.0.0.1:0/db";
        properties = new Properties(); 
        driver.parseURL(url, properties);
        assertEquals(properties.get("host"), "127.0.0.1");
        assertEquals(properties.get("port"), "0"); 
        assertEquals(properties.get("dbname"), "db");
    }
}