package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverAutoloadTest {

    private Properties properties;
    private String host = "127.0.0.1";

    @Test
    public void testRestful() throws SQLException {
//        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        final String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
    }

    @Test
    public void testJni() throws SQLException {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
    }


    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    }

}
