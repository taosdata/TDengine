package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ResetQueryCacheTest {

    static Connection connection;
    static Statement statement;
    static String host = "127.0.0.1";

    @Before
    public void init() {
        try {
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
            statement = connection.createStatement();
        } catch (SQLException e) {
            return;
        }
    }

    @Test
    public void testResetQueryCache() throws SQLException {
        String resetSql = "reset query cache";
        ResultSet rs = statement.executeQuery(resetSql);
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