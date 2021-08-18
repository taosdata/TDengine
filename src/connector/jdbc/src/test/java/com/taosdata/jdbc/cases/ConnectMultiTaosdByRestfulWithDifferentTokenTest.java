package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class ConnectMultiTaosdByRestfulWithDifferentTokenTest {

    private static final String host1 = "192.168.17.156";
    private static final String user1 = "root";
    private static final String password1 = "tqueue";
    private Connection conn1;
    private static final String host2 = "192.168.17.82";
    private static final String user2 = "root";
    private static final String password2 = "taosdata";
    private Connection conn2;

    @Test
    public void test() {
        //when
        executeSelectStatus(conn1);
        executeSelectStatus(conn2);
        executeSelectStatus(conn1);
    }

    private void executeSelectStatus(Connection connection) {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select server_status()");
            ResultSetMetaData meta = rs.getMetaData();
            Assert.assertNotNull(meta);
            while (rs.next()) {
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url1 = "jdbc:TAOS-RS://" + host1 + ":6041/?user=" + user1 + "&password=" + password1;
        String url2 = "jdbc:TAOS-RS://" + host2 + ":6041/?user=" + user2 + "&password=" + password2;
        try {
            conn1 = DriverManager.getConnection(url1, properties);
            conn2 = DriverManager.getConnection(url2, properties);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
