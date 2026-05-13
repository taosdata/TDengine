package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

@Ignore
public class ConnectMultiTaosdByRestfulWithDifferentTokenTest {

    private static final String HOST_1 = "192.168.17.156";
    private static final String USER_1 = TestEnvUtil.getUser();
    private static final String PASSWORD_1 = "tqueue";
    private Connection conn1;
    private static final String HOST_2 = "192.168.17.82";
    private static final String USER_2 = TestEnvUtil.getUser();
    private static final String PASSWORD_2 = "taosdata";
    private Connection conn2;

    @Test
    public void test() throws SQLException {
        //when
        executeSelectStatus(conn1);
        executeSelectStatus(conn2);
        executeSelectStatus(conn1);
    }

    private void executeSelectStatus(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("select 1");
            ResultSetMetaData meta = rs.getMetaData();
            Assert.assertNotNull(meta);
            while (rs.next()) {
            }
        }
    }

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        String url1 = "jdbc:TAOS-RS://" + HOST_1 + ":6041/?user=" + USER_1 + "&password=" + PASSWORD_1;
        String url2 = "jdbc:TAOS-RS://" + HOST_2 + ":6041/?user=" + USER_2 + "&password=" + PASSWORD_2;
        try {
            conn1 = DriverManager.getConnection(url1, properties);
            conn2 = DriverManager.getConnection(url2, properties);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
