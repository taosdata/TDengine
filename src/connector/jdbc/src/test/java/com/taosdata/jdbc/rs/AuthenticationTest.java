package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class AuthenticationTest {

    private static final String host = "master";
    private static final String user = "root";
    private static final String password = "123456";
    private Connection conn;

    @Test
    public void test() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("show databases");
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ":" + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            Properties props = new Properties();
            props.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
            props.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test", props);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
