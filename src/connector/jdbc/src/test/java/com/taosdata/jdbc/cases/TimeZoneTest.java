package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class TimeZoneTest {

    private String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

    @Test
    public void test() {
        // given
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try (Connection connection = DriverManager.getConnection(url, props)) {
            Statement stmt = connection.createStatement();
            stmt.execute("insert into timezone_test.weather(ts, temperature) values('1970-01-01 00:00:00', 1.0)");

            ResultSet rs = stmt.executeQuery("select * from timezone_test.weather");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("ts");
                System.out.println(ts);
//                assertEquals();
            }

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Before
    public void before() {
        try {
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists timezone_test");
            stmt.execute("create database if not exists timezone_test");
            stmt.execute("use timezone_test");
            stmt.execute("create table weather(ts timestamp, temperature float)");
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists timezone_test");
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
