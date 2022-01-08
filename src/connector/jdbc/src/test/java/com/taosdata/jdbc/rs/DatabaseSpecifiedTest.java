package com.taosdata.jdbc.rs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class DatabaseSpecifiedTest {

    private static String host = "127.0.0.1";
    private static String dbname = "test_db_spec";

    private Connection connection;
    private long ts;

    @Test
    public void test() throws SQLException {
        // when
        connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/" + dbname + "?user=root&password=taosdata");
        try (Statement stmt = connection.createStatement();) {
            ResultSet rs = stmt.executeQuery("select * from weather");

            //then
            assertNotNull(rs);
            rs.next();
            long now = rs.getTimestamp("ts").getTime();
            assertEquals(ts, now);
            int f1 = rs.getInt(2);
            assertEquals(1, f1);
            String loc = rs.getString("loc");
            assertEquals("beijing", loc);
        }
    }

    @Before
    public void before() {
        ts = System.currentTimeMillis();
        try {
            Connection connection = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata");
            Statement stmt = connection.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("insert into t1 using weather tags('beijing') values( " + ts + ", 1)");

            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.execute("drop database if exists " + dbname);
                statement.close();
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
