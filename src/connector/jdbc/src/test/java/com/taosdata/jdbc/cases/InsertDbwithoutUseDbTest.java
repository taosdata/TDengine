package com.taosdata.jdbc.cases;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;
import java.util.Properties;
import java.util.Random;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InsertDbwithoutUseDbTest {

    private static String host = "127.0.0.1";
    //    private static String host = "master";
    private static Properties properties;
    private static Random random = new Random(System.currentTimeMillis());

    @Test
    public void case001() throws ClassNotFoundException, SQLException {
        // prepare schema
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists inWithoutDb");
            stmt.execute("create database if not exists inWithoutDb");
            stmt.execute("create table inWithoutDb.weather(ts timestamp, f1 int)");
        }
        conn.close();

        // execute insert
        url = "jdbc:TAOS://127.0.0.1:6030/inWithoutDb?user=root&password=taosdata";
        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            int affectedRow = stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
            Assert.assertEquals(1, affectedRow);
            boolean flag = stmt.execute("insert into weather(ts, f1) values(now + 10s," + random.nextInt(100) + ")");
            Assert.assertEquals(false, flag);
            ResultSet rs = stmt.executeQuery("select count(*) from weather");
            rs.next();
            int count = rs.getInt("count(*)");
            Assert.assertEquals(2, count);

        } catch (SQLException e) {
            e.printStackTrace();
        }

        conn.close();
    }

    @Test
    public void case002() throws ClassNotFoundException, SQLException {
        // prepare the schema
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        final String url = "jdbc:TAOS-RS://" + host + ":6041/inWithoutDb?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists inWithoutDb");
            stmt.execute("create database if not exists inWithoutDb");
            stmt.execute("create table inWithoutDb.weather(ts timestamp, f1 int)");
        }
        conn.close();

        // execute
        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            int affectedRow = stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
            Assert.assertEquals(1, affectedRow);
            boolean flag = stmt.execute("insert into weather(ts, f1) values(now + 10s," + random.nextInt(100) + ")");
            Assert.assertEquals(false, flag);
            ResultSet rs = stmt.executeQuery("select count(*) from weather");
            rs.next();
            int count = rs.getInt("count(*)");
            Assert.assertEquals(2, count);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {
        properties = new Properties();
        properties.setProperty("charset", "UTF-8");
        properties.setProperty("locale", "en_US.UTF-8");
        properties.setProperty("timezone", "UTC-8");
    }

}
