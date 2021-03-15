package com.taosdata.jdbc.cases;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;

public class InsertDbwithoutUseDbTest {

    //    private static String host = "127.0.0.1";
    private static String host = "master";
    private static Connection jniConn;
    private static Connection restConn;
    private static Random random = new Random(System.currentTimeMillis());

    @Test
    public void case001() {
        Properties properties = new Properties();
        properties.setProperty("charset", "UTF-8");
        properties.setProperty("locale", "en_US.UTF-8");
        properties.setProperty("timezone", "UTC-8");

        try {
            // prepare schema
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            final String url = "jdbc:TAOS://" + host + ":6030/inWithoutDb?user=root&password=taosdata";
            Connection jniConn = DriverManager.getConnection(url, properties);
            try (Statement stmt = jniConn.createStatement()) {
                stmt.execute("drop database if exists inWithoutDb");
                stmt.execute("create database if not exists inWithoutDb");
                stmt.execute("create table inWithoutDb.weather(ts timestamp, f1 int)");
            }
            jniConn.close();

            // execute insert
            jniConn = DriverManager.getConnection(url, properties);
            try (Statement stmt = jniConn.createStatement()) {
                int affectedRow = stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
                Assert.assertEquals(1, affectedRow);
                stmt.executeQuery("select count(*) from weather");

            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }


    }

    @Test
    public void case002() {
        try (Statement stmt = restConn.createStatement()) {
            stmt.executeUpdate("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
            stmt.execute("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
//            stmt.executeQuery("insert into weather(ts, f1) values(now," + random.nextInt(100) + ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @BeforeClass
    public static void beforeClass() {


        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            final String url = "jdbc:TAOS-RS://" + host + ":6041/inWithoutDb?user=root&password=taosdata";
            restConn = DriverManager.getConnection(url, properties);
//            try (Statement stmt = restConn.createStatement()) {
//                stmt.execute("drop database if exists inWithoutDb");
//                stmt.execute("create database if not exists inWithoutDb");
//                stmt.execute("create table inWithoutDb.weather(ts timestamp, f1 int)");
//            }
//            restConn.close();
//            restConn = DriverManager.getConnection(url, properties);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (jniConn != null)
                jniConn.close();
            if (restConn != null)
                restConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
