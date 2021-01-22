package com.taosdata.example;

import java.sql.*;
import java.util.Properties;

public class JdbcRestfulDemo {
    private static final String host = "master";

    public static void main(String[] args) {
        try {
            // load JDBC-restful driver
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            // use port 6041 in url when use JDBC-restful
            String url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";

            Properties properties = new Properties();
//            properties.setProperty("charset", "UTF-8");
//            properties.setProperty("locale", "en_US.UTF-8");
//            properties.setProperty("timezone", "UTC-8");

            Connection conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists restful_test");
            stmt.execute("create database if not exists restful_test");
            stmt.execute("use restful_test");
            stmt.execute("create table restful_test.weather(ts timestamp, temperature float) tags(location nchar(64))");
            stmt.executeUpdate("insert into t1 using restful_test.weather tags('北京') values(now, 18.2)");
            ResultSet rs = stmt.executeQuery("select * from restful_test.weather");
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
            }

            rs.close();
            stmt.close();
            conn.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
