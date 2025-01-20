package com.taosdata.example;

import java.sql.*;
import java.util.Properties;

public class JdbcRestfulDemo {
    private static final String host = "localhost";
    private static final String dbname = "test";
    private static final String user = "root";
    private static final String password = "taosdata";

    public static void main(String[] args) {
        try {
            // use port 6041 in url when use JDBC-restful
            String url = "jdbc:TAOS-RS://" + host + ":6041/?user=" + user + "&password=" + password;

            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");

            Connection conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table " + dbname + ".weather(ts timestamp, temperature float) tags(location nchar(64))");
            stmt.executeUpdate("insert into t1 using " + dbname + ".weather tags('北京') values(now, 18.2)");
            ResultSet rs = stmt.executeQuery("select * from " + dbname + ".weather");
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
