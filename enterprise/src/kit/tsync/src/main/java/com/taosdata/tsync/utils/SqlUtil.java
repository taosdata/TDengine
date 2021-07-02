package com.taosdata.tsync.utils;

import java.sql.*;

public class SqlUtil {

    public static boolean execute(String host, String dbname, String user, String password, String sql) {
        boolean result = false;
        String url = "jdbc:TAOS-RS://" + host + ":6041/" + dbname + "?user=" + user + "&password=" + password;
        try {
            Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
            result = stmt.execute(sql);
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static int executeUpdate(String host, String dbname, String user, String password, String sql) {
        int result = 0;
        String url = "jdbc:TAOS-RS://" + host + ":6041/" + dbname + "?user=" + user + "&password=" + password;
        try {
            Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
            result = stmt.executeUpdate(sql);
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static ResultSet executeQuery(String host, String dbname, String user, String password, String sql) {
        ResultSet result = null;
        String url = "jdbc:TAOS-RS://" + host + ":6041/" + dbname + "?user=" + user + "&password=" + password;
        try {
            Connection connection = DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
            result = stmt.executeQuery(sql);
            stmt.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
