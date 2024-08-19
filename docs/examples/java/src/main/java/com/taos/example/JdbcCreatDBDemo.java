package com.taos.example;

import java.sql.*;
import java.util.Properties;

public class JdbcCreatDBDemo {
    private static final String host = "localhost";
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private static final String user = "root";
    private static final String password = "taosdata";


    public static void main(String[] args) throws SQLException {

        final String jdbcUrl = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;

// get connection
        Properties properties = new Properties();
        properties.setProperty("charset", "UTF-8");
        properties.setProperty("locale", "en_US.UTF-8");
        properties.setProperty("timezone", "UTC-8");
        System.out.println("get connection starting...");
// ANCHOR: create_db_and_table
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement()) {

            // create database
            int rowsAffected = stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power");
            // you can check rowsAffected here
            System.out.println("Create database power successfully, rowsAffected: " + rowsAffected);
            // create table
            rowsAffected = stmt.executeUpdate("CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
            // you can check rowsAffected here
            System.out.println("Create stable power.meters successfully, rowsAffected: " + rowsAffected);
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to create database power or stable meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
// ANCHOR_END: create_db_and_table

    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        Util.printResult(resultSet);
    }

}
