package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RESTConnectExample {
    // ANCHOR: main
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        try (Connection conn = DriverManager.getConnection(jdbcUrl)){
            System.out.println("Connected");

            // you can use the connection for execute SQL here

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
        }
    }
    // ANCHOR_END: main
}