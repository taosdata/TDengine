package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RESTConnectExample {
    // ANCHOR: main
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
    // ANCHOR_END: main
}