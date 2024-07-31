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
        // handle any errors, please refer to the JDBC specifications for detailed exceptions info
        System.out.println("SQLState: " + ex.getSQLState());
        System.out.println("Error Code: " + ex.getErrorCode());
        System.out.println("Message: " + ex.getMessage());
    }
}
// ANCHOR_END: main
}