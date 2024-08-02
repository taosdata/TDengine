package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RESTConnectExample {
// ANCHOR: main
public static void main(String[] args) throws SQLException {
    String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
    try (Connection conn = DriverManager.getConnection(jdbcUrl)){
        System.out.println("Connected to " + jdbcUrl + " successfully.");

        // you can use the connection for execute SQL here

    } catch (SQLException ex) {
        // handle any errors, please refer to the JDBC specifications for detailed exceptions info
        System.out.println("Failed to connect to " + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
        throw ex;
    } catch (Exception ex){
        System.out.println("Failed to connect to " + jdbcUrl + "; ErrMessage: " + ex.getMessage());
        throw ex;
    }
}
// ANCHOR_END: main
}