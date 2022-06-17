package com.taos.example;

// ANCHOR: connect
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement();
        // test the connection by firing a query
        stmt.executeQuery("select server_version()");
        stmt.close();
        conn.close();
    }
}
// ANCHOR_END: connect
