package com.taos.example;
import com.taosdata.jdbc.TSDBDriver;

// ANCHOR: connect
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String jdbcUrl = System.getenv("TDENGINE_JDBC_URL");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        conn.close();
    }
}
// ANCHOR_END: connect
