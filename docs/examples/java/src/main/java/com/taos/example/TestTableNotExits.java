package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestTableNotExits {
    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }
    public static void main(String[] args) throws SQLException {
        try(Connection conn = getConnection()) {
            try(Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate("insert into test.t1 values(1, 2) test.t2 values(3, 4)");
                } catch (SQLException e) {
                    System.out.println(e.getErrorCode());
                    System.out.println(Integer.toHexString(e.getErrorCode()));
                    System.out.println(e);
                }
            }
        }
    }
}
