package com.taosdata.example;

import com.taosdata.jdbc.AbstractStatement;

import java.sql.*;
import java.util.Properties;

public class JdbcReqIdDemo {
    private static final String host = "localhost";
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private static final String user = "root";
    private static final String password = "taosdata";


    public static void main(String[] args) throws SQLException {

final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;

// get connection
Properties properties = new Properties();
properties.setProperty("charset", "UTF-8");
properties.setProperty("locale", "en_US.UTF-8");
properties.setProperty("timezone", "UTC-8");
System.out.println("get connection starting...");

// ANCHOR: with_reqid
try (Connection connection = DriverManager.getConnection(url, properties);
     // Create a statement that allows specifying a request ID
     AbstractStatement aStmt = (AbstractStatement) connection.createStatement()) {

    boolean hasResultSet = aStmt.execute("CREATE DATABASE IF NOT EXISTS power", 1L);
    assert !hasResultSet;

    int rowsAffected = aStmt.executeUpdate("USE power", 2L);
    assert rowsAffected == 0;

    try (ResultSet rs = aStmt.executeQuery("SELECT * FROM meters limit 1", 3L)) {
        while (rs.next()) {
            Timestamp timestamp = rs.getTimestamp(1);
            System.out.println("timestamp = " + timestamp);
        }
    }
} catch (SQLException ex) {
    // handle any errors
    System.out.println("SQLException: " + ex.getMessage());
}
// ANCHOR_END: with_reqid
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        Util.printResult(resultSet);
    }

}
