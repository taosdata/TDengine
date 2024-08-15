package com.taosdata.example;

import com.taosdata.jdbc.AbstractStatement;

import java.sql.*;
import java.util.Properties;

public class JdbcInsertDataDemo {
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
// ANCHOR: insert_data
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             Statement stmt = connection.createStatement()) {

            // insert data, please make sure the database and table are created before
            String insertQuery = "INSERT INTO " +
                    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 219, 0.31000) " +
                    "(NOW + 2a, 12.60000, 218, 0.33000) " +
                    "(NOW + 3a, 12.30000, 221, 0.31000) " +
                    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
                    "VALUES " +
                    "(NOW + 1a, 10.30000, 218, 0.25000) ";
            int affectedRows = stmt.executeUpdate(insertQuery);
            // you can check affectedRows here
            System.out.println("Successfully inserted " + affectedRows + " rows to power.meters.");
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to insert data to power.meters, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
// ANCHOR_END: insert_data
    }
}
