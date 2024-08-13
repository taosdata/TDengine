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

        final String jdbcUrl = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;

// get connection
        Properties properties = new Properties();
        properties.setProperty("charset", "UTF-8");
        properties.setProperty("locale", "en_US.UTF-8");
        properties.setProperty("timezone", "UTC-8");
        System.out.println("get connection starting...");

// ANCHOR: with_reqid
        long reqId = 3L;
        try (Connection connection = DriverManager.getConnection(jdbcUrl, properties);
             // Create a statement that allows specifying a request ID
             AbstractStatement aStmt = (AbstractStatement) connection.createStatement()) {

            try (ResultSet resultSet = aStmt.executeQuery("SELECT ts, current, location FROM power.meters limit 1", reqId)) {
                Timestamp ts;
                float current;
                String location;
                while (resultSet.next()) {
                    ts = resultSet.getTimestamp(1);
                    current = resultSet.getFloat(2);
                    // we recommend using the column name to get the value
                    location = resultSet.getString("location");

                    // you can check data here
                    System.out.printf("ts: %s, current: %f, location: %s %n", ts, current, location);

                }
            }
        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to execute sql with reqId: " + reqId + ", url:" + jdbcUrl + "; ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw ex;
        } catch (Exception ex){
            System.out.println("Failed to execute sql with reqId: " + reqId + ", url:" + jdbcUrl + "; ErrMessage: " + ex.getMessage());
            throw ex;
        }
// ANCHOR_END: with_reqid
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        Util.printResult(resultSet);
    }

}
