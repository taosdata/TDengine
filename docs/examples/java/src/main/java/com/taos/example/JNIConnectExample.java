package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.taosdata.jdbc.TSDBDriver;

public class JNIConnectExample {
    public static void main(String[] args) throws SQLException {
        // use
        // String jdbcUrl = "jdbc:TAOS://localhost:6030/dbName?user=root&password=taosdata";
        // if you want to connect a specified database named "dbName".
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try {
            Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
            System.out.println("Connected");

            // you can use the connection for execute SQL here

            conn.close();
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
        }
    }
}

