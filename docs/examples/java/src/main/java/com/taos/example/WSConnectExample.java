package com.taos.example;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class WSConnectExample {
// ANCHOR: main
public static void main(String[] args) throws SQLException {
    // use
    // String jdbcUrl = "jdbc:TAOS-RS://localhost:6041/dbName?user=root&password=taosdata";
    // if you want to connect a specified database named "dbName".
    String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
    Properties connProps = new Properties();
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
    connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

    try (Connection conn = DriverManager.getConnection(jdbcUrl, connProps)){
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
