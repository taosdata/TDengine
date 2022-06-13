package com.taos.example;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectCloudExample {
    public static void main(String[] args) throws SQLException {
        String host = System.getenv("TDENGINE_CLOUD_HOST");
        String port = System.getenv("TDENGINE_CLOUD_PORT");
        String token = System.getenv("TDENGINE_CLOUD_TOKEN");
        String user = System.getenv("TDENGINE_USER_NAME");
        String password = System.getenv("TDENGINE_PASSWORD");
        String jdbcUrl = String.format("jdbc:TAOS-RS://%s:%s?user=%s&password=%s", host, port, user, password);
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TOKEN, token);
        Connection conn = DriverManager.getConnection(jdbcUrl);
        System.out.println("Connected");
        conn.close();
    }
}
