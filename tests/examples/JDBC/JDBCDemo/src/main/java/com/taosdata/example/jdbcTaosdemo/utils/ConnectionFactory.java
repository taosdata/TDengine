package com.taosdata.example.jdbcTaosdemo.utils;

import com.taosdata.example.jdbcTaosdemo.domain.JdbcTaosdemoConfig;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory {

    public static Connection build(JdbcTaosdemoConfig config) throws SQLException {
        return build(config.host, config.port, config.database, config.user, config.password);
    }

    public static Connection build(String host, int port, String dbName) throws SQLException {
        return build(host, port, dbName, "root", "taosdata");
    }

    private static Connection build(String host, int port, String dbName, String user, String password) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, user);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, password);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        return DriverManager.getConnection("jdbc:TAOS://" + host + ":" + port + "/" + dbName + "", properties);
    }


}
