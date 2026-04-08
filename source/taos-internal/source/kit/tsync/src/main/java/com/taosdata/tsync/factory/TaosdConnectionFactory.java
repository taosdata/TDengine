package com.taosdata.tsync.factory;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.tsync.entity.config.TaosdConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TaosdConnectionFactory {
    public static Connection build(TaosdConfiguration taosdConfiguration) {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, taosdConfiguration.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, taosdConfiguration.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, taosdConfiguration.getCharset());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, taosdConfiguration.getLocale());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, taosdConfiguration.getTimezone());

        final String url = "jdbc:TAOS-RS://" + taosdConfiguration.getHost() + ":" + taosdConfiguration.getPort() + "/";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
