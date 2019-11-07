package com.taosdata.iot.javaTestSuit.utils;

import com.google.common.base.Strings;
import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionFactory {

    /**
     * Default connection params are defined below.
     */
    // jdbc:TAOS://127.0.0.1:0/db?user=root&password=taosdata
    private static final String DEFAULT_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String JDBC_PROTOCOL = "jdbc:TSDB://";
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final String DEFAULT_PORT = "0";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "taosdata";
    private static final String DEFAULT_DBNAME = "";
    private static final String DEFAULT_JDBC_URL = JDBC_PROTOCOL + DEFAULT_HOST + ":" + DEFAULT_PORT + "/";
    private static final String DEFAULT_FULL_JDBC_URL = DEFAULT_JDBC_URL + DEFAULT_DBNAME + "?user=" + DEFAULT_USER + "&password=" + DEFAULT_PASSWORD;


    /**
     * Generate a java.sql.Connection instance using the default properties
     * @return TSDBConnectionImpl
     */
    public Connection getConnection() {

        System.out.printf("%s: connecting to host: %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
        Connection connection = null;
        try {
            Class.forName(DEFAULT_DRIVER);
            connection = DriverManager.getConnection(DEFAULT_FULL_JDBC_URL);
        } catch (Exception e) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
            e.printStackTrace();
        } catch (Error error) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
            error.printStackTrace();
        }
        return connection;
    }

    /**
     * Generate a java.sql.Connection instance using the user-provided properties
     */
    public Connection getConnection(Properties properties) {

        Connection connection = null;
        try {
            Class.forName(DEFAULT_DRIVER);
            if (properties == null) {
                connection = getConnection();
            } else {
                // if user/password is null, set it to default value
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_USER))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, DEFAULT_USER);
                }
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, DEFAULT_PASSWORD);
                }
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_PORT, DEFAULT_PORT);
                }
                System.out.printf("%s: connecting to host: %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
                connection = DriverManager.getConnection(DEFAULT_JDBC_URL, properties);
            }

        } catch (Exception e) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
            e.printStackTrace();
        } catch (Error error) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), DEFAULT_HOST);
            error.printStackTrace();
        }

        return connection;
    }

    /**
     * Generate a java.sql.Connection instance using the user-provided url and properties
     */
    public Connection getConnection(String host, Properties properties) {

        System.out.printf("%s: connecting to host: %s\n", Thread.currentThread().getName(), host);
        Connection connection = null;
        String url = JDBC_PROTOCOL + host + ":" + DEFAULT_PORT + "/";
        try {
            Class.forName(DEFAULT_DRIVER);
            if (properties == null) {
                properties = new Properties();
            } else {
                // if user/password is null, set it to default value
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_USER))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, DEFAULT_USER);
                }
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_PASSWORD))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, DEFAULT_PASSWORD);
                }
                if (Strings.isNullOrEmpty(properties.getProperty(TSDBDriver.PROPERTY_KEY_PORT))) {
                    properties.setProperty(TSDBDriver.PROPERTY_KEY_PORT, DEFAULT_PORT);
                }
            }
            connection = DriverManager.getConnection(url, properties);

        } catch (Exception e) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), host);
            e.printStackTrace();
        } catch (Error error) {
            System.out.printf("%s: failed to connect to %s\n", Thread.currentThread().getName(), host);
            error.printStackTrace();
        }

        return connection;
    }
}
