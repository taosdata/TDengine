package com.taosdata.example;

import java.sql.*;
import java.util.Properties;

public class JDBCDemo {
    private static String host;
    private static String driverType;
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private Connection connection;

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1)
                host = args[++i];
            if ("-driverType".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                driverType = args[++i];
                if (!"jni".equalsIgnoreCase(driverType) && !"restful".equalsIgnoreCase(driverType))
                    printHelp();
            }
        }

        if (host == null || driverType == null) {
            printHelp();
        }

        JDBCDemo demo = new JDBCDemo();
        demo.init();
        demo.createDatabase();
        demo.useDatabase();
        demo.dropTable();
        demo.createTable();
        demo.insert();
        demo.select();
        demo.dropTable();
        demo.close();
    }

    private void init() {
        // get connection
        try {
            String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
            if (driverType.equals("restful")) {
                Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
                url = "jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata";
            } else {
                Class.forName("com.taosdata.jdbc.TSDBDriver");
            }
            Properties properties = new Properties();
            properties.setProperty("host", host);
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection(url, properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void createDatabase() {
        String sql = "create database if not exists " + dbName;
        exuete(sql);
    }

    private void useDatabase() {
        String sql = "use " + dbName;
        exuete(sql);
    }

    private void select() {
        final String sql = "select * from test.weather";
        executeQuery(sql);
    }

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                System.out.printf("%s: %s\t", columnLabel, value);
            }
            System.out.println();
        }
    }

    private void insert() {
        final String sql = "insert into test.weather (ts, temperature, humidity) values(now, 20.5, 34)";
        exuete(sql);
    }

    private void createTable() {
        final String sql = "create table if not exists " + dbName + "." + tbName + " (ts timestamp, temperature float, humidity int)";
        exuete(sql);
    }

    private void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void exuete(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, execute, (end - start));
        } catch (SQLException e) {
            e.printStackTrace();

        }
    }

    private void close() {
        try {
            if (connection != null) {
                this.connection.close();
                System.out.println("connection closed.");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void dropTable() {
        final String sql = "drop table if exists " + dbName + "." + tbName + "";
        exuete(sql);
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JdbcDemo.jar -host <hostname> -driverType <jni|restful>");
        System.exit(0);
    }


}
