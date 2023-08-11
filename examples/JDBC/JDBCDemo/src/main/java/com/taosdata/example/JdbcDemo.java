package com.taosdata.example;

import java.sql.*;
import java.util.Properties;

public class JdbcDemo {
    private static String host;
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private static final String user = "root";
    private static final String password = "taosdata";

    private Connection connection;

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1)
                host = args[++i];
        }
        if (host == null) {
            printHelp();
        }
        JdbcDemo demo = new JdbcDemo();
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
        final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;
        // get connection
        try {
            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection(url, properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void createDatabase() {
        String sql = "create database if not exists " + dbName;
        execute(sql);
    }

    private void useDatabase() {
        String sql = "use " + dbName;
        execute(sql);
    }

    private void dropTable() {
        final String sql = "drop table if exists " + dbName + "." + tbName + "";
        execute(sql);
    }

    private void createTable() {
        final String sql = "create table if not exists " + dbName + "." + tbName + " (ts timestamp, temperature float, humidity int)";
        execute(sql);
    }

    private void insert() {
        final String sql = "insert into " + dbName + "." + tbName + " (ts, temperature, humidity) values(now, 20.5, 34)";
        execute(sql);
    }

    private void select() {
        final String sql = "select * from " + dbName + "." + tbName;
        executeQuery(sql);
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

    private void executeQuery(String sql) {
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            long end = System.currentTimeMillis();
            printSql(sql, false, (end - start));
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

    private void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void execute(String sql) {
        long start = System.currentTimeMillis();
        try (Statement statement = connection.createStatement()) {
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
        } catch (SQLException e) {
            long end = System.currentTimeMillis();
            printSql(sql, false, (end - start));
            e.printStackTrace();
        }
    }

    private static void printHelp() {
        System.out.println("Usage: java -jar JDBCDemo.jar -host <hostname>");
        System.exit(0);
    }

}
