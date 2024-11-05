package com.taosdata.example;

import java.sql.*;
import java.util.Properties;

import com.sun.org.apache.bcel.internal.generic.ACONST_NULL;
import com.taosdata.jdbc.TSDBDriver;

public class JdbcDemo {
    private static String host = "localhost";
    private static final String dbName = "power";
    private static final String tbName = "meters";
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

        try {
           Connection restCon = demo.getRestConn();
           restCon.close();
        }catch (Exception e){
            System.out.println(e);
        }
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
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName;
        execute(sql);
    }

    private void useDatabase() {
        String sql = "USE " + dbName;
        execute(sql);
    }

    private void createTable() {
        final String sql = "CREATE STABLE IF NOT EXISTS " + dbName + "." + tbName + " (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, `location` VARCHAR(24))";
        execute(sql);
    }

    private void dropTable() {
        final String sql = "DROP STABLE IF EXISTS " + dbName + "." + tbName + "";
        execute(sql);
    }

    private void insert() {
        final String sql = "INSERT INTO " + dbName + "." + tbName + " (tbname, location, groupId, ts, current, voltage, phase)" +
                            "values('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:34.630', 10.2, 219, 0.32)" +
                            "('d31001', 'California.SanFrancisco', 2, '2021-07-13 14:06:35.779', 10.15, 217, 0.33)" +
                            "('d31002', NULL, 2, '2021-07-13 14:06:34.255', 10.15, 217, 0.33)";
        execute(sql);
    }

    private void select() {
        final String sql = "SELECT * FROM " + dbName + "." + tbName;
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
            Util.printResult(resultSet);
        } catch (SQLException e) {
            long end = System.currentTimeMillis();
            printSql(sql, false, (end - start));
            e.printStackTrace();
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

    public Connection getRestConn() throws Exception{
        Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041/power?user=root&password=taosdata";
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        Connection conn = DriverManager.getConnection(jdbcUrl, connProps);
        return conn;
    }

}
