package com.taosdata.example;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

public class JdbcChecker {
    private static String host;
    private static String dbName = "test";
    private static String tbName = "weather";
    private Connection connection;

    /**
     * get connection
     **/
    private void init() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException("connection failed: " + host);
        }
    }

    /**
     * create database
     */
    private void createDatabase() {
        String sql = "create database if not exists " + dbName;
        exuete(sql);
    }

    /**
     * use database
     */
    private void useDatabase() {
        String sql = "use " + dbName;
        exuete(sql);
    }

    /**
     * select
     */
    private void checkSelect() {
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

    private String formatString(String str) {
        StringBuilder sb = new StringBuilder();
        int blankCnt = (26 - str.length()) / 2;
        for (int j = 0; j < blankCnt; j++)
            sb.append(" ");
        sb.append(str);
        for (int j = 0; j < blankCnt; j++)
            sb.append(" ");
        sb.append("|");
        return sb.toString();
    }


    /**
     * insert
     */
    private void checkInsert() {
        final String sql = "insert into test.weather (ts, temperature, humidity) values(now, 20.5, 34)";
        exuete(sql);
    }

    /**
     * create table
     */
    private void createTable() {
        final String sql = "create table if not exists " + dbName + "." + tbName + " (ts timestamp, temperature float, humidity int)";
        exuete(sql);
    }

    private final void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private final void exuete(String sql) {
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

    private void checkDropTable() {
        final String sql = "drop table if exists " + dbName + "." + tbName + "";
        exuete(sql);
    }

    public static void main(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if ("-host".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                host = args[++i];
            }
            if ("-db".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                dbName = args[++i];
            }
            if ("-t".equalsIgnoreCase(args[i]) && i < args.length - 1) {
                tbName = args[++i];
            }
        }

        if (host == null) {
            System.out.println("Usage: java -jar JDBCConnectorChecker.jar -host <hostname>");
            return;
        }

        JdbcChecker checker = new JdbcChecker();
        checker.init();
        checker.createDatabase();
        checker.useDatabase();
        checker.checkDropTable();
        checker.createTable();
        checker.checkInsert();
        checker.checkSelect();
        checker.checkDropTable();
        checker.close();
    }

}
