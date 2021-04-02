package com.taosdata.jdbc.utils;

import java.sql.*;

public class SQLExecutor {

    // insert, import
    public static void executeUpdate(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            int affectedRows = statement.executeUpdate(sql);
            long end = System.currentTimeMillis();
            System.out.println("[ affected rows : " + affectedRows + " ] time cost: " + (end - start) + " ms, execute statement ====> " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // show databases, show tables, show stables
    public static void executeWithResult(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            printResult(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // use database, create database, create table, drop table...
    public static void execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            boolean execute = statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, execute, (end - start));
        } catch (SQLException e) {
            System.out.println("ERROR execute SQL ===> " + sql);
            e.printStackTrace();
        }
    }

    // select
    public static void executeQuery(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, true, (end - start));
            printResult(resultSet);
        } catch (SQLException e) {
            System.out.println("ERROR execute SQL ===> " + sql);
            e.printStackTrace();
        }
    }

    private static void printSql(String sql, boolean succeed, long cost) {
        System.out.println("[ " + (succeed ? "OK" : "ERROR!") + " ] time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnLabel = metaData.getColumnLabel(i);
                String value = resultSet.getString(i);
                sb.append(columnLabel + ": " + value + "\t");
            }
            System.out.println(sb.toString());
        }
    }

}
