package com.taosdata.jdbc.lib;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TSDBCommon {

    public static Connection getConn(String host) throws SQLException, ClassNotFoundException {
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        return DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
    }

    public static void createDatabase(Connection connection, String dbName) throws SQLException {
        Statement statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("use " + dbName);
        statement.close();
    }

    public static void createStable(Connection connection, String stbName) throws SQLException {
        Statement statement = connection.createStatement();
        String createTableSql = "create table " + stbName + "(ts timestamp, f1 int, f2 int, f3 int) tags(areaid int, loc binary(20))";
        statement.executeUpdate(createTableSql);
        statement.close();
    }

    public static void createTables(Connection connection, int numOfTables, String stbName,String tablePrefix) throws SQLException {
        Statement statement = connection.createStatement();
        for(int i = 0; i < numOfTables; i++) {
            String loc = i % 2 == 0 ? "beijing" : "shanghai";
            String createSubTalbesSql = "create table " + tablePrefix + i + " using " + stbName + " tags(" + i + ", '" + loc + "')";
            statement.executeUpdate(createSubTalbesSql);
        }
        statement.close();
    }
}
