package com.taosdata.example;

import com.alibaba.fastjson.JSON;
import com.taosdata.jdbc.AbstractStatement;

import java.sql.*;
import java.util.Properties;

public class JdbcBasicDemo {
    private static final String host = "localhost";
    private static final String dbName = "test";
    private static final String tbName = "weather";
    private static final String user = "root";
    private static final String password = "taosdata";


    public static void main(String[] args) throws SQLException {

final String url = "jdbc:TAOS://" + host + ":6030/?user=" + user + "&password=" + password;
Connection connection;

// get connection
Properties properties = new Properties();
properties.setProperty("charset", "UTF-8");
properties.setProperty("locale", "en_US.UTF-8");
properties.setProperty("timezone", "UTC-8");
System.out.println("get connection starting...");
connection = DriverManager.getConnection(url, properties);
if (connection != null){
    System.out.println("[ OK ] Connection established.");
} else {
    System.out.println("[ ERR ] Connection can not be established.");
    return;
}

Statement stmt = connection.createStatement();

// ANCHOR: create_db_and_table
// create database
stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS power");

// use database
stmt.executeUpdate("USE power");

// create table
stmt.executeUpdate("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
// ANCHOR_END: create_db_and_table

// ANCHOR: insert_data
// insert data
String insertQuery = "INSERT INTO " +
    "power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
    "VALUES " +
    "(NOW + 1a, 10.30000, 219, 0.31000) " +
    "(NOW + 2a, 12.60000, 218, 0.33000) " +
    "(NOW + 3a, 12.30000, 221, 0.31000) " +
    "power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
    "VALUES " +
    "(NOW + 1a, 10.30000, 218, 0.25000) ";
int affectedRows = stmt.executeUpdate(insertQuery);
System.out.println("insert " + affectedRows + " rows.");
// ANCHOR_END: insert_data


// ANCHOR: query_data
// query data
ResultSet resultSet = stmt.executeQuery("SELECT * FROM meters");

Timestamp ts;
float current;
String location;
while(resultSet.next()){
    ts = resultSet.getTimestamp(1);
    current = resultSet.getFloat(2);
    location = resultSet.getString("location");

    System.out.printf("%s, %f, %s\n", ts, current, location);
}
// ANCHOR_END: query_data

// ANCHOR: with_reqid
AbstractStatement aStmt = (AbstractStatement) connection.createStatement();
aStmt.execute("CREATE DATABASE IF NOT EXISTS power", 1L);
aStmt.executeUpdate("USE power", 2L);
try (ResultSet rs = aStmt.executeQuery("SELECT * FROM meters limit 1", 3L)) {
    while(rs.next()){
        Timestamp timestamp = rs.getTimestamp(1);
        System.out.println("timestamp = " + timestamp);
    }
}
aStmt.close();
// ANCHOR_END: with_reqid


String sql = "SELECT * FROM meters limit 2;";

// ANCHOR: jdbc_exception
try (Statement statement = connection.createStatement()) {
    // executeQuery
    ResultSet tempResultSet = statement.executeQuery(sql);
    // print result
    printResult(tempResultSet);
} catch (SQLException e) {
    System.out.println("ERROR Message: " + e.getMessage());
    System.out.println("ERROR Code: " + e.getErrorCode());
    e.printStackTrace();
}
// ANCHOR_END: jdbc_exception
    }

    private static void printResult(ResultSet resultSet) throws SQLException {
        Util.printResult(resultSet);
    }

}
