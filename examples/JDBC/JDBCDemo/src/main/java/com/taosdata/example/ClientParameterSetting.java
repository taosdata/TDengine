package com.taosdata.example;

import com.taosdata.jdbc.TSDBDriver;

import java.sql.*;
import java.util.Properties;

public class ClientParameterSetting {
    private static final String host = "127.0.0.1";

    public static void main(String[] args) throws SQLException {
        setParameterInJdbcUrl();

        setParameterInProperties();
    }

    private static void setParameterInJdbcUrl() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/?debugFlag=135&asyncLog=0";

        Connection connection = DriverManager.getConnection(jdbcUrl, "root", "taosdata");

        printDatabase(connection);

        connection.close();
    }

    private static void setParameterInProperties() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "taosdata");
        properties.setProperty("debugFlag", "135");
        properties.setProperty("asyncLog", "0");
        properties.setProperty("maxSQLLength", "1048576");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, properties)) {
            printDatabase(conn);
        }
    }

    private static void printDatabase(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show databases");

            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        }
    }
}
