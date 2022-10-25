package com.taos.example;

import java.sql.*;

public class RestQueryExample {
    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041/power?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void printRow(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String value = rs.getString(i);
            System.out.print(value);
            System.out.print("\t");
        }
        System.out.println();
    }

    private static void printColName(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String colLabel = meta.getColumnLabel(i);
            System.out.print(colLabel);
            System.out.print("\t");
        }
        System.out.println();
    }

    private static void processResult(ResultSet rs) throws SQLException {
        printColName(rs);
        while (rs.next()) {
            printRow(rs);
        }
    }

    private static void queryData() throws SQLException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT AVG(voltage) FROM meters GROUP BY location");
                processResult(rs);
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        queryData();
    }
}

// possible output:
// avg(voltage)	location
// 222.0	California.LosAngeles
// 219.0	California.SanFrancisco
