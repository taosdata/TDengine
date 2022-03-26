package com.taos.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;


public class RestInsertExample {
    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static List<String> getRawData() {
        return Arrays.asList(
                "d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,Beijing.Chaoyang,2",
                "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,Beijing.Chaoyang,2",
                "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,Beijing.Chaoyang,2",
                "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,Beijing.Chaoyang,3",
                "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,Beijing.Haidian,2",
                "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,Beijing.Haidian,2",
                "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,Beijing.Haidian,3",
                "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,Beijing.Haidian,3"
        );
    }


    /**
     * The generated SQL is:
     * INSERT INTO power.d1001 USING power.meters TAGS(Beijing.Chaoyang, 2) VALUES('2018-10-03 14:38:05.000',10.30000,219,0.31000)
     *         power.d1001 USING power.meters TAGS(Beijing.Chaoyang, 2) VALUES('2018-10-03 14:38:15.000',12.60000,218,0.33000)
     *         power.d1001 USING power.meters TAGS(Beijing.Chaoyang, 2) VALUES('2018-10-03 14:38:16.800',12.30000,221,0.31000)
     *         power.d1002 USING power.meters TAGS(Beijing.Chaoyang, 3) VALUES('2018-10-03 14:38:16.650',10.30000,218,0.25000)
     *         power.d1003 USING power.meters TAGS(Beijing.Haidian, 2) VALUES('2018-10-03 14:38:05.500',11.80000,221,0.28000)
     *         power.d1003 USING power.meters TAGS(Beijing.Haidian, 2) VALUES('2018-10-03 14:38:16.600',13.40000,223,0.29000)
     *         power.d1004 USING power.meters TAGS(Beijing.Haidian, 3) VALUES('2018-10-03 14:38:05.000',10.80000,223,0.29000)
     *         power.d1004 USING power.meters TAGS(Beijing.Haidian, 3) VALUES('2018-10-03 14:38:06.500',11.50000,221,0.35000)
     */
    private static String getSQL() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        for (String line : getRawData()) {
            String[] fields = line.split(",");
            sb.append("power." + fields[0]).append(" USING power.meters TAGS(")
                    .append(fields[5]).append(", ")
                    .append(fields[6])
                    .append(") VALUES(")
                    .append('\'').append(fields[1]).append('\'').append(",")
                    .append(fields[2]).append(",")
                    .append(fields[3]).append(",")
                    .append(fields[4]).append(") ");
        }
        return sb.toString();
    }

    public static void insertData() throws SQLException {
        try(Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE DATABASE power KEEP 3650");
                stmt.execute("CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                        "TAGS (location BINARY(64), groupId INT)");
                String sql = getSQL();
                int rowCount = stmt.executeUpdate(sql);
                System.out.println("rowCount=" + rowCount); // rowCount=8
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        insertData();
    }
}
