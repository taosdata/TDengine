package com.taos.example;

import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StmtInsertExample {
    private static ArrayList<Long> tsToLongArray(String ts) {
        ArrayList<Long> result = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        LocalDateTime localDateTime = LocalDateTime.parse(ts, formatter);
        result.add(localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
        return result;
    }

    private static <T> ArrayList<T> toArray(T v) {
        ArrayList<T> result = new ArrayList<>();
        result.add(v);
        return result;
    }

    private static List<String> getRawData() {
        return Arrays.asList(
                "d1001,2018-10-03 14:38:05.000,10.30000,219,0.31000,California.SanFrancisco,2",
                "d1001,2018-10-03 14:38:15.000,12.60000,218,0.33000,California.SanFrancisco,2",
                "d1001,2018-10-03 14:38:16.800,12.30000,221,0.31000,California.SanFrancisco,2",
                "d1002,2018-10-03 14:38:16.650,10.30000,218,0.25000,California.SanFrancisco,3",
                "d1003,2018-10-03 14:38:05.500,11.80000,221,0.28000,California.LosAngeles,2",
                "d1003,2018-10-03 14:38:16.600,13.40000,223,0.29000,California.LosAngeles,2",
                "d1004,2018-10-03 14:38:05.000,10.80000,223,0.29000,California.LosAngeles,3",
                "d1004,2018-10-03 14:38:06.500,11.50000,221,0.35000,California.LosAngeles,3"
        );
    }

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void createTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE power KEEP 3650");
            stmt.executeUpdate("USE power");
            stmt.execute("CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                    "TAGS (location BINARY(64), groupId INT)");
        }
    }

    private static void insertData() throws SQLException {
        try (Connection conn = getConnection()) {
            createTable(conn);
            String psql = "INSERT INTO ? USING meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
            try (TSDBPreparedStatement pst = (TSDBPreparedStatement) conn.prepareStatement(psql)) {
                for (String line : getRawData()) {
                    String[] ps = line.split(",");
                    // bind table name and tags
                    pst.setTableName(ps[0]);
                    pst.setTagString(0, ps[5]);
                    pst.setTagInt(1, Integer.valueOf(ps[6]));
                    // bind values
                    pst.setTimestamp(0, tsToLongArray(ps[1])); //ps[1] looks like: 2018-10-03 14:38:05.000
                    pst.setFloat(1, toArray(Float.valueOf(ps[2])));
                    pst.setInt(2, toArray(Integer.valueOf(ps[3])));
                    pst.setFloat(3, toArray(Float.valueOf(ps[4])));
                    pst.columnDataAddBatch();
                }
                pst.columnDataExecuteBatch();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        insertData();
    }
}
