package com.taos.example;

import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

public class StmtInsertExample {
    private static String datePattern = "yyyy-MM-dd HH:mm:ss.SSS";
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern(datePattern);

    private static List<String> getRawData(int size) {
        SimpleDateFormat format = new SimpleDateFormat(datePattern);
        List<String> result = new ArrayList<>();
        long current = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            String time = format.format(current + i);
            int id = random.nextInt(10);
            result.add("d" + id + "," + time + ",10.30000,219,0.31000,California.SanFrancisco,2");
        }
        return result.stream()
                .sorted(Comparator.comparing(s -> s.split(",")[0])).collect(Collectors.toList());
    }

    private static Connection getConnection() throws SQLException {
        String jdbcUrl = "jdbc:TAOS://localhost:6030?user=root&password=taosdata";
        return DriverManager.getConnection(jdbcUrl);
    }

    private static void createTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE if not exists power KEEP 3650");
            stmt.executeUpdate("use power");
            stmt.execute("CREATE STABLE if not exists meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) " +
                    "TAGS (location BINARY(64), groupId INT)");
        }
    }

    private static void insertData() throws SQLException {
        try (Connection conn = getConnection()) {
            createTable(conn);
            String psql = "INSERT INTO ? USING power.meters TAGS(?, ?) VALUES(?, ?, ?, ?)";
            try (TSDBPreparedStatement pst = (TSDBPreparedStatement) conn.prepareStatement(psql)) {
                String tableName = null;
                ArrayList<Long> ts = new ArrayList<>();
                ArrayList<Float> current = new ArrayList<>();
                ArrayList<Integer> voltage = new ArrayList<>();
                ArrayList<Float> phase = new ArrayList<>();
                for (String line : getRawData(100000)) {
                    String[] ps = line.split(",");
                    if (tableName == null) {
                        // bind table name and tags
                        tableName = "power." + ps[0];
                        pst.setTableName(ps[0]);
                        pst.setTagString(0, ps[5]);
                        pst.setTagInt(1, Integer.valueOf(ps[6]));
                    } else {
                        if (!tableName.equals(ps[0])) {
                            pst.setTimestamp(0, ts);
                            pst.setFloat(1, current);
                            pst.setInt(2, voltage);
                            pst.setFloat(3, phase);
                            pst.columnDataAddBatch();
                            pst.columnDataExecuteBatch();

                            // bind table name and tags
                            tableName = ps[0];
                            pst.setTableName(ps[0]);
                            pst.setTagString(0, ps[5]);
                            pst.setTagInt(1, Integer.valueOf(ps[6]));
                            ts.clear();
                            current.clear();
                            voltage.clear();
                            phase.clear();
                        }
                    }
                    // bind values
                    // ps[1] looks like: 2018-10-03 14:38:05.000
                    LocalDateTime localDateTime = LocalDateTime.parse(ps[1], formatter);
                    ts.add(localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
                    current.add(Float.valueOf(ps[2]));
                    voltage.add(Integer.valueOf(ps[3]));
                    phase.add(Float.valueOf(ps[4]));
                }
                pst.setTimestamp(0, ts);
                pst.setFloat(1, current);
                pst.setInt(2, voltage);
                pst.setFloat(3, phase);
                pst.columnDataAddBatch();
                pst.columnDataExecuteBatch();
            }
        }
    }

    public static void main(String[] args) throws SQLException {
        insertData();
    }
}
