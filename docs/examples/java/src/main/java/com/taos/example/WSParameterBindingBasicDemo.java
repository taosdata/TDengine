package com.taos.example;

import com.taosdata.jdbc.ws.TSWSPreparedStatement;

import java.sql.*;
import java.util.Random;

// ANCHOR: para_bind
public class WSParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-WS://" + host + ":6041";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {
            init(conn);

            String sql = "INSERT INTO ? USING power.meters TAGS(?,?) VALUES (?,?,?,?)";

            try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

                for (int i = 1; i <= numOfSubTable; i++) {
                    // set table name
                    pstmt.setTableName("d_bind_" + i);

                    // set tags
                    pstmt.setTagInt(0, i);
                    pstmt.setTagString(1, "location_" + i);

                    // set columns
                    long current = System.currentTimeMillis();
                    for (int j = 0; j < numOfRow; j++) {
                        pstmt.setTimestamp(1, new Timestamp(current + j));
                        pstmt.setFloat(2, random.nextFloat() * 30);
                        pstmt.setInt(3, random.nextInt(300));
                        pstmt.setFloat(4, random.nextFloat());
                        pstmt.addBatch();
                    }
                    int[] exeResult = pstmt.executeBatch();
                    // you can check exeResult here
                    System.out.println("Successfully inserted " + exeResult.length + " rows to power.meters.");
                }
            }
        } catch (Exception ex) {
            // please refer to the JDBC specifications for detailed exceptions info
            System.out.printf("Failed to insert to table meters using stmt, %sErrMessage: %s%n",
                    ex instanceof SQLException ? "ErrCode: " + ((SQLException) ex).getErrorCode() + ", " : "",
                    ex.getMessage());
            // Print stack trace for context in examples. Use logging in production.
            ex.printStackTrace();
            throw ex;
        }
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
            stmt.execute(
                    "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
// ANCHOR_END: para_bind
