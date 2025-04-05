package com.taos.example;

import java.sql.*;
import java.util.Random;

// ANCHOR: para_bind
public class WSParameterBindingStdInterfaceDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int NUM_OF_SUB_TABLE = 10, NUM_OF_ROW = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-WS://" + host + ":6041";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {
            init(conn);

            // If you are certain that the child table exists, you can avoid binding the tag column to improve performance.
            String sql = "INSERT INTO power.meters (tbname, groupid, location, ts, current, voltage, phase) VALUES (?,?,?,?,?,?,?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                long current = System.currentTimeMillis();

                for (int i = 1; i <= NUM_OF_SUB_TABLE; i++) {
                    for (int j = 0; j < NUM_OF_ROW; j++) {
                        pstmt.setString(1, "d_bind_" + i);

                        pstmt.setInt(2, i);
                        pstmt.setString(3, "location_" + i);

                        pstmt.setTimestamp(4, new Timestamp(current + j));
                        pstmt.setFloat(5, random.nextFloat() * 30);
                        pstmt.setInt(6, random.nextInt(300));
                        pstmt.setFloat(7, random.nextFloat());
                        pstmt.addBatch();
                    }
                }
                int[] exeResult = pstmt.executeBatch();
                // you can check exeResult here
                System.out.println("Successfully inserted " + exeResult.length + " rows to power.meters.");
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
