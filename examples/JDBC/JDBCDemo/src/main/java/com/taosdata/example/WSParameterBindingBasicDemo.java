package com.taosdata.example;

import com.taosdata.jdbc.TSDBPreparedStatement;
import com.taosdata.jdbc.ws.TSWSPreparedStatement;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

// ANCHOR: para_bind
public class WSParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-RS://" + host + ":6041/?batchfetch=true";
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata");

        init(conn);

        init(conn);

        String sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";

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
                pstmt.executeBatch();
            }
        }

        conn.close();
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS power");
            stmt.execute("USE power");
            stmt.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
// ANCHOR_END: para_bind
