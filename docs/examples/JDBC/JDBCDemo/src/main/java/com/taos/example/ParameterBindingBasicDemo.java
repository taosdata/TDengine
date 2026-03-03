package com.taos.example;

import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

// ANCHOR: para_bind
public class ParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {

            init(conn);

            String sql = "INSERT INTO ? USING power.meters TAGS(?,?) VALUES (?,?,?,?)";

            try (TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class)) {

                for (int i = 1; i <= numOfSubTable; i++) {
                    // set table name
                    pstmt.setTableName("d_bind_" + i);

                    // set tags
                    pstmt.setTagInt(0, i);
                    pstmt.setTagString(1, "location_" + i);

                    // set column ts
                    ArrayList<Long> tsList = new ArrayList<>();
                    long current = System.currentTimeMillis();
                    for (int j = 0; j < numOfRow; j++)
                        tsList.add(current + j);
                    pstmt.setTimestamp(0, tsList);

                    // set column current
                    ArrayList<Float> currentList = new ArrayList<>();
                    for (int j = 0; j < numOfRow; j++)
                        currentList.add(random.nextFloat() * 30);
                    pstmt.setFloat(1, currentList);

                    // set column voltage
                    ArrayList<Integer> voltageList = new ArrayList<>();
                    for (int j = 0; j < numOfRow; j++)
                        voltageList.add(random.nextInt(300));
                    pstmt.setInt(2, voltageList);

                    // set column phase
                    ArrayList<Float> phaseList = new ArrayList<>();
                    for (int j = 0; j < numOfRow; j++)
                        phaseList.add(random.nextFloat());
                    pstmt.setFloat(3, phaseList);
                    // add column
                    pstmt.columnDataAddBatch();
                }
                // execute column
                pstmt.columnDataExecuteBatch();
                // you can check exeResult here
                System.out.println("Successfully inserted " + (numOfSubTable * numOfRow) + " rows to power.meters.");
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
            stmt.execute("CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
        }
    }
}
// ANCHOR_END: para_bind
