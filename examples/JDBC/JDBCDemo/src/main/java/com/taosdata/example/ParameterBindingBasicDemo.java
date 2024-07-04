package com.taosdata.example;

import com.taosdata.jdbc.TSDBPreparedStatement;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// ANCHOR: para_bind
public class ParameterBindingBasicDemo {

    // modify host to your own
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata");

        init(conn);

        String sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";

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
                ArrayList<Float> f1List = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    f1List.add(random.nextFloat() * 30);
                pstmt.setFloat(1, f1List);

                // set column voltage
                ArrayList<Integer> f2List = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    f2List.add(random.nextInt(300));
                pstmt.setInt(2, f2List);

                // set column phase
                ArrayList<Float> f3List = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    f3List.add(random.nextFloat());
                pstmt.setFloat(3, f3List);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
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
