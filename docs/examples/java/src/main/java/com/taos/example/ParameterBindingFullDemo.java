package com.taos.example;

import com.taosdata.jdbc.TSDBPreparedStatement;
import com.taosdata.jdbc.utils.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

// ANCHOR: para_bind
public class ParameterBindingFullDemo {

    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int BINARY_COLUMN_SIZE = 100;
    private static final String[] schemaList = {
            "drop database if exists example_all_type_stmt",
            "CREATE DATABASE IF NOT EXISTS example_all_type_stmt",
            "USE example_all_type_stmt",
            "CREATE STABLE IF NOT EXISTS stb_json (" +
                    "ts TIMESTAMP, " +
                    "int_col INT) " +
                    "tags (json_tag json)",
            "CREATE STABLE IF NOT EXISTS stb (" +
                    "ts TIMESTAMP, " +
                    "int_col INT, " +
                    "double_col DOUBLE, " +
                    "bool_col BOOL, " +
                    "binary_col BINARY(100), " +
                    "nchar_col NCHAR(100), " +
                    "varbinary_col VARBINARY(100), " +
                    "geometry_col GEOMETRY(100)) " +
                    "tags (" +
                    "int_tag INT, " +
                    "double_tag DOUBLE, " +
                    "bool_tag BOOL, " +
                    "binary_tag BINARY(100), " +
                    "nchar_tag NCHAR(100), " +
                    "varbinary_tag VARBINARY(100), " +
                    "geometry_tag GEOMETRY(100))"
    };
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {

            init(conn);
            stmtJsonTag(conn);
            stmtAll(conn);

        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed exceptions info
            System.out.println("Failed to insert data using stmt, ErrCode:" + ex.getErrorCode() + "; ErrMessage: " + ex.getMessage());
            throw ex;
        } catch (Exception ex) {
            System.out.println("Failed to insert data using stmt, ErrMessage: " + ex.getMessage());
            throw ex;
        }
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (int i = 0; i < schemaList.length; i++) {
                stmt.execute(schemaList[i]);
            }
        }
    }

    private static void stmtJsonTag(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb_json tags(?) VALUES (?,?)";

        try (TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set table name
                pstmt.setTableName("ntb_json_" + i);
                // set tags
                pstmt.setTagJson(0, "{\"device\":\"device_" + i + "\"}");
                // set columns
                ArrayList<Long> tsList = new ArrayList<>();
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++)
                    tsList.add(current + j);
                pstmt.setTimestamp(0, tsList);

                ArrayList<Integer> f1List = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++)
                    f1List.add(random.nextInt(Integer.MAX_VALUE));
                pstmt.setInt(1, f1List);

                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb_json");
        }
    }

    private static void stmtAll(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb tags(?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);

        for (int i = 1; i <= numOfSubTable; i++) {
            // set table name
            pstmt.setTableName("ntb" + i);
            // set tags
            pstmt.setTagInt(0, i);
            pstmt.setTagDouble(1, 1.1);
            pstmt.setTagBoolean(2, true);
            pstmt.setTagString(3, "binary_value");
            pstmt.setTagNString(4, "nchar_value");
            pstmt.setTagVarbinary(5, new byte[]{(byte) 0x98, (byte) 0xf4, 0x6e});
            pstmt.setTagGeometry(6, new byte[]{
                    0x01, 0x01, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59,
                    0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40});

            // set columns
            ArrayList<Long> tsList = new ArrayList<>();
            long current = System.currentTimeMillis();
            for (int j = 0; j < numOfRow; j++)
                tsList.add(current + j);
            pstmt.setTimestamp(0, tsList);

            ArrayList<Integer> f1List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f1List.add(random.nextInt(Integer.MAX_VALUE));
            pstmt.setInt(1, f1List);

            ArrayList<Double> f2List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f2List.add(random.nextDouble());
            pstmt.setDouble(2, f2List);

            ArrayList<Boolean> f3List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f3List.add(true);
            pstmt.setBoolean(3, f3List);

            ArrayList<String> f4List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f4List.add("binary_value");
            pstmt.setString(4, f4List, BINARY_COLUMN_SIZE);

            ArrayList<String> f5List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f5List.add("nchar_value");
            pstmt.setNString(5, f5List, BINARY_COLUMN_SIZE);

            ArrayList<byte[]> f6List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f6List.add(new byte[]{(byte) 0x98, (byte) 0xf4, 0x6e});
            pstmt.setVarbinary(6, f6List, BINARY_COLUMN_SIZE);

            ArrayList<byte[]> f7List = new ArrayList<>();
            for (int j = 0; j < numOfRow; j++)
                f7List.add(new byte[]{
                        0x01, 0x01, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x59,
                        0x40, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x59, 0x40});
            pstmt.setGeometry(7, f7List, BINARY_COLUMN_SIZE);

            // add column
            pstmt.columnDataAddBatch();
        }
        // execute
        pstmt.columnDataExecuteBatch();
        System.out.println("Successfully inserted rows to example_all_type_stmt.ntb");
        // close if no try-with-catch statement is used
        pstmt.close();
    }
}
// ANCHOR_END: para_bind
