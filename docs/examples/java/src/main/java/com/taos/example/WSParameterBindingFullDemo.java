package com.taos.example;

import com.taosdata.jdbc.ws.TSWSPreparedStatement;

import java.math.BigInteger;
import java.sql.*;
import java.util.Random;

// ANCHOR: para_bind
public class WSParameterBindingFullDemo {
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int BINARY_COLUMN_SIZE = 30;
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
                    "geometry_col GEOMETRY(100)," +
                    "utinyint_col tinyint unsigned," +
                    "usmallint_col smallint unsigned," +
                    "uint_col int unsigned," +
                    "ubigint_col bigint unsigned" +
                    ") " +
                    "tags (" +
                    "int_tag INT, " +
                    "double_tag DOUBLE, " +
                    "bool_tag BOOL, " +
                    "binary_tag BINARY(100), " +
                    "nchar_tag NCHAR(100), " +
                    "varbinary_tag VARBINARY(100), " +
                    "geometry_tag GEOMETRY(100)," +
                    "utinyint_tag tinyint unsigned," +
                    "usmallint_tag smallint unsigned," +
                    "uint_tag int unsigned," +
                    "ubigint_tag bigint unsigned" +
                    ")"
    };
    private static final int numOfSubTable = 10, numOfRow = 10;

    public static void main(String[] args) throws SQLException {

        String jdbcUrl = "jdbc:TAOS-WS://" + host + ":6041/";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata")) {

            init(conn);

            stmtJsonTag(conn);

            stmtAll(conn);

        } catch (SQLException ex) {
            // handle any errors, please refer to the JDBC specifications for detailed
            // exceptions info
            System.out.println("Failed to insert data using stmt, ErrCode:" + ex.getErrorCode() + "; ErrMessage: "
                    + ex.getMessage());
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

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set table name
                pstmt.setTableName("ntb_json_" + i);
                // set tags
                pstmt.setTagJson(0, "{\"device\":\"device_" + i + "\"}");
                // set columns
                long current = System.currentTimeMillis();
                for (int j = 0; j < numOfRow; j++) {
                    pstmt.setTimestamp(1, new Timestamp(current + j));
                    pstmt.setInt(2, j);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb_json");
        }
    }

    private static void stmtAll(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb tags(?,?,?,?,?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            // set table name
            pstmt.setTableName("ntb");
            // set tags
            pstmt.setTagInt(0, 1);
            pstmt.setTagDouble(1, 1.1);
            pstmt.setTagBoolean(2, true);
            pstmt.setTagString(3, "binary_value");
            pstmt.setTagNString(4, "nchar_value");
            pstmt.setTagVarbinary(5, new byte[] { (byte) 0x98, (byte) 0xf4, 0x6e });
            pstmt.setTagGeometry(6, new byte[] {
                    0x01, 0x01, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59,
                    0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40 });
            pstmt.setTagShort(7, (short)255);
            pstmt.setTagInt(8, 65535);
            pstmt.setTagLong(9, 4294967295L);
            pstmt.setTagBigInteger(10, new BigInteger("18446744073709551615"));

            long current = System.currentTimeMillis();

            pstmt.setTimestamp(1, new Timestamp(current));
            pstmt.setInt(2, 1);
            pstmt.setDouble(3, 1.1);
            pstmt.setBoolean(4, true);
            pstmt.setString(5, "binary_value");
            pstmt.setNString(6, "nchar_value");
            pstmt.setVarbinary(7, new byte[] { (byte) 0x98, (byte) 0xf4, 0x6e });
            pstmt.setGeometry(8, new byte[] {
                    0x01, 0x01, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59,
                    0x40, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x59, 0x40 });
            pstmt.setShort(9, (short)255);
            pstmt.setInt(10, 65535);
            pstmt.setLong(11, 4294967295L);
            pstmt.setObject(12, new BigInteger("18446744073709551615"));
            pstmt.addBatch();
            pstmt.executeBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb");
        }
    }
}
// ANCHOR_END: para_bind
