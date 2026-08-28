package com.taos.example;

import com.taosdata.jdbc.ws.TSWSPreparedStatement;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import com.taosdata.jdbc.common.TDBlob;

/**
 * Full-type parameter binding demo using the TDengine extension interface in
 * column-data mode: list setters paired with columnDataAddBatch/columnDataExecuteBatch,
 * never mixed with the standard single-value setters.
 *
 * @deprecated The extension interface is deprecated; use the standard JDBC parameter
 * binding interface instead (see WSParameterBindingStdInterfaceDemo).
 */
@Deprecated
public class WSParameterBindingExtendFullDemo {
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int BINARY_COLUMN_SIZE = 30;
    private static final byte[] GEOMETRY_BYTES = new byte[] {
            0x01, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x59,
            0x40, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x59, 0x40 };
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
                    "ubigint_col bigint unsigned," +
                    "blob_col BLOB," +
                    "decimal64_col decimal(4,2)," +
                    "decimal128_col decimal(30,10)" +
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

        String jdbcUrl = "jdbc:TAOS-WS://" + host + ":6041/?varcharAsString=true";

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
                pstmt.setTableName("ntb_ext_json_" + i);
                // set tags
                pstmt.setTagJson(0, "{\"device\":\"device_" + i + "\"}");
                // set columns as lists
                long current = System.currentTimeMillis();
                ArrayList<Long> tsList = new ArrayList<>();
                ArrayList<Integer> intList = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++) {
                    tsList.add(current + j);
                    intList.add(random.nextInt(Integer.MAX_VALUE));
                }
                pstmt.setTimestamp(0, tsList);
                pstmt.setInt(1, intList);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb_ext_json");
        }
    }

    private static void stmtAll(Connection conn) throws SQLException {
        String sql = "INSERT INTO ? using stb tags(?,?,?,?,?,?,?,?,?,?,?) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (TSWSPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSWSPreparedStatement.class)) {

            for (int i = 1; i <= numOfSubTable; i++) {
                // set table name
                pstmt.setTableName("ntb_ext_" + i);
                // set tags (11 tags)
                pstmt.setTagInt(0, i);
                pstmt.setTagDouble(1, 1.1);
                pstmt.setTagBoolean(2, true);
                pstmt.setTagString(3, "binary_value");
                pstmt.setTagNString(4, "nchar_value");
                pstmt.setTagVarbinary(5, new byte[] { (byte) 0x98, (byte) 0xf4, 0x6e });
                pstmt.setTagGeometry(6, GEOMETRY_BYTES);
                pstmt.setTagShort(7, (short) 255);
                pstmt.setTagInt(8, 65535);
                pstmt.setTagLong(9, 4294967295L);
                pstmt.setTagBigInteger(10, new BigInteger("18446744073709551615"));

                long current = System.currentTimeMillis();

                // set columns as lists (15 columns)
                ArrayList<Long> tsList = new ArrayList<>();
                ArrayList<Integer> intList = new ArrayList<>();
                ArrayList<Double> doubleList = new ArrayList<>();
                ArrayList<Boolean> boolList = new ArrayList<>();
                ArrayList<String> binaryList = new ArrayList<>();
                ArrayList<String> ncharList = new ArrayList<>();
                ArrayList<byte[]> varbinaryList = new ArrayList<>();
                ArrayList<byte[]> geometryList = new ArrayList<>();
                ArrayList<Short> utinyintList = new ArrayList<>();
                ArrayList<Integer> usmallintList = new ArrayList<>();
                ArrayList<Long> uintList = new ArrayList<>();
                ArrayList<BigInteger> ubigintList = new ArrayList<>();
                ArrayList<Blob> blobList = new ArrayList<>();
                ArrayList<BigDecimal> decimal64List = new ArrayList<>();
                ArrayList<BigDecimal> decimal128List = new ArrayList<>();
                for (int j = 0; j < numOfRow; j++) {
                    tsList.add(current + j);
                    intList.add(j);
                    doubleList.add(1.1 + j);
                    boolList.add(j % 2 == 0);
                    binaryList.add("binary_value_" + j);
                    ncharList.add("nchar_value_" + j);
                    varbinaryList.add(new byte[] { (byte) 0x98, (byte) 0xf4, (byte) j });
                    geometryList.add(GEOMETRY_BYTES);
                    utinyintList.add((short) 255);
                    usmallintList.add(65535);
                    uintList.add(4294967295L);
                    ubigintList.add(new BigInteger("18446744073709551615"));
                    blobList.add(new TDBlob(("blob data " + j).getBytes(), true));
                    decimal64List.add(new BigDecimal("12.34"));
                    decimal128List.add(new BigDecimal("12345678901234567890.123456789"));
                }
                pstmt.setTimestamp(0, tsList);
                pstmt.setInt(1, intList);
                pstmt.setDouble(2, doubleList);
                pstmt.setBoolean(3, boolList);
                pstmt.setString(4, binaryList, BINARY_COLUMN_SIZE);
                pstmt.setNString(5, ncharList, BINARY_COLUMN_SIZE);
                pstmt.setVarbinary(6, varbinaryList, BINARY_COLUMN_SIZE);
                pstmt.setGeometry(7, geometryList, BINARY_COLUMN_SIZE);
                pstmt.setShort(8, utinyintList);
                pstmt.setInt(9, usmallintList);
                pstmt.setLong(10, uintList);
                pstmt.setBigInteger(11, ubigintList);
                pstmt.setBlob(12, blobList, BINARY_COLUMN_SIZE);
                pstmt.setBigDecimal(13, decimal64List);
                pstmt.setBigDecimal(14, decimal128List);
                // add column
                pstmt.columnDataAddBatch();
            }
            // execute column
            pstmt.columnDataExecuteBatch();
            System.out.println("Successfully inserted rows to example_all_type_stmt.ntb_ext");
        }
    }
}
