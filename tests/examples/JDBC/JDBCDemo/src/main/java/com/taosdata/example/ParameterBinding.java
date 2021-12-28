package com.taosdata.example;

import com.taosdata.jdbc.TSDBPreparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

public class ParameterBinding {
    private static final String host = "127.0.0.1";
    private static final Random random = new Random(System.currentTimeMillis());
    private static final int BINARY_COLUMN_SIZE = 20;
    private static final String[] schemaList = {
            "create table stable1(ts timestamp, f1 tinyint, f2 smallint, f3 int, f4 bigint) tags(t1 tinyint, t2 smallint, t3 int, t4 bigint)",
            "create table stable2(ts timestamp, f1 float, f2 double) tags(t1 float, t2 double)",
            "create table stable3(ts timestamp, f1 bool) tags(t1 bool)",
            "create table stable4(ts timestamp, f1 binary(" + BINARY_COLUMN_SIZE + ")) tags(t1 binary(" + BINARY_COLUMN_SIZE + "))",
            "create table stable5(ts timestamp, f1 nchar(" + BINARY_COLUMN_SIZE + ")) tags(t1 nchar(" + BINARY_COLUMN_SIZE + "))"
    };
    private static final int numOfTable = 10, numOfRow = 32767;

    public static void main(String[] args) throws SQLException {
        String jdbcUrl = "jdbc:TAOS://" + host + ":6030/";
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "taosdata");

        init(conn);

        for (int i = 0; i < numOfTable; i++)
            bindInteger(conn, "t1_" + (i + 1));

        for (int i = 0; i < numOfTable; i++)
            bindFloat(conn, "tb2_" + (i + 1));


        for (int i = 0; i < numOfTable; i++)
            bindBoolean(conn, "tb3_" + (i + 1));

        for (int i = 0; i < numOfTable; i++)
            bindBytes(conn, "tb4_" + (i + 1));

        for (int i = 0; i < numOfTable; i++)
            bindString(conn, "tb5_" + (i + 1));

        conn.close();
    }

    private static void init(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists test_parabind");
            stmt.execute("create database if not exists test_parabind");
            stmt.execute("use test_parabind");
            for (int i = 0; i < schemaList.length; i++) {
                stmt.execute(schemaList[i]);
            }
        }
    }

    private static void bindInteger(Connection conn, String tbname) throws SQLException {
        String sql = "insert into ? using stable1 tags(?,?,?,?) values(?,?,?,?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);
        // set table name
        pstmt.setTableName(tbname);
        // set tags
        pstmt.setTagByte(0, Byte.parseByte(Integer.toString(random.nextInt(Byte.MAX_VALUE))));
        pstmt.setTagShort(1, Short.parseShort(Integer.toString(random.nextInt(Short.MAX_VALUE))));
        pstmt.setTagInt(2, random.nextInt(Integer.MAX_VALUE));
        pstmt.setTagLong(3, random.nextLong());
        // set columns
        ArrayList<Long> tsList = new ArrayList<>();
        long current = System.currentTimeMillis();
        for (int j = 0; j < numOfRow; j++)
            tsList.add(current + j);
        pstmt.setTimestamp(0, tsList);

        ArrayList<Byte> f1List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f1List.add(Byte.parseByte(Integer.toString(random.nextInt(Byte.MAX_VALUE))));
        pstmt.setByte(1, f1List);

        ArrayList<Short> f2List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f2List.add(Short.parseShort(Integer.toString(random.nextInt(Short.MAX_VALUE))));
        pstmt.setShort(2, f2List);

        ArrayList<Integer> f3List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f3List.add(random.nextInt(Integer.MAX_VALUE));
        pstmt.setInt(3, f3List);

        ArrayList<Long> f4List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f4List.add(random.nextLong());
        pstmt.setLong(4, f4List);

        pstmt.columnDataAddBatch();
        pstmt.columnDataExecuteBatch();
        pstmt.columnDataClearBatch();
        pstmt.columnDataCloseBatch();
    }

    private static void bindFloat(Connection conn, String tbname) throws SQLException {
        String sql = "insert into ? using stable2 tags(?,?) values(?,?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);
        // set table name
        pstmt.setTableName(tbname);
        // set tags
        pstmt.setTagFloat(0, random.nextFloat());
        pstmt.setTagDouble(1, random.nextDouble());
        // set columns
        ArrayList<Long> tsList = new ArrayList<>();
        long current = System.currentTimeMillis();
        for (int j = 0; j < numOfRow; j++)
            tsList.add(current + j);
        pstmt.setTimestamp(0, tsList);

        ArrayList<Float> f1List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f1List.add(random.nextFloat());
        pstmt.setFloat(1, f1List);

        ArrayList<Double> f2List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f2List.add(random.nextDouble());
        pstmt.setDouble(2, f2List);

        pstmt.columnDataAddBatch();
        pstmt.columnDataExecuteBatch();
        pstmt.columnDataClearBatch();
        pstmt.columnDataCloseBatch();
    }

    private static void bindBoolean(Connection conn, String tbname) throws SQLException {
        String sql = "insert into ? using stable3 tags(?) values(?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);
        // set table name
        pstmt.setTableName(tbname);
        // set tags
        pstmt.setTagBoolean(0, random.nextBoolean());
        // set columns
        ArrayList<Long> tsList = new ArrayList<>();
        long current = System.currentTimeMillis();
        for (int j = 0; j < numOfRow; j++)
            tsList.add(current + j);
        pstmt.setTimestamp(0, tsList);

        ArrayList<Boolean> f1List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++)
            f1List.add(random.nextBoolean());
        pstmt.setBoolean(1, f1List);

        pstmt.columnDataAddBatch();
        pstmt.columnDataExecuteBatch();
        pstmt.columnDataClearBatch();
        pstmt.columnDataCloseBatch();
    }

    private static void bindBytes(Connection conn, String tbname) throws SQLException {
        String sql = "insert into ? using stable4 tags(?) values(?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);
        // set table name
        pstmt.setTableName(tbname);
        // set tags
        pstmt.setTagString(0, new String("abc"));

        // set columns
        ArrayList<Long> tsList = new ArrayList<>();
        long current = System.currentTimeMillis();
        for (int j = 0; j < numOfRow; j++)
            tsList.add(current + j);
        pstmt.setTimestamp(0, tsList);

        ArrayList<String> f1List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++) {
            f1List.add(new String("abc"));
        }
        pstmt.setString(1, f1List, BINARY_COLUMN_SIZE);

        pstmt.columnDataAddBatch();
        pstmt.columnDataExecuteBatch();
        pstmt.columnDataClearBatch();
        pstmt.columnDataCloseBatch();
    }

    private static void bindString(Connection conn, String tbname) throws SQLException {
        String sql = "insert into ? using stable5 tags(?) values(?,?)";

        TSDBPreparedStatement pstmt = conn.prepareStatement(sql).unwrap(TSDBPreparedStatement.class);
        // set table name
        pstmt.setTableName(tbname);
        // set tags
        pstmt.setTagNString(0, "北京-abc");

        // set columns
        ArrayList<Long> tsList = new ArrayList<>();
        long current = System.currentTimeMillis();
        for (int j = 0; j < numOfRow; j++)
            tsList.add(current + j);
        pstmt.setTimestamp(0, tsList);

        ArrayList<String> f1List = new ArrayList<>();
        for (int j = 0; j < numOfRow; j++) {
            f1List.add("北京-abc");
        }
        pstmt.setNString(1, f1List, BINARY_COLUMN_SIZE);

        pstmt.columnDataAddBatch();
        pstmt.columnDataExecuteBatch();
        pstmt.columnDataClearBatch();
        pstmt.columnDataCloseBatch();
    }

}