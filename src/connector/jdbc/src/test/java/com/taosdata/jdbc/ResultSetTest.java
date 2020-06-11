package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ResultSetTest {
    static Connection connection = null;
    static Statement statement = null;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";
    static ResultSet resSet = null;

    @BeforeClass
    public static void createDatabaseAndTable() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);

        statement = connection.createStatement();
        statement.executeUpdate("drop database if exists " + dbName);
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName +
                " (ts timestamp, k1 int, k2 bigint, k3 float, k4 double, k5 binary(30), k6 smallint, k7 bool, k8 nchar(20))");

        statement.executeQuery("use " + dbName);
    }

    @Test
    public void testResultSet() {
        String sql = null;
        long ts = 1496732686000l;
        int v1 = 2147483600;
        long v2 = ts + 1000;
        float v3 = 3.1415926f;
        double v4 = 3.1415926535897;
        String v5 = "涛思数据，强~！";
        short v6 = 12;
        boolean v7 = false;
        String v8 = "TDengine is powerful";

        sql = "insert into " + dbName + "." + tName + " values (" + ts + "," + v1 + "," + v2 + "," + v3 + "," + v4
                + ",\"" + v5 + "\"," + v6 + "," + v7 + ",\"" + v8 + "\")";
//        System.out.println(sql);

        try {
            statement.executeUpdate(sql);
            assertEquals(1, statement.getUpdateCount());
        } catch (SQLException e) {
            assert false : "insert error " + e.getMessage();
        }

        try {
            statement.executeQuery("select * from " + dbName + "." + tName);
            resSet = statement.getResultSet();
            System.out.println(((TSDBResultSet) resSet).getRowData());
            while (resSet.next()) {
                assertEquals(ts, resSet.getLong(1));
                assertEquals(ts, resSet.getLong("ts"));

                System.out.println(resSet.getTimestamp(1));

                assertEquals(v1, resSet.getInt(2));
                assertEquals(v1, resSet.getInt("k1"));

                assertEquals(v2, resSet.getLong(3));
                assertEquals(v2, resSet.getLong("k2"));

                assertEquals(v3, resSet.getFloat(4), 7);
                assertEquals(v3, resSet.getFloat("k3"), 7);

                assertEquals(v4, resSet.getDouble(5), 13);
                assertEquals(v4, resSet.getDouble("k4"), 13);

                assertEquals(v5, resSet.getString(6));
                assertEquals(v5, resSet.getString("k5"));

                assertEquals(v6, resSet.getShort(7));
                assertEquals(v6, resSet.getShort("k6"));

                assertEquals(v7, resSet.getBoolean(8));
                assertEquals(v7, resSet.getBoolean("k7"));

                assertEquals(v8, resSet.getString(9));
                assertEquals(v8, resSet.getString("k8"));

                resSet.getBytes(9);
                resSet.getObject(6);
                resSet.getObject("k8");
            }
            resSet.close();
        } catch (SQLException e) {
            assert false : "insert error " + e.getMessage();
        }
    }

    @Test
    public void testBatch() throws SQLException {
        String[] sqls = new String[]{"insert into test.t0 values (1496732686001,2147483600,1496732687000,3.1415925,3.1415926\n" +
                "535897,\"涛思数据，强~！\",12,12,\"TDengine is powerful\")", "insert into test.t0 values (1496732686002,2147483600,1496732687000,3.1415925,3.1415926\n" +
                "535897,\"涛思数据，强~！\",12,12,\"TDengine is powerful\")"};
        for (String sql : sqls) {
            statement.addBatch(sql);
        }
        int[] res = statement.executeBatch();
        assertEquals(res.length, 2);
        statement.clearBatch();
    }

    @AfterClass
    public static void close() throws SQLException {
        statement.executeUpdate("drop database " + dbName);
        statement.close();
        connection.close();
    }

}
