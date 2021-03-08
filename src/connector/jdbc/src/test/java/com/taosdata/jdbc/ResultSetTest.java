package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class ResultSetTest {
    static Connection connection;
    static Statement statement;
    static String dbName = "test";
    static String tName = "t0";
    static String host = "localhost";
    static ResultSet resSet;

    @BeforeClass
    public static void createDatabaseAndTable() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);
            statement = connection.createStatement();
            statement.executeUpdate("drop database if exists " + dbName);
            statement.executeUpdate("create database if not exists " + dbName);
            statement.execute("use " + dbName);
            statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k1 int, k2 bigint, k3 float, k4 double, k5 binary(30), k6 smallint, k7 bool, k8 nchar(20))");
        } catch (ClassNotFoundException | SQLException e) {
            return;
        }
    }

    @Test
    public void testResultSet() {
        String sql;
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
        try {
            statement.executeUpdate(sql);
            assertEquals(1, statement.getUpdateCount());
        } catch (SQLException e) {
            assert false : "insert error " + e.getMessage();
        }
        try {
            statement.execute("select * from " + dbName + "." + tName + " where ts = " + ts);
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
            if (!resSet.isClosed()) {
                resSet.close();
            }
        } catch (SQLException e) {
            assert false : "insert error " + e.getMessage();
        }
    }

    @Test(expected = SQLException.class)
    public void testUnsupport() throws SQLException, UnsupportedEncodingException {
        statement.execute("show databases");
        resSet = statement.getResultSet();
        Assert.assertNotNull(resSet.unwrap(TSDBResultSet.class));
        Assert.assertTrue(resSet.isWrapperFor(TSDBResultSet.class));
        resSet.getUnicodeStream(null);
        resSet.getBinaryStream(null);
        resSet.getAsciiStream("");
        resSet.getUnicodeStream(null);
        resSet.getBinaryStream(null);
        resSet.getWarnings();
        resSet.clearWarnings();
        resSet.getCursorName();
        resSet.getCharacterStream(null);
        resSet.getCharacterStream(null);
        resSet.isBeforeFirst();
        resSet.isAfterLast();
        resSet.isFirst();
        resSet.isLast();
        resSet.beforeFirst();
        resSet.afterLast();
        resSet.first();
        resSet.last();
        resSet.getRow();
        resSet.absolute(1);
        resSet.relative(1);
        resSet.previous();
        resSet.setFetchDirection(0);
        resSet.getFetchDirection();
        resSet.setFetchSize(0);
        resSet.getFetchSize();
        resSet.getConcurrency();
        resSet.rowUpdated();
        resSet.rowInserted();
        resSet.rowDeleted();
        resSet.updateNull(null);
        resSet.updateBoolean(0, true);
        resSet.updateByte(0, (byte) 2);
        resSet.updateShort(0, (short) 1);
        resSet.updateInt(0, 0);
        resSet.updateLong(0, 0l);
        resSet.updateFloat(0, 3.14f);
        resSet.updateDouble(0, 3.1415);
        resSet.updateBigDecimal(null, null);
        resSet.updateString(null, null);
        resSet.updateBytes(null, null);
        resSet.updateDate(null, null);
        resSet.updateTime(null, null);
        resSet.updateTimestamp(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
        resSet.updateObject(null, null);
        resSet.updateObject(null, null);
        resSet.updateNull(null);
        resSet.updateBoolean("", false);
        resSet.updateByte("", (byte) 1);
        resSet.updateShort("", (short) 1);
        resSet.updateInt("", 0);
        resSet.updateLong("", 0l);
        resSet.updateFloat("", 3.14f);
        resSet.updateDouble("", 3.1415);
        resSet.updateBigDecimal(null, null);
        resSet.updateString(null, null);
        resSet.updateBytes(null, null);
        resSet.updateDate(null, null);
        resSet.updateTime(null, null);
        resSet.updateTimestamp(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
        resSet.updateObject(null, null);
        resSet.updateObject(null, null);
        resSet.insertRow();
        resSet.updateRow();
        resSet.deleteRow();
        resSet.refreshRow();
        resSet.cancelRowUpdates();
        resSet.moveToInsertRow();
        resSet.moveToCurrentRow();
        resSet.getStatement();
        resSet.getObject(0, new HashMap<>());
        resSet.getRef(null);
        resSet.getBlob(null);
        resSet.getClob(null);
        resSet.getArray(null);
        resSet.getObject("", new HashMap<>());
        resSet.getRef(null);
        resSet.getBlob(null);
        resSet.getClob(null);
        resSet.getArray(null);
        resSet.getDate(null, null);
        resSet.getDate(null, null);
        resSet.getTime(null, null);
        resSet.getTime(null, null);
        resSet.getTimestamp(null, null);
        resSet.getTimestamp(null, null);
        resSet.getURL(null);
        resSet.getURL(null);
        resSet.updateRef(null, null);
        resSet.updateRef(null, null);
        resSet.updateBlob(0, new SerialBlob("".getBytes("UTF8")));
        resSet.updateBlob("", new SerialBlob("".getBytes("UTF8")));
        resSet.updateClob("", new SerialClob("".toCharArray()));
        resSet.updateClob(0, new SerialClob("".toCharArray()));
        resSet.updateArray(null, null);
        resSet.updateArray(null, null);
        resSet.getRowId(null);
        resSet.getRowId(null);
        resSet.updateRowId(null, null);
        resSet.updateRowId(null, null);
        resSet.getHoldability();
        resSet.updateNString(null, null);
        resSet.updateNString(null, null);
        resSet.getNClob(null);
        resSet.getNClob(null);
        resSet.getSQLXML(null);
        resSet.getSQLXML(null);
        resSet.updateSQLXML(null, null);
        resSet.updateSQLXML(null, null);
        resSet.getNCharacterStream(null);
        resSet.getNCharacterStream(null);
        resSet.updateNCharacterStream(null, null);
        resSet.updateNCharacterStream(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
        resSet.updateNCharacterStream(null, null);
        resSet.updateNCharacterStream(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
        resSet.updateAsciiStream(null, null);
        resSet.updateBinaryStream(null, null);
        resSet.updateCharacterStream(null, null);
    }

    @Test
    public void testBatch() throws SQLException {
        String[] sqls = new String[]{"insert into test.t0 values (1496732686001,2147483600,1496732687000,3.1415925,3.1415926535897," +
                "'涛思数据，强~',12,0,'TDengine is powerful')", "insert into test.t0 values (1496732686002,2147483600,1496732687000,3.1415925,3.1415926535897," +
                "'涛思数据，强~',12,1,'TDengine is powerful')"};
        for (String sql : sqls) {
            statement.addBatch(sql);
        }
        int[] res = statement.executeBatch();
        assertEquals(res.length, 2);
        statement.clearBatch();
    }

    @AfterClass
    public static void close() {
        try {
            statement.executeUpdate("drop database " + dbName);
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
