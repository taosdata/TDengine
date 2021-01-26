package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
            statement.executeQuery("select * from " + dbName + "." + tName + " where ts = " + ts);
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

    @Test
    public void testUnsupport() throws SQLException {
        statement.executeQuery("show databases");
        resSet = statement.getResultSet();
        try {
            resSet.unwrap(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.isWrapperFor(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getAsciiStream(0);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getUnicodeStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getBinaryStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getAsciiStream("");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getUnicodeStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getBinaryStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getWarnings();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.clearWarnings();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getCursorName();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getCharacterStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getCharacterStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.isBeforeFirst();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.isAfterLast();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.isFirst();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.isLast();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.beforeFirst();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.afterLast();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.first();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.last();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.absolute(1);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.relative(1);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.previous();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.setFetchDirection(0);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getFetchDirection();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.setFetchSize(0);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getFetchSize();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getConcurrency();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.rowUpdated();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.rowInserted();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.rowDeleted();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNull(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBoolean(0, true);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateByte(0, (byte) 2);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateShort(0, (short) 1);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateInt(0, 0);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateLong(0, 0l);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateFloat(0, 3.14f);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateDouble(0, 3.1415);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBigDecimal(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateString(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBytes(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateDate(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateTime(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateTimestamp(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateObject(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateObject(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNull(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBoolean("", false);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateByte("", (byte) 1);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateShort("", (short) 1);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateInt("", 0);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateLong("", 0l);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateFloat("", 3.14f);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateDouble("", 3.1415);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBigDecimal(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateString(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBytes(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateDate(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateTime(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateTimestamp(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateObject(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateObject(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.insertRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.deleteRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.refreshRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.cancelRowUpdates();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.moveToInsertRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.moveToCurrentRow();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getObject(0, new HashMap<>());
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getRef(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getBlob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getClob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getArray(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getObject("", new HashMap<>());
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getRef(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getBlob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getClob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getArray(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getDate(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getDate(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getTime(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getTime(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getTimestamp(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getTimestamp(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getURL(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getURL(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateRef(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateRef(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBlob(0, new SerialBlob("".getBytes("UTF8")));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBlob("", new SerialBlob("".getBytes("UTF8")));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateClob("", new SerialClob("".toCharArray()));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateClob(0, new SerialClob("".toCharArray()));
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateArray(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateArray(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getRowId(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getRowId(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateRowId(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateRowId(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getHoldability();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNString(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNString(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }

        try {
            resSet.getNClob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getNClob(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getSQLXML(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getSQLXML(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateSQLXML(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateSQLXML(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getNCharacterStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.getNCharacterStream(null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }

        try {
            resSet.updateNCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateNCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateAsciiStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateBinaryStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
        try {
            resSet.updateCharacterStream(null, null);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("this operation is NOT supported currently!"));
        }
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
