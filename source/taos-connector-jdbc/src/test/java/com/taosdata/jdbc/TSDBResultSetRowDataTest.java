package com.taosdata.jdbc;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Timestamp;

import static org.junit.Assert.*;

public class TSDBResultSetRowDataTest {
    private TSDBResultSetRowData rowData;

    @Before
    public void setUp() {
        rowData = new TSDBResultSetRowData(3); // 假设列数为 3
    }

    @Test
    public void testClear() {
        rowData.clear();
    }

    @Test
    public void testWasNull() {
        rowData.clear();
        assertTrue(rowData.wasNull(1));
        rowData.setIntValue(1, 10);
        assertFalse(rowData.wasNull(1));
    }

    @Test
    public void testSetBooleanValue()  throws SQLException {
        rowData.setBooleanValue(1, true);
        assertTrue(rowData.getBoolean(1, 1));
    }

    @Test
    public void testSetIntValue() throws SQLException  {
        rowData.setIntValue(1, 10);
        assertEquals(10, rowData.getInt(1, 4));
    }

    @Test
    public void testSetLongValue()  throws SQLException {
        rowData.setLongValue(1, 100L);
        assertEquals(100L, rowData.getLong(1, 5));
    }

    @Test
    public void testSetFloatValue() throws SQLException {
        rowData.setFloatValue(1, 1.0f);
        assertEquals(1.0f, rowData.getFloat(1, 0), 0.001);
    }

    @Test
    public void testSetDoubleValue() throws SQLException  {
        rowData.setDoubleValue(1, 1.0);
        assertEquals(1.0, rowData.getDouble(1, 0), 0.001);
    }

    @Test
    public void testSetStringValue()  throws SQLException {
        rowData.setStringValue(1, "test");
        assertEquals("test", rowData.getString(1, 0));
    }

    @Test
    public void testSetTimestampValue() {
        long currentTime = System.currentTimeMillis();
        rowData.setTimestampValue(1, currentTime);
        Timestamp timestamp = rowData.getTimestamp(1, 0);
        assertEquals(currentTime, timestamp.getTime());

        rowData.setTimestamp(0, currentTime * 1000, 1);
        timestamp = rowData.getTimestamp(1, 0);
        assertEquals(currentTime, timestamp.getTime());

        rowData.setTimestamp(0, currentTime * 1000 * 1000, 2);
        timestamp = rowData.getTimestamp(1, 0);
        assertEquals(currentTime, timestamp.getTime());

        rowData.setLongValue(1, currentTime);
        timestamp = rowData.getTimestamp(1, 5);
        assertEquals(currentTime, timestamp.getTime());
    }

    @Test
    public void testGetObject() {
        rowData.setStringValue(1, "test");
        assertEquals("test", rowData.getObject(1));
    }

    @Test
    public void testGetStringWithNull() throws SQLException {
        rowData.clear();
        Assert.assertNull(rowData.getString(1, 0));
    }
}