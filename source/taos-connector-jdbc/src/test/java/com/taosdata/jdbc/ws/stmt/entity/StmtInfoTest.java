package com.taosdata.jdbc.ws.stmt.entity;

import com.taosdata.jdbc.ws.stmt2.entity.Field;
import com.taosdata.jdbc.ws.stmt2.entity.StmtInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class StmtInfoTest {

    private StmtInfo stmtInfo;
    private final long stmtId = 2L;
    private final int toBeBindTableNameIndex = 3;
    private final int toBeBindTagCount = 4;
    private final int toBeBindColCount = 5;
    private final int precision = 6;
    private final List<Field> fields = Arrays.asList(new Field(), new Field());
    private final String sql = "SELECT * FROM test";

    @Before
    public void setUp() {
        stmtInfo = new StmtInfo(
                sql
        );

        stmtInfo.setStmtId(stmtId);
        stmtInfo.setToBeBindTableNameIndex(toBeBindTableNameIndex);
        stmtInfo.setToBeBindTagCount(toBeBindTagCount);
        stmtInfo.setToBeBindColCount(toBeBindColCount);
        stmtInfo.setPrecision(precision);
        stmtInfo.setFields(fields);
    }

    @Test
    public void testConstructorAndGetters() {
        assertEquals(stmtId, stmtInfo.getStmtId());
        assertEquals(toBeBindTableNameIndex, stmtInfo.getToBeBindTableNameIndex());
        assertEquals(toBeBindTagCount, stmtInfo.getToBeBindTagCount());
        assertEquals(toBeBindColCount, stmtInfo.getToBeBindColCount());
        assertEquals(precision, stmtInfo.getPrecision());
        assertEquals(fields, stmtInfo.getFields());
        assertEquals(sql, stmtInfo.getSql());
    }
    @Test
    public void testSetStmtId() {
        long newStmtId = 20L;
        stmtInfo.setStmtId(newStmtId);
        assertEquals(newStmtId, stmtInfo.getStmtId());
    }

    @Test
    public void testSetToBeBindTableNameIndex() {
        int newIndex = 30;
        stmtInfo.setToBeBindTableNameIndex(newIndex);
        assertEquals(newIndex, stmtInfo.getToBeBindTableNameIndex());
    }

    @Test
    public void testSetToBeBindTagCount() {
        int newCount = 40;
        stmtInfo.setToBeBindTagCount(newCount);
        assertEquals(newCount, stmtInfo.getToBeBindTagCount());
    }

    @Test
    public void testSetToBeBindColCount() {
        int newCount = 50;
        stmtInfo.setToBeBindColCount(newCount);
        assertEquals(newCount, stmtInfo.getToBeBindColCount());
    }

    @Test
    public void testSetPrecision() {
        int newPrecision = 60;
        stmtInfo.setPrecision(newPrecision);
        assertEquals(newPrecision, stmtInfo.getPrecision());
    }

    @Test
    public void testGetFields() {
        List<Field> originalFields = stmtInfo.getFields();
        assertNotNull(originalFields);
        assertEquals(2, originalFields.size());
    }

    @Test
    public void testGetSql() {
        assertEquals(sql, stmtInfo.getSql());
    }

    @Test
    public void testDefaultStmtIdValue() {
        StmtInfo newStmtInfo = new StmtInfo(
                sql
        );
        assertEquals(0, newStmtInfo.getStmtId());
    }

    @Test
    public void testFieldListIsSameInstance() {
        assertSame(fields, stmtInfo.getFields());
    }

    @Test
    public void testNegativeValues() {
        stmtInfo.setStmtId(-2L);
        stmtInfo.setToBeBindTableNameIndex(-3);
        stmtInfo.setToBeBindTagCount(-4);
        stmtInfo.setToBeBindColCount(-5);
        stmtInfo.setPrecision(-6);

        assertEquals(-2L, stmtInfo.getStmtId());
        assertEquals(-3, stmtInfo.getToBeBindTableNameIndex());
        assertEquals(-4, stmtInfo.getToBeBindTagCount());
        assertEquals(-5, stmtInfo.getToBeBindColCount());
        assertEquals(-6, stmtInfo.getPrecision());
    }

    @Test
    public void testZeroValues() {
        stmtInfo.setStmtId(0L);
        stmtInfo.setToBeBindTableNameIndex(0);
        stmtInfo.setToBeBindTagCount(0);
        stmtInfo.setToBeBindColCount(0);
        stmtInfo.setPrecision(0);

        assertEquals(0L, stmtInfo.getStmtId());
        assertEquals(0, stmtInfo.getToBeBindTableNameIndex());
        assertEquals(0, stmtInfo.getToBeBindTagCount());
        assertEquals(0, stmtInfo.getToBeBindColCount());
        assertEquals(0, stmtInfo.getPrecision());
    }
}