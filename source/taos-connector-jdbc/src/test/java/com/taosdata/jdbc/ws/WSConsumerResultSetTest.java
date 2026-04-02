package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.ws.tmq.WSConsumerResultSet;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;
public class WSConsumerResultSetTest {

    private WSConsumerResultSet wsConsumerResultSet;

    @Before
    public void setUp() {
        wsConsumerResultSet = new WSConsumerResultSet(null, null, 0, null, null);
    }

    @Test(expected = SQLException.class)
    public void testIsBeforeFirst() throws SQLException {
        wsConsumerResultSet.isBeforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsAfterLast() throws SQLException {
        wsConsumerResultSet.isAfterLast();
    }

    @Test(expected = SQLException.class)
    public void testIsFirst() throws SQLException {
        wsConsumerResultSet.isFirst();
    }

    @Test(expected = SQLException.class)
    public void testIsLast() throws SQLException {
        wsConsumerResultSet.isLast();
    }

    @Test(expected = SQLException.class)
    public void testBeforeFirst() throws SQLException {
        wsConsumerResultSet.beforeFirst();
    }

    @Test(expected = SQLException.class)
    public void testafterFirst() throws SQLException {
        wsConsumerResultSet.afterLast();
    }

    @Test(expected = SQLException.class)
    public void testFirst() throws SQLException {
        wsConsumerResultSet.first();
    }

    @Test(expected = SQLException.class)
    public void testLast() throws SQLException {
        wsConsumerResultSet.last();
    }

    @Test(expected = SQLException.class)
    public void testGetRow() throws SQLException {
        assertEquals(0, wsConsumerResultSet.getRow());
    }

    @Test(expected = SQLException.class)
    public void testAbsolute() throws SQLException {
        wsConsumerResultSet.absolute(0);
    }

    @Test(expected = SQLException.class)
    public void testRelative() throws SQLException {
        wsConsumerResultSet.relative(0);
    }

    @Test(expected = SQLException.class)
    public void testPrevious() throws SQLException {
        wsConsumerResultSet.previous();
    }
    @Test(expected = SQLException.class)
    public void testGetStatement() throws SQLException {
        wsConsumerResultSet.getStatement();
    }

}

