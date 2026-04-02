package com.taosdata.jdbc.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class FieldBindTypeTest {

    @Test
    public void testGetValue() {
        assertEquals(1, FieldBindType.TAOS_FIELD_COL.getValue());
        assertEquals(2, FieldBindType.TAOS_FIELD_TAG.getValue());
        assertEquals(3, FieldBindType.TAOS_FIELD_QUERY.getValue());
        assertEquals(4, FieldBindType.TAOS_FIELD_TBNAME.getValue());
    }

    @Test
    public void testFromValue() {
        assertEquals(FieldBindType.TAOS_FIELD_COL, FieldBindType.fromValue(1));
        assertEquals(FieldBindType.TAOS_FIELD_TAG, FieldBindType.fromValue(2));
        assertEquals(FieldBindType.TAOS_FIELD_QUERY, FieldBindType.fromValue(3));
        assertEquals(FieldBindType.TAOS_FIELD_TBNAME, FieldBindType.fromValue(4));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromValue_InvalidValue() {
        FieldBindType.fromValue(99);
    }
}