package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class AlterTypeTest {

    @Test
    public void testEnumValues() {
        assertEquals(10, AlterType.values().length);

        assertEquals(AlterType.ADD_TAG, AlterType.valueOf("ADD_TAG"));
        assertEquals(AlterType.DROP_TAG, AlterType.valueOf("DROP_TAG"));
        assertEquals(AlterType.RENAME_TAG_NAME, AlterType.valueOf("RENAME_TAG_NAME"));
        assertEquals(AlterType.SET_TAG, AlterType.valueOf("SET_TAG"));
        assertEquals(AlterType.ADD_COLUMN, AlterType.valueOf("ADD_COLUMN"));
        assertEquals(AlterType.DROP_COLUMN, AlterType.valueOf("DROP_COLUMN"));
        assertEquals(AlterType.MODIFY_COLUMN_LENGTH, AlterType.valueOf("MODIFY_COLUMN_LENGTH"));
        assertEquals(AlterType.MODIFY_TAG_LENGTH, AlterType.valueOf("MODIFY_TAG_LENGTH"));
        assertEquals(AlterType.MODIFY_TABLE_OPTION, AlterType.valueOf("MODIFY_TABLE_OPTION"));
        assertEquals(AlterType.RENAME_COLUMN_NAME, AlterType.valueOf("RENAME_COLUMN_NAME"));
    }

    @Test
    public void testGetValue() {
        assertEquals(1, AlterType.ADD_TAG.getValue());
        assertEquals(2, AlterType.DROP_TAG.getValue());
        assertEquals(3, AlterType.RENAME_TAG_NAME.getValue());
        assertEquals(4, AlterType.SET_TAG.getValue());
        assertEquals(5, AlterType.ADD_COLUMN.getValue());
        assertEquals(6, AlterType.DROP_COLUMN.getValue());
        assertEquals(7, AlterType.MODIFY_COLUMN_LENGTH.getValue());
        assertEquals(8, AlterType.MODIFY_TAG_LENGTH.getValue());
        assertEquals(9, AlterType.MODIFY_TABLE_OPTION.getValue());
        assertEquals(10, AlterType.RENAME_COLUMN_NAME.getValue());
    }

    @Test
    public void testOrdinal() {
        assertEquals(0, AlterType.ADD_TAG.ordinal());
        assertEquals(1, AlterType.DROP_TAG.ordinal());
        assertEquals(2, AlterType.RENAME_TAG_NAME.ordinal());
        assertEquals(3, AlterType.SET_TAG.ordinal());
        assertEquals(4, AlterType.ADD_COLUMN.ordinal());
        assertEquals(5, AlterType.DROP_COLUMN.ordinal());
        assertEquals(6, AlterType.MODIFY_COLUMN_LENGTH.ordinal());
        assertEquals(7, AlterType.MODIFY_TAG_LENGTH.ordinal());
        assertEquals(8, AlterType.MODIFY_TABLE_OPTION.ordinal());
        assertEquals(9, AlterType.RENAME_COLUMN_NAME.ordinal());
    }

    @Test
    public void testToString() {
        assertEquals("ADD_TAG", AlterType.ADD_TAG.toString());
        assertEquals("DROP_TAG", AlterType.DROP_TAG.toString());
        assertEquals("RENAME_TAG_NAME", AlterType.RENAME_TAG_NAME.toString());
        assertEquals("SET_TAG", AlterType.SET_TAG.toString());
        assertEquals("ADD_COLUMN", AlterType.ADD_COLUMN.toString());
        assertEquals("DROP_COLUMN", AlterType.DROP_COLUMN.toString());
        assertEquals("MODIFY_COLUMN_LENGTH", AlterType.MODIFY_COLUMN_LENGTH.toString());
        assertEquals("MODIFY_TAG_LENGTH", AlterType.MODIFY_TAG_LENGTH.toString());
        assertEquals("MODIFY_TABLE_OPTION", AlterType.MODIFY_TABLE_OPTION.toString());
        assertEquals("RENAME_COLUMN_NAME", AlterType.RENAME_COLUMN_NAME.toString());
    }

    @Test
    public void testValueOf() {
        assertSame(AlterType.ADD_TAG, AlterType.valueOf("ADD_TAG"));
        assertSame(AlterType.DROP_TAG, AlterType.valueOf("DROP_TAG"));
        assertSame(AlterType.RENAME_TAG_NAME, AlterType.valueOf("RENAME_TAG_NAME"));
        assertSame(AlterType.SET_TAG, AlterType.valueOf("SET_TAG"));
        assertSame(AlterType.ADD_COLUMN, AlterType.valueOf("ADD_COLUMN"));
        assertSame(AlterType.DROP_COLUMN, AlterType.valueOf("DROP_COLUMN"));
        assertSame(AlterType.MODIFY_COLUMN_LENGTH, AlterType.valueOf("MODIFY_COLUMN_LENGTH"));
        assertSame(AlterType.MODIFY_TAG_LENGTH, AlterType.valueOf("MODIFY_TAG_LENGTH"));
        assertSame(AlterType.MODIFY_TABLE_OPTION, AlterType.valueOf("MODIFY_TABLE_OPTION"));
        assertSame(AlterType.RENAME_COLUMN_NAME, AlterType.valueOf("RENAME_COLUMN_NAME"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValueOfInvalidName() {
        AlterType.valueOf("INVALID_TYPE");
    }

    @Test
    public void testEnumComparison() {
        assertTrue(AlterType.ADD_TAG.getValue() < AlterType.DROP_TAG.getValue());
        assertTrue(AlterType.DROP_TAG.getValue() < AlterType.RENAME_TAG_NAME.getValue());

        assertFalse(AlterType.ADD_TAG.equals(AlterType.DROP_TAG));
        assertNotEquals(AlterType.ADD_TAG, AlterType.DROP_TAG);
    }

    @Test
    public void testEnumIteration() {
        int expectedValue = 1;
        for (AlterType type : AlterType.values()) {
            assertEquals(expectedValue++, type.getValue());
        }
    }

    @Test
    public void testEnumHashCode() {
        assertEquals(AlterType.ADD_TAG.hashCode(), AlterType.ADD_TAG.hashCode());
        assertNotEquals(AlterType.ADD_TAG.hashCode(), AlterType.DROP_TAG.hashCode());
    }

    @Test
    public void testEnumCompareTo() {
        assertTrue(AlterType.ADD_TAG.compareTo(AlterType.DROP_TAG) < 0);
        assertTrue(AlterType.DROP_TAG.compareTo(AlterType.ADD_TAG) > 0);
        assertEquals(0, AlterType.ADD_TAG.compareTo(AlterType.ADD_TAG));
    }

    @Test
    public void testEnumName() {
        assertEquals("ADD_TAG", AlterType.ADD_TAG.name());
        assertEquals("DROP_TAG", AlterType.DROP_TAG.name());
        assertEquals("RENAME_TAG_NAME", AlterType.RENAME_TAG_NAME.name());
        assertEquals("SET_TAG", AlterType.SET_TAG.name());
        assertEquals("ADD_COLUMN", AlterType.ADD_COLUMN.name());
        assertEquals("DROP_COLUMN", AlterType.DROP_COLUMN.name());
        assertEquals("MODIFY_COLUMN_LENGTH", AlterType.MODIFY_COLUMN_LENGTH.name());
        assertEquals("MODIFY_TAG_LENGTH", AlterType.MODIFY_TAG_LENGTH.name());
        assertEquals("MODIFY_TABLE_OPTION", AlterType.MODIFY_TABLE_OPTION.name());
        assertEquals("RENAME_COLUMN_NAME", AlterType.RENAME_COLUMN_NAME.name());
    }
}