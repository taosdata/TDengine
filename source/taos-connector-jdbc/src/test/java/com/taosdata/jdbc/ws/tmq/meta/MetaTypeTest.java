package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Test;

import static org.junit.Assert.*;

public class MetaTypeTest {

    @Test
    public void testFromStringWithValidUpperCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("CREATE"));
        assertEquals(MetaType.DROP, MetaType.fromString("DROP"));
        assertEquals(MetaType.ALTER, MetaType.fromString("ALTER"));
        assertEquals(MetaType.DELETE, MetaType.fromString("DELETE"));
    }

    @Test
    public void testFromStringWithValidLowerCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("create"));
        assertEquals(MetaType.DROP, MetaType.fromString("drop"));
        assertEquals(MetaType.ALTER, MetaType.fromString("alter"));
        assertEquals(MetaType.DELETE, MetaType.fromString("delete"));
    }

    @Test
    public void testFromStringWithValidMixedCaseValues() {
        assertEquals(MetaType.CREATE, MetaType.fromString("CrEaTe"));
        assertEquals(MetaType.DROP, MetaType.fromString("DrOp"));
        assertEquals(MetaType.ALTER, MetaType.fromString("AlTeR"));
        assertEquals(MetaType.DELETE, MetaType.fromString("DeLeTe"));
    }

    @Test
    public void testFromStringWithNullValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString(null);
        });
        assertEquals("MetaType value cannot be null", exception.getMessage());
    }

    @Test
    public void testFromStringWithInvalidValue() {
        // Test non-existent value
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString("UPDATE");
        });
        assertEquals("Invalid MetaType value: UPDATE", exception.getMessage());

        // Test empty string
        exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString("");
        });
        assertEquals("Invalid MetaType value: ", exception.getMessage());

        // Test whitespace value
        exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.fromString(" CREATE ");
        });
        assertEquals("Invalid MetaType value:  CREATE ", exception.getMessage());
    }

    @Test
    public void testMatchesWithMatchingValue() {
        assertTrue(MetaType.CREATE.matches("CREATE"));
        assertTrue(MetaType.CREATE.matches("create"));
        assertTrue(MetaType.CREATE.matches("CrEaTe"));

        assertTrue(MetaType.DROP.matches("DROP"));
        assertTrue(MetaType.DROP.matches("drop"));

        assertTrue(MetaType.ALTER.matches("ALTER"));
        assertTrue(MetaType.ALTER.matches("alter"));

        assertTrue(MetaType.DELETE.matches("DELETE"));
        assertTrue(MetaType.DELETE.matches("delete"));
    }

    @Test
    public void testMatchesWithNonMatchingValue() {
        assertFalse(MetaType.CREATE.matches("DROP"));
        assertFalse(MetaType.DROP.matches("ALTER"));
        assertFalse(MetaType.ALTER.matches("DELETE"));
        assertFalse(MetaType.DELETE.matches("CREATE"));
    }

    @Test
    public void testMatchesWithNullValue() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            MetaType.CREATE.matches(null);
        });
        assertEquals("MetaType value cannot be null", exception.getMessage());
    }

    @Test
    public void testEnumValuesCount() {
        // Ensure the enum has exactly 4 values (CREATE/DROP/ALTER/DELETE)
        assertEquals(4, MetaType.values().length);
    }
}