package com.taosdata.jdbc.enums;

import org.junit.Test;

import static org.junit.Assert.*;

public class SchemalessProtocolTypeTest {

    @Test
    public void testParse() {
        assertEquals(SchemalessProtocolType.LINE, SchemalessProtocolType.parse("LINE"));
        assertEquals(SchemalessProtocolType.TELNET, SchemalessProtocolType.parse("TELNET"));
        assertEquals(SchemalessProtocolType.JSON, SchemalessProtocolType.parse("JSON"));
        assertEquals(SchemalessProtocolType.UNKNOWN, SchemalessProtocolType.parse("INVALID"));
    }

    @Test
    public void testParse_CaseInsensitive() {
        assertEquals(SchemalessProtocolType.LINE, SchemalessProtocolType.parse("line"));
    }
}