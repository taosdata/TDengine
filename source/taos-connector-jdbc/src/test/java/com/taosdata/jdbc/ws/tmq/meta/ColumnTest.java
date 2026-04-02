package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ColumnTest {
    private Column column;

    @Before
    public void setUp() {
        column = new Column();
    }

    @Test
    public void testDefaultValues() {
        assertNull(column.getName());
        assertEquals(0, column.getType());
        assertNull(column.getLength());
        assertNull(column.isPrimarykey());
        assertNull(column.getEncode());
        assertNull(column.getCompress());
        assertNull(column.getLevel());
    }

    @Test
    public void testNameGetterAndSetter() {
        String name = "testColumn";
        column.setName(name);
        assertEquals(name, column.getName());

        column.setName(null);
        assertNull(column.getName());

        column.setName("");
        assertEquals("", column.getName());
    }

    @Test
    public void testTypeGetterAndSetter() {
        column.setType(10);
        assertEquals(10, column.getType());

        column.setType(0);
        assertEquals(0, column.getType());

        column.setType(-5);
        assertEquals(-5, column.getType());
    }

    @Test
    public void testLengthGetterAndSetter() {
        Integer length = 255;
        column.setLength(length);
        assertEquals(length, column.getLength());

        column.setLength(null);
        assertNull(column.getLength());

        column.setLength(0);
        assertEquals(0, column.getLength().intValue());
    }

    @Test
    public void testPrimarykeyGetterAndSetter() {
        column.setPrimarykey(true);
        assertTrue(column.isPrimarykey());

        column.setPrimarykey(false);
        assertFalse(column.isPrimarykey());

        column.setPrimarykey(null);
        assertNull(column.isPrimarykey());
    }

    @Test
    public void testEncodeGetterAndSetter() {
        String encode = "UTF8";
        column.setEncode(encode);
        assertEquals(encode, column.getEncode());

        column.setEncode(null);
        assertNull(column.getEncode());

        column.setEncode("");
        assertEquals("", column.getEncode());
    }

    @Test
    public void testCompressGetterAndSetter() {
        String compress = "GZIP";
        column.setCompress(compress);
        assertEquals(compress, column.getCompress());

        column.setCompress(null);
        assertNull(column.getCompress());

        column.setCompress("");
        assertEquals("", column.getCompress());
    }

    @Test
    public void testLevelGetterAndSetter() {
        String level = "high";
        column.setLevel(level);
        assertEquals(level, column.getLevel());

        column.setLevel(null);
        assertNull(column.getLevel());

        column.setLevel("");
        assertEquals("", column.getLevel());
    }

    @Test
    public void testAllArgsConstructor() {
        String name = "id";
        int type = 1;
        Integer length = 10;
        Boolean primarykey = true;
        String encode = "ASCII";
        String compress = "SNAPPY";
        String level = "medium";

        Column col = new Column(name, type, length, primarykey, encode, compress, level);
        assertEquals(name, col.getName());
        assertEquals(type, col.getType());
        assertEquals(length, col.getLength());
        assertEquals(primarykey, col.isPrimarykey());
        assertEquals(encode, col.getEncode());
        assertEquals(compress, col.getCompress());
        assertEquals(level, col.getLevel());
    }

    @Test
    public void testEqualsWithSameObject() {
        column.setName("test");
        column.setType(5);
        assertEquals(column, column);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, column);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(column, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        Column column1 = new Column();
        column1.setName("col1");
        column1.setType(2);
        column1.setLength(50);
        column1.setPrimarykey(true);
        column1.setEncode("UTF8");
        column1.setCompress("GZIP");
        column1.setLevel("low");

        Column column2 = new Column();
        column2.setName("col1");
        column2.setType(2);
        column2.setLength(50);
        column2.setPrimarykey(true);
        column2.setEncode("UTF8");
        column2.setCompress("GZIP");
        column2.setLevel("low");

        assertEquals(column1, column2);
        assertEquals(column1.hashCode(), column2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentName() {
        Column column1 = new Column();
        column1.setName("col1");

        Column column2 = new Column();
        column2.setName("col2");

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentType() {
        Column column1 = new Column();
        column1.setType(1);

        Column column2 = new Column();
        column2.setType(2);

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentLength() {
        Column column1 = new Column();
        column1.setLength(10);

        Column column2 = new Column();
        column2.setLength(20);

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithNullAndNonNullLength() {
        Column column1 = new Column();
        column1.setLength(null);

        Column column2 = new Column();
        column2.setLength(10);

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentPrimarykey() {
        Column column1 = new Column();
        column1.setPrimarykey(true);

        Column column2 = new Column();
        column2.setPrimarykey(false);

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithNullAndNonNullPrimarykey() {
        Column column1 = new Column();
        column1.setPrimarykey(null);

        Column column2 = new Column();
        column2.setPrimarykey(true);

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentEncode() {
        Column column1 = new Column();
        column1.setEncode("UTF8");

        Column column2 = new Column();
        column2.setEncode("GBK");

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentCompress() {
        Column column1 = new Column();
        column1.setCompress("GZIP");

        Column column2 = new Column();
        column2.setCompress("LZ4");

        assertNotEquals(column1, column2);
    }

    @Test
    public void testEqualsWithDifferentLevel() {
        Column column1 = new Column();
        column1.setLevel("high");

        Column column2 = new Column();
        column2.setLevel("low");

        assertNotEquals(column1, column2);
    }

    @Test
    public void testHashCodeConsistency() {
        column.setName("test");
        column.setType(5);
        column.setLength(100);
        column.setPrimarykey(true);
        column.setEncode("UTF8");
        column.setCompress("GZIP");
        column.setLevel("medium");

        int initialHashCode = column.hashCode();
        assertEquals(initialHashCode, column.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        Column column1 = new Column();
        column1.setName("col1");
        column1.setType(2);

        Column column2 = new Column();
        column2.setName("col1");
        column2.setType(2);

        assertEquals(column1.hashCode(), column2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        Column column1 = new Column();
        column1.setName("col1");
        column1.setType(2);

        Column column2 = new Column();
        column2.setName("col2");
        column2.setType(3);

        assertNotEquals(column1.hashCode(), column2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        column.setName("username");
        column.setType(3);
        column.setLength(30);
        column.setPrimarykey(false);
        column.setEncode("UTF16");
        column.setCompress("ZSTD");
        column.setLevel("high");

        assertEquals("username", column.getName());
        assertEquals(3, column.getType());
        assertEquals(30, column.getLength().intValue());
        assertFalse(column.isPrimarykey());
        assertEquals("UTF16", column.getEncode());
        assertEquals("ZSTD", column.getCompress());
        assertEquals("high", column.getLevel());
    }
}