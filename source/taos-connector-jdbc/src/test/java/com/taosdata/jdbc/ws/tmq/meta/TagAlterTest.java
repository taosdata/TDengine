package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TagAlterTest {
    private TagAlter tagAlter;

    @Before
    public void setUp() {
        tagAlter = new TagAlter();
    }

    @Test
    public void testDefaultValues() {
        assertNull(tagAlter.getColName());
        assertNull(tagAlter.getColValue());
        assertFalse(tagAlter.isColValueNull());
    }

    @Test
    public void testColNameGetterAndSetter() {
        String colName = "tag_id";
        tagAlter.setColName(colName);
        assertEquals(colName, tagAlter.getColName());

        tagAlter.setColName(null);
        assertNull(tagAlter.getColName());

        tagAlter.setColName("");
        assertEquals("", tagAlter.getColName());
    }

    @Test
    public void testColValueGetterAndSetter() {
        String colValue = "tag_value_001";
        tagAlter.setColValue(colValue);
        assertEquals(colValue, tagAlter.getColValue());

        tagAlter.setColValue(null);
        assertNull(tagAlter.getColValue());

        tagAlter.setColValue("");
        assertEquals("", tagAlter.getColValue());
    }

    @Test
    public void testColValueNullGetterAndSetter() {
        tagAlter.setColValueNull(true);
        assertTrue(tagAlter.isColValueNull());

        tagAlter.setColValueNull(false);
        assertFalse(tagAlter.isColValueNull());
    }

    @Test
    public void testAllArgsConstructor() {
        String colName = "device_tag";
        String colValue = "sensor_room_1";
        boolean colValueNull = true;

        TagAlter tagAlter = new TagAlter(colName, colValue, colValueNull);
        assertEquals(colName, tagAlter.getColName());
        assertEquals(colValue, tagAlter.getColValue());
        assertEquals(colValueNull, tagAlter.isColValueNull());
    }

    @Test
    public void testEqualsWithSameObject() {
        tagAlter.setColName("test_tag");
        tagAlter.setColValue("test_value");
        tagAlter.setColValueNull(false);
        assertEquals(tagAlter, tagAlter);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, tagAlter);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(tagAlter, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColName("tag1");
        tagAlter1.setColValue("value1");
        tagAlter1.setColValueNull(true);

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColName("tag1");
        tagAlter2.setColValue("value1");
        tagAlter2.setColValueNull(true);

        assertEquals(tagAlter1, tagAlter2);
        assertEquals(tagAlter1.hashCode(), tagAlter2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentColName() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColName("tag1");

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColName("tag2");

        assertNotEquals(tagAlter1, tagAlter2);
    }

    @Test
    public void testEqualsWithDifferentColValue() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColValue("value1");

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColValue("value2");

        assertNotEquals(tagAlter1, tagAlter2);
    }

    @Test
    public void testEqualsWithDifferentColValueNull() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColValueNull(true);

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColValueNull(false);

        assertNotEquals(tagAlter1, tagAlter2);
    }

    @Test
    public void testEqualsWithNullAndNonNullColName() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColName(null);

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColName("tag1");

        assertNotEquals(tagAlter1, tagAlter2);
    }

    @Test
    public void testEqualsWithNullAndNonNullColValue() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColValue(null);

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColValue("value1");

        assertNotEquals(tagAlter1, tagAlter2);
    }

    @Test
    public void testHashCodeConsistency() {
        tagAlter.setColName("test_tag");
        tagAlter.setColValue("test_value");
        tagAlter.setColValueNull(true);

        int initialHashCode = tagAlter.hashCode();
        assertEquals(initialHashCode, tagAlter.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColName("tag1");
        tagAlter1.setColValue("value1");

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColName("tag1");
        tagAlter2.setColValue("value1");

        assertEquals(tagAlter1.hashCode(), tagAlter2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        TagAlter tagAlter1 = new TagAlter();
        tagAlter1.setColName("tag1");
        tagAlter1.setColValue("value1");

        TagAlter tagAlter2 = new TagAlter();
        tagAlter2.setColName("tag2");
        tagAlter2.setColValue("value2");

        assertNotEquals(tagAlter1.hashCode(), tagAlter2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        tagAlter.setColName("location_tag");
        tagAlter.setColValue("building_5");
        tagAlter.setColValueNull(false);

        assertEquals("location_tag", tagAlter.getColName());
        assertEquals("building_5", tagAlter.getColValue());
        assertFalse(tagAlter.isColValueNull());
    }
}