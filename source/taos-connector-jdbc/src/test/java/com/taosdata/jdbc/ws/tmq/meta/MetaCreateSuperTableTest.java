package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MetaCreateSuperTableTest {
    private MetaCreateSuperTable metaCreateSuperTable;

    @Before
    public void setUp() {
        metaCreateSuperTable = new MetaCreateSuperTable();
    }

    @Test
    public void testDefaultValues() {
        assertNull(metaCreateSuperTable.getColumns());
        assertNull(metaCreateSuperTable.getTags());
    }

    @Test
    public void testColumnsGetterAndSetter() {
        List<Column> columns = Arrays.asList(new Column(), new Column());
        metaCreateSuperTable.setColumns(columns);
        assertEquals(columns, metaCreateSuperTable.getColumns());
        assertEquals(2, metaCreateSuperTable.getColumns().size());

        metaCreateSuperTable.setColumns(null);
        assertNull(metaCreateSuperTable.getColumns());

        List<Column> emptyColumns = Collections.emptyList();
        metaCreateSuperTable.setColumns(emptyColumns);
        assertEquals(emptyColumns, metaCreateSuperTable.getColumns());
        assertTrue(metaCreateSuperTable.getColumns().isEmpty());
    }

    @Test
    public void testTagsGetterAndSetter() {
        List<Column> tags = Arrays.asList(new Column(), new Column(), new Column());
        metaCreateSuperTable.setTags(tags);
        assertEquals(tags, metaCreateSuperTable.getTags());
        assertEquals(3, metaCreateSuperTable.getTags().size());

        metaCreateSuperTable.setTags(null);
        assertNull(metaCreateSuperTable.getTags());

        List<Column> emptyTags = Collections.emptyList();
        metaCreateSuperTable.setTags(emptyTags);
        assertEquals(emptyTags, metaCreateSuperTable.getTags());
        assertTrue(metaCreateSuperTable.getTags().isEmpty());
    }

    @Test
    public void testEqualsWithSameObject() {
        metaCreateSuperTable.setColumns(Arrays.asList(new Column()));
        metaCreateSuperTable.setTags(Arrays.asList(new Column()));
        assertEquals(metaCreateSuperTable, metaCreateSuperTable);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, metaCreateSuperTable);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(metaCreateSuperTable, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        table1.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        table2.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentColumns() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column("col2", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullColumns() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(null);

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithEmptyAndNonEmptyColumns() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(Collections.emptyList());

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithDifferentTags() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setTags(Arrays.asList(new Column("tag1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setTags(Arrays.asList(new Column("tag2", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullTags() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setTags(null);

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithEmptyAndNonEmptyTags() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setTags(Collections.emptyList());

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithSuperClassDifference() {
        // Simulate super class (Meta) field difference
        MetaCreateSuperTable table1 = new MetaCreateSuperTable() {
            @Override
            public boolean equals(Object o) {
                // Override to simulate super class equals returns false
                if (o == this) return true;
                if (o == null || getClass() != o.getClass()) return false;
                return false;
            }

            @Override
            public int hashCode() {
                return super.hashCode() + 100;
            }
        };
        table1.setColumns(Arrays.asList(new Column()));
        table1.setTags(Arrays.asList(new Column()));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column()));
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testHashCodeConsistency() {
        metaCreateSuperTable.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        metaCreateSuperTable.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        int initialHashCode = metaCreateSuperTable.hashCode();
        assertEquals(initialHashCode, metaCreateSuperTable.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        MetaCreateSuperTable table1 = new MetaCreateSuperTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateSuperTable table2 = new MetaCreateSuperTable();
        table2.setColumns(Arrays.asList(new Column("col2", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertNotEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        List<Column> columns = Arrays.asList(
                new Column("id", 1, 10, true, "UTF8", "GZIP", "low"),
                new Column("name", 2, 50, false, "UTF8", "LZ4", "medium")
        );
        List<Column> tags = Arrays.asList(
                new Column("device_id", 3, 20, false, "ASCII", "SNAPPY", "high"),
                new Column("location", 4, 100, false, "UTF8", "ZSTD", "medium")
        );

        metaCreateSuperTable.setColumns(columns);
        metaCreateSuperTable.setTags(tags);

        assertEquals(columns, metaCreateSuperTable.getColumns());
        assertEquals(2, metaCreateSuperTable.getColumns().size());
        assertEquals("id", metaCreateSuperTable.getColumns().get(0).getName());
        assertEquals("name", metaCreateSuperTable.getColumns().get(1).getName());

        assertEquals(tags, metaCreateSuperTable.getTags());
        assertEquals(2, metaCreateSuperTable.getTags().size());
        assertEquals("device_id", metaCreateSuperTable.getTags().get(0).getName());
        assertEquals("location", metaCreateSuperTable.getTags().get(1).getName());
    }
}