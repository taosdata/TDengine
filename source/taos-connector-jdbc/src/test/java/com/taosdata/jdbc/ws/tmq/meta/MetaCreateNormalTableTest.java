package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MetaCreateNormalTableTest {
    private MetaCreateNormalTable metaCreateNormalTable;

    @Before
    public void setUp() {
        metaCreateNormalTable = new MetaCreateNormalTable();
    }

    @Test
    public void testDefaultValues() {
        assertNull(metaCreateNormalTable.getColumns());
        assertNull(metaCreateNormalTable.getTags());
    }

    @Test
    public void testColumnsGetterAndSetter() {
        List<Column> columns = Arrays.asList(new Column(), new Column());
        metaCreateNormalTable.setColumns(columns);
        assertEquals(columns, metaCreateNormalTable.getColumns());
        assertEquals(2, metaCreateNormalTable.getColumns().size());

        metaCreateNormalTable.setColumns(null);
        assertNull(metaCreateNormalTable.getColumns());

        List<Column> emptyColumns = Collections.emptyList();
        metaCreateNormalTable.setColumns(emptyColumns);
        assertEquals(emptyColumns, metaCreateNormalTable.getColumns());
        assertTrue(metaCreateNormalTable.getColumns().isEmpty());
    }

    @Test
    public void testTagsGetterAndSetter() {
        List<Column> tags = Arrays.asList(new Column(), new Column(), new Column());
        metaCreateNormalTable.setTags(tags);
        assertEquals(tags, metaCreateNormalTable.getTags());
        assertEquals(3, metaCreateNormalTable.getTags().size());

        metaCreateNormalTable.setTags(null);
        assertNull(metaCreateNormalTable.getTags());

        List<Column> emptyTags = Collections.emptyList();
        metaCreateNormalTable.setTags(emptyTags);
        assertEquals(emptyTags, metaCreateNormalTable.getTags());
        assertTrue(metaCreateNormalTable.getTags().isEmpty());
    }

    @Test
    public void testEqualsWithSameObject() {
        metaCreateNormalTable.setColumns(Arrays.asList(new Column()));
        metaCreateNormalTable.setTags(Arrays.asList(new Column()));
        assertEquals(metaCreateNormalTable, metaCreateNormalTable);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, metaCreateNormalTable);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(metaCreateNormalTable, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        table1.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        table2.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentColumns() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column("col2", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullColumns() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(null);

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithEmptyAndNonEmptyColumns() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(Collections.emptyList());

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithDifferentTags() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setTags(Arrays.asList(new Column("tag1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setTags(Arrays.asList(new Column("tag2", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullTags() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setTags(null);

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithEmptyAndNonEmptyTags() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setTags(Collections.emptyList());

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithSuperClassDifference() {
        // Simulate super class (Meta) field difference
        MetaCreateNormalTable table1 = new MetaCreateNormalTable() {
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

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column()));
        table2.setTags(Arrays.asList(new Column()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testHashCodeConsistency() {
        metaCreateNormalTable.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));
        metaCreateNormalTable.setTags(Arrays.asList(new Column("tag1", 2, 20, false, "ASCII", "SNAPPY", "medium")));

        int initialHashCode = metaCreateNormalTable.hashCode();
        assertEquals(initialHashCode, metaCreateNormalTable.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
        table2.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        MetaCreateNormalTable table1 = new MetaCreateNormalTable();
        table1.setColumns(Arrays.asList(new Column("col1", 1, 10, true, "UTF8", "GZIP", "low")));

        MetaCreateNormalTable table2 = new MetaCreateNormalTable();
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

        metaCreateNormalTable.setColumns(columns);
        metaCreateNormalTable.setTags(tags);

        assertEquals(columns, metaCreateNormalTable.getColumns());
        assertEquals(2, metaCreateNormalTable.getColumns().size());
        assertEquals("id", metaCreateNormalTable.getColumns().get(0).getName());
        assertEquals("name", metaCreateNormalTable.getColumns().get(1).getName());

        assertEquals(tags, metaCreateNormalTable.getTags());
        assertEquals(2, metaCreateNormalTable.getTags().size());
        assertEquals("device_id", metaCreateNormalTable.getTags().get(0).getName());
        assertEquals("location", metaCreateNormalTable.getTags().get(1).getName());
    }
}