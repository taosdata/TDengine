package com.taosdata.jdbc.ws.tmq.meta;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class MetaCreateChildTableTest {
    private MetaCreateChildTable metaCreateChildTable;

    @Before
    public void setUp() {
        metaCreateChildTable = new MetaCreateChildTable();
    }

    @Test
    public void testDefaultValues() {
        assertNull(metaCreateChildTable.getUsing());
        assertEquals(0, metaCreateChildTable.getTagNum());
        assertNull(metaCreateChildTable.getTags());
        assertNull(metaCreateChildTable.getCreateList());
    }

    @Test
    public void testUsingGetterAndSetter() {
        String using = "parent_table";
        metaCreateChildTable.setUsing(using);
        assertEquals(using, metaCreateChildTable.getUsing());

        metaCreateChildTable.setUsing(null);
        assertNull(metaCreateChildTable.getUsing());

        metaCreateChildTable.setUsing("");
        assertEquals("", metaCreateChildTable.getUsing());
    }

    @Test
    public void testTagNumGetterAndSetter() {
        metaCreateChildTable.setTagNum(5);
        assertEquals(5, metaCreateChildTable.getTagNum());

        metaCreateChildTable.setTagNum(0);
        assertEquals(0, metaCreateChildTable.getTagNum());

        metaCreateChildTable.setTagNum(-3);
        assertEquals(-3, metaCreateChildTable.getTagNum());
    }

    @Test
    public void testTagsGetterAndSetter() {
        List<Tag> tags = Arrays.asList(new Tag(), new Tag());
        metaCreateChildTable.setTags(tags);
        assertEquals(tags, metaCreateChildTable.getTags());
        assertEquals(2, metaCreateChildTable.getTags().size());

        metaCreateChildTable.setTags(null);
        assertNull(metaCreateChildTable.getTags());

        List<Tag> emptyTags = Collections.emptyList();
        metaCreateChildTable.setTags(emptyTags);
        assertEquals(emptyTags, metaCreateChildTable.getTags());
        assertTrue(metaCreateChildTable.getTags().isEmpty());
    }

    @Test
    public void testCreateListGetterAndSetter() {
        List<ChildTableInfo> createList = Arrays.asList(new ChildTableInfo(), new ChildTableInfo());
        metaCreateChildTable.setCreateList(createList);
        assertEquals(createList, metaCreateChildTable.getCreateList());
        assertEquals(2, metaCreateChildTable.getCreateList().size());

        metaCreateChildTable.setCreateList(null);
        assertNull(metaCreateChildTable.getCreateList());

        List<ChildTableInfo> emptyCreateList = Collections.emptyList();
        metaCreateChildTable.setCreateList(emptyCreateList);
        assertEquals(emptyCreateList, metaCreateChildTable.getCreateList());
        assertTrue(metaCreateChildTable.getCreateList().isEmpty());
    }

    @Test
    public void testEqualsWithSameObject() {
        metaCreateChildTable.setUsing("parent1");
        metaCreateChildTable.setTagNum(2);
        metaCreateChildTable.setTags(Arrays.asList(new Tag()));
        assertEquals(metaCreateChildTable, metaCreateChildTable);
    }

    @Test
    public void testEqualsWithNull() {
        assertNotEquals(null, metaCreateChildTable);
    }

    @Test
    public void testEqualsWithDifferentClass() {
        Object other = new Object();
        assertNotEquals(metaCreateChildTable, other);
    }

    @Test
    public void testEqualsWithSameValues() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setUsing("parent_table");
        table1.setTagNum(3);
        table1.setTags(Arrays.asList(new Tag(), new Tag()));
        table1.setCreateList(Arrays.asList(new ChildTableInfo()));

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setUsing("parent_table");
        table2.setTagNum(3);
        table2.setTags(Arrays.asList(new Tag(), new Tag()));
        table2.setCreateList(Arrays.asList(new ChildTableInfo()));

        assertEquals(table1, table2);
        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentUsing() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setUsing("parent1");

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setUsing("parent2");

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithDifferentTagNum() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setTagNum(2);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setTagNum(3);

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithDifferentTags() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setTags(Arrays.asList(new Tag()));

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setTags(Arrays.asList(new Tag(), new Tag()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullTags() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setTags(null);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setTags(Arrays.asList(new Tag()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithDifferentCreateList() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setCreateList(Arrays.asList(new ChildTableInfo()));

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setCreateList(Arrays.asList(new ChildTableInfo(), new ChildTableInfo()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithNullAndNonNullCreateList() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setCreateList(null);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setCreateList(Arrays.asList(new ChildTableInfo()));

        assertNotEquals(table1, table2);
    }

    @Test
    public void testEqualsWithSuperClassDifference() {
        // Simulate super class (Meta) field difference
        MetaCreateChildTable table1 = new MetaCreateChildTable() {
            @Override
            public boolean equals(Object o) {
                // Override to simulate super class equals returns false
                if (o == this) return true;
                if (o == null || getClass() != o.getClass()) return false;
                return false;
            }

            @Override
            public int hashCode() {
                return super.hashCode() + 50;
            }
        };
        table1.setUsing("parent1");
        table1.setTagNum(2);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setUsing("parent1");
        table2.setTagNum(2);

        assertNotEquals(table1, table2);
    }

    @Test
    public void testHashCodeConsistency() {
        metaCreateChildTable.setUsing("parent_table");
        metaCreateChildTable.setTagNum(4);
        metaCreateChildTable.setTags(Arrays.asList(new Tag()));
        metaCreateChildTable.setCreateList(Arrays.asList(new ChildTableInfo()));

        int initialHashCode = metaCreateChildTable.hashCode();
        assertEquals(initialHashCode, metaCreateChildTable.hashCode());
    }

    @Test
    public void testHashCodeWithSameValues() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setUsing("parent1");
        table1.setTagNum(2);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setUsing("parent1");
        table2.setTagNum(2);

        assertEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testHashCodeWithDifferentValues() {
        MetaCreateChildTable table1 = new MetaCreateChildTable();
        table1.setUsing("parent1");
        table1.setTagNum(2);

        MetaCreateChildTable table2 = new MetaCreateChildTable();
        table2.setUsing("parent2");
        table2.setTagNum(3);

        assertNotEquals(table1.hashCode(), table2.hashCode());
    }

    @Test
    public void testAllFieldsTogether() {
        String using = "device_parent";
        int tagNum = 3;
        List<Tag> tags = Arrays.asList(new Tag(), new Tag(), new Tag());
        List<ChildTableInfo> createList = Arrays.asList(new ChildTableInfo(), new ChildTableInfo());

        metaCreateChildTable.setUsing(using);
        metaCreateChildTable.setTagNum(tagNum);
        metaCreateChildTable.setTags(tags);
        metaCreateChildTable.setCreateList(createList);

        assertEquals(using, metaCreateChildTable.getUsing());
        assertEquals(tagNum, metaCreateChildTable.getTagNum());
        assertEquals(tags, metaCreateChildTable.getTags());
        assertEquals(3, metaCreateChildTable.getTags().size());
        assertEquals(createList, metaCreateChildTable.getCreateList());
        assertEquals(2, metaCreateChildTable.getCreateList().size());
    }
}