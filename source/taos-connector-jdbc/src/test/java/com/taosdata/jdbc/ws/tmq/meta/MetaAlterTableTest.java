package com.taosdata.jdbc.ws.tmq.meta;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class MetaAlterTableTest {
  private MetaAlterTable metaAlterTable;

  @Before
  public void setUp() {
    metaAlterTable = new MetaAlterTable();
  }

  @Test
  public void testDefaultValues() {
    assertEquals(0, metaAlterTable.getAlterType());
    assertNull(metaAlterTable.getColName());
    assertNull(metaAlterTable.getColNewName());
    assertEquals(0, metaAlterTable.getColType());
    assertEquals(0, metaAlterTable.getColLength());
    assertNull(metaAlterTable.getColValue());
    assertFalse(metaAlterTable.isColValueNull());
    assertNull(metaAlterTable.getTags());
    assertNull(metaAlterTable.getEncode());
    assertNull(metaAlterTable.getCompress());
    assertNull(metaAlterTable.getLevel());
    assertNull(metaAlterTable.getRefDbName());
    assertNull(metaAlterTable.getRefTbName());
    assertNull(metaAlterTable.getRefColName());
    assertNull(metaAlterTable.getTables());
    assertNull(metaAlterTable.getWhere());
  }

  @Test
  public void testAlterTypeGetterAndSetter() {
    metaAlterTable.setAlterType(5);
    assertEquals(5, metaAlterTable.getAlterType());

    metaAlterTable.setAlterType(1);
    assertEquals(1, metaAlterTable.getAlterType());

    metaAlterTable.setAlterType(-1);
    assertEquals(-1, metaAlterTable.getAlterType());
  }

  @Test
  public void testColNameGetterAndSetter() {
    String colName = "testColumn";
    metaAlterTable.setColName(colName);
    assertEquals(colName, metaAlterTable.getColName());

    metaAlterTable.setColName(null);
    assertNull(metaAlterTable.getColName());

    metaAlterTable.setColName("");
    assertEquals("", metaAlterTable.getColName());
  }

  @Test
  public void testColNewNameGetterAndSetter() {
    String colNewName = "newColumnName";
    metaAlterTable.setColNewName(colNewName);
    assertEquals(colNewName, metaAlterTable.getColNewName());

    metaAlterTable.setColNewName(null);
    assertNull(metaAlterTable.getColNewName());

    metaAlterTable.setColNewName("");
    assertEquals("", metaAlterTable.getColNewName());
  }

  @Test
  public void testColTypeGetterAndSetter() {
    metaAlterTable.setColType(10);
    assertEquals(10, metaAlterTable.getColType());

    metaAlterTable.setColType(0);
    assertEquals(0, metaAlterTable.getColType());

    metaAlterTable.setColType(-5);
    assertEquals(-5, metaAlterTable.getColType());
  }

  @Test
  public void testColLengthGetterAndSetter() {
    metaAlterTable.setColLength(100);
    assertEquals(100, metaAlterTable.getColLength());

    metaAlterTable.setColLength(0);
    assertEquals(0, metaAlterTable.getColLength());

    metaAlterTable.setColLength(-1);
    assertEquals(-1, metaAlterTable.getColLength());
  }

  @Test
  public void testColValueGetterAndSetter() {
    String colValue = "testValue";
    metaAlterTable.setColValue(colValue);
    assertEquals(colValue, metaAlterTable.getColValue());

    metaAlterTable.setColValue(null);
    assertNull(metaAlterTable.getColValue());

    metaAlterTable.setColValue("");
    assertEquals("", metaAlterTable.getColValue());
  }

  @Test
  public void testColValueNullGetterAndSetter() {
    metaAlterTable.setColValueNull(true);
    assertTrue(metaAlterTable.isColValueNull());

    metaAlterTable.setColValueNull(false);
    assertFalse(metaAlterTable.isColValueNull());
  }

  @Test
  public void testTagsGetterAndSetter() {
    List<TagAlter> tags = Arrays.asList(new TagAlter(), new TagAlter());
    metaAlterTable.setTags(tags);
    assertEquals(tags, metaAlterTable.getTags());
    assertEquals(2, metaAlterTable.getTags().size());

    metaAlterTable.setTags(null);
    assertNull(metaAlterTable.getTags());
  }

  @Test
  public void testEqualsWithSameObject() {
    metaAlterTable.setAlterType(5);
    metaAlterTable.setColName("testCol");
    metaAlterTable.setColType(1);

    assertEquals(metaAlterTable, metaAlterTable);
  }

  @Test
  public void testEqualsWithNull() {
    assertNotEquals(null, metaAlterTable);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    Object other = new Object();
    assertNotEquals(metaAlterTable, other);
  }

  @Test
  public void testEqualsWithSameValues() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setColName("col1");
    table1.setColType(1);
    table1.setColLength(100);
    table1.setColValue("value1");
    table1.setColValueNull(false);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setColName("col1");
    table2.setColType(1);
    table2.setColLength(100);
    table2.setColValue("value1");
    table2.setColValueNull(false);

    assertEquals(table1, table2);
    assertEquals(table1.hashCode(), table2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentValues() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setColName("col1");

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(6);
    table2.setColName("col2");

    assertNotEquals(table1, table2);
  }

  @Test
  public void testEqualsWithTags() {
    List<TagAlter> tags = Arrays.asList(new TagAlter());

    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setTags(tags);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setTags(tags);

    assertEquals(table1, table2);
    assertEquals(table1.hashCode(), table2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentTags() {
    List<TagAlter> tags1 = Arrays.asList(new TagAlter());
    List<TagAlter> tags2 = Arrays.asList(new TagAlter(), new TagAlter());

    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setTags(tags1);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setTags(tags2);

    assertNotEquals(table1, table2);
  }

  @Test
  public void testEqualsWithNullTags() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setTags(null);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setTags(null);

    assertEquals(table1, table2);
  }

  @Test
  public void testEqualsWithOneNullTags() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setTags(Arrays.asList(new TagAlter()));

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setTags(null);

    assertNotEquals(table1, table2);
  }

  @Test
  public void testHashCodeConsistency() {
    metaAlterTable.setAlterType(5);
    metaAlterTable.setColName("test");
    metaAlterTable.setColType(1);
    metaAlterTable.setColLength(100);
    metaAlterTable.setColValue("value");
    metaAlterTable.setColValueNull(true);

    int initialHashCode = metaAlterTable.hashCode();
    assertEquals(initialHashCode, metaAlterTable.hashCode());
  }

  @Test
  public void testHashCodeWithSameValues() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setColName("col1");

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);
    table2.setColName("col1");

    assertEquals(table1.hashCode(), table2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentValues() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);
    table1.setColName("col1");

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(6);
    table2.setColName("col2");

    assertNotEquals(table1.hashCode(), table2.hashCode());
  }

  @Test
  public void testInheritanceFromMeta() {
    assertNotNull(metaAlterTable);
    assertTrue(metaAlterTable instanceof Meta);
  }

  @Test
  public void testAllFieldsTogether() {
    metaAlterTable.setAlterType(4);
    metaAlterTable.setColName("tag1");
    metaAlterTable.setColNewName("tag1_new");
    metaAlterTable.setColType(2);
    metaAlterTable.setColLength(50);
    metaAlterTable.setColValue("new_tag_value");
    metaAlterTable.setColValueNull(false);

    List<TagAlter> tags = Arrays.asList(new TagAlter());
    metaAlterTable.setTags(tags);

    assertEquals(4, metaAlterTable.getAlterType());
    assertEquals("tag1", metaAlterTable.getColName());
    assertEquals("tag1_new", metaAlterTable.getColNewName());
    assertEquals(2, metaAlterTable.getColType());
    assertEquals(50, metaAlterTable.getColLength());
    assertEquals("new_tag_value", metaAlterTable.getColValue());
    assertFalse(metaAlterTable.isColValueNull());
    assertEquals(tags, metaAlterTable.getTags());
  }

  @Test
  public void testEqualsWithColValueNullDifference() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(4);
    table1.setColValueNull(true);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(4);
    table2.setColValueNull(false);

    assertNotEquals(table1, table2);
  }

  @Test
  public void testEqualsWithColNewNameDifference() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(3);
    table1.setColNewName("name1");

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(3);
    table2.setColNewName("name2");

    assertNotEquals(table1, table2);
  }

  @Test
  public void testEqualsWithNullColNewName() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(3);
    table1.setColNewName(null);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(3);
    table2.setColNewName("name");

    assertNotEquals(table1, table2);
  }

  @Test
  public void testSuperClassEqualsImpact() {
    MetaAlterTable table1 = new MetaAlterTable();
    table1.setAlterType(5);

    MetaAlterTable table2 = new MetaAlterTable();
    table2.setAlterType(5);

    assertEquals(table1, table2);
  }

  @Test
  public void testEncodeCompressLevelGetterAndSetter() {
    metaAlterTable.setEncode("simple8b");
    metaAlterTable.setCompress("lz4");
    metaAlterTable.setLevel("medium");

    assertEquals("simple8b", metaAlterTable.getEncode());
    assertEquals("lz4", metaAlterTable.getCompress());
    assertEquals("medium", metaAlterTable.getLevel());

    metaAlterTable.setEncode(null);
    assertNull(metaAlterTable.getEncode());
  }

  @Test
  public void testRefFieldsGetterAndSetter() {
    metaAlterTable.setRefDbName("db_src");
    metaAlterTable.setRefTbName("src_table");
    metaAlterTable.setRefColName("src_col");

    assertEquals("db_src", metaAlterTable.getRefDbName());
    assertEquals("src_table", metaAlterTable.getRefTbName());
    assertEquals("src_col", metaAlterTable.getRefColName());

    metaAlterTable.setRefDbName(null);
    assertNull(metaAlterTable.getRefDbName());
  }

  @Test
  public void testTablesGetterAndSetter() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));
    metaAlterTable.setTables(Arrays.asList(info));

    assertEquals(1, metaAlterTable.getTables().size());
    assertEquals("ct1", metaAlterTable.getTables().get(0).getTableName());

    metaAlterTable.setTables(null);
    assertNull(metaAlterTable.getTables());
  }

  @Test
  public void testWhereGetterAndSetter() {
    metaAlterTable.setWhere("`groupid` = 100");
    assertEquals("`groupid` = 100", metaAlterTable.getWhere());

    metaAlterTable.setWhere(null);
    assertNull(metaAlterTable.getWhere());
  }

  @Test
  public void testEqualsWithNewFields() {
    MetaAlterTable t1 = new MetaAlterTable();
    t1.setAlterType(13);
    t1.setColName("c1");
    t1.setEncode("simple8b");
    t1.setCompress("lz4");
    t1.setLevel("medium");

    MetaAlterTable t2 = new MetaAlterTable();
    t2.setAlterType(13);
    t2.setColName("c1");
    t2.setEncode("simple8b");
    t2.setCompress("lz4");
    t2.setLevel("medium");

    assertEquals(t1, t2);
    assertEquals(t1.hashCode(), t2.hashCode());
  }

  @Test
  public void testNotEqualsWhenEncodeDiffers() {
    MetaAlterTable t1 = new MetaAlterTable();
    t1.setEncode("simple8b");

    MetaAlterTable t2 = new MetaAlterTable();
    t2.setEncode("delta-i");

    assertNotEquals(t1, t2);
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithSameObject() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));
    assertEquals(info, info);
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithNull() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));
    assertNotEquals(null, info);
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithDifferentClass() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));
    assertNotEquals(info, new Object());
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithSameValues() {
    TagAlter tag1 = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info1 = new AlterTableTagsInfo("ct1", Arrays.asList(tag1));

    TagAlter tag2 = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info2 = new AlterTableTagsInfo("ct1", Arrays.asList(tag2));

    assertEquals(info1, info2);
    assertEquals(info1.hashCode(), info2.hashCode());
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithDifferentTableName() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info1 = new AlterTableTagsInfo("ct1", Arrays.asList(tag));
    AlterTableTagsInfo info2 = new AlterTableTagsInfo("ct2", Arrays.asList(tag));

    assertNotEquals(info1, info2);
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithDifferentTags() {
    TagAlter tag1 = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info1 = new AlterTableTagsInfo("ct1", Arrays.asList(tag1));

    TagAlter tag2 = new TagAlter("t2", "200", false);
    AlterTableTagsInfo info2 = new AlterTableTagsInfo("ct1", Arrays.asList(tag2));

    assertNotEquals(info1, info2);
  }

  @Test
  public void testAlterTableTagsInfoEqualsWithNullTags() {
    AlterTableTagsInfo info1 = new AlterTableTagsInfo("ct1", null);
    AlterTableTagsInfo info2 = new AlterTableTagsInfo("ct1", null);

    assertEquals(info1, info2);
    assertEquals(info1.hashCode(), info2.hashCode());
  }

  @Test
  public void testAlterTableTagsInfoHashCodeConsistency() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));

    int initialHashCode = info.hashCode();
    assertEquals(initialHashCode, info.hashCode());
  }

  @Test
  public void testAlterTableTagsInfoHashCodeWithSameValues() {
    TagAlter tag1 = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info1 = new AlterTableTagsInfo("ct1", Arrays.asList(tag1));

    TagAlter tag2 = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info2 = new AlterTableTagsInfo("ct1", Arrays.asList(tag2));

    assertEquals(info1.hashCode(), info2.hashCode());
  }

  @Test
  public void testAlterTableTagsInfoToString() {
    TagAlter tag = new TagAlter("t1", "100", false);
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", Arrays.asList(tag));

    String result = info.toString();
    assertTrue(result.contains("AlterTableTagsInfo"));
    assertTrue(result.contains("tableName='ct1'"));
    assertTrue(result.contains("tags="));
  }

  @Test
  public void testAlterTableTagsInfoToStringWithNullTags() {
    AlterTableTagsInfo info = new AlterTableTagsInfo("ct1", null);

    String result = info.toString();
    assertTrue(result.contains("AlterTableTagsInfo"));
    assertTrue(result.contains("tableName='ct1'"));
    assertTrue(result.contains("tags="));
  }
}
