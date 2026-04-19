package com.taosdata.jdbc.ws.tmq.meta;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ChildColRefTest {
  private ChildColRef childColRef;

  @Before
  public void setUp() {
    childColRef = new ChildColRef();
  }

  @Test
  public void testDefaultConstructor() {
    ChildColRef ref = new ChildColRef();
    assertNull(ref.getColName());
    assertNull(ref.getRefDbName());
    assertNull(ref.getRefTableName());
    assertNull(ref.getRefColName());
  }

  @Test
  public void testParameterizedConstructor() {
    ChildColRef ref = new ChildColRef("col1", "db1", "table1", "refCol1");
    assertEquals("col1", ref.getColName());
    assertEquals("db1", ref.getRefDbName());
    assertEquals("table1", ref.getRefTableName());
    assertEquals("refCol1", ref.getRefColName());
  }

  @Test
  public void testColNameGetterAndSetter() {
    childColRef.setColName("testColumn");
    assertEquals("testColumn", childColRef.getColName());

    childColRef.setColName(null);
    assertNull(childColRef.getColName());

    childColRef.setColName("");
    assertEquals("", childColRef.getColName());
  }

  @Test
  public void testRefDbNameGetterAndSetter() {
    childColRef.setRefDbName("testDb");
    assertEquals("testDb", childColRef.getRefDbName());

    childColRef.setRefDbName(null);
    assertNull(childColRef.getRefDbName());

    childColRef.setRefDbName("");
    assertEquals("", childColRef.getRefDbName());
  }

  @Test
  public void testRefTableNameGetterAndSetter() {
    childColRef.setRefTableName("testTable");
    assertEquals("testTable", childColRef.getRefTableName());

    childColRef.setRefTableName(null);
    assertNull(childColRef.getRefTableName());

    childColRef.setRefTableName("");
    assertEquals("", childColRef.getRefTableName());
  }

  @Test
  public void testRefColNameGetterAndSetter() {
    childColRef.setRefColName("testRefCol");
    assertEquals("testRefCol", childColRef.getRefColName());

    childColRef.setRefColName(null);
    assertNull(childColRef.getRefColName());

    childColRef.setRefColName("");
    assertEquals("", childColRef.getRefColName());
  }

  @Test
  public void testEqualsWithSameObject() {
    childColRef.setColName("col1");
    childColRef.setRefDbName("db1");
    childColRef.setRefTableName("table1");
    childColRef.setRefColName("refCol1");

    assertEquals(childColRef, childColRef);
  }

  @Test
  public void testEqualsWithNull() {
    childColRef.setColName("col1");
    assertNotEquals(null, childColRef);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    childColRef.setColName("col1");
    assertNotEquals(childColRef, new Object());
  }

  @Test
  public void testEqualsWithSameValues() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col1", "db1", "table1", "refCol1");

    assertEquals(ref1, ref2);
    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentColName() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col2", "db1", "table1", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithDifferentRefDbName() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col1", "db2", "table1", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithDifferentRefTableName() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col1", "db1", "table2", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithDifferentRefColName() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col1", "db1", "table1", "refCol2");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithNullValues() {
    ChildColRef ref1 = new ChildColRef(null, null, null, null);
    ChildColRef ref2 = new ChildColRef(null, null, null, null);

    assertEquals(ref1, ref2);
    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testEqualsWithOneNullValue() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef(null, "db1", "table1", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testHashCodeConsistency() {
    childColRef.setColName("col1");
    childColRef.setRefDbName("db1");
    childColRef.setRefTableName("table1");
    childColRef.setRefColName("refCol1");

    int initialHashCode = childColRef.hashCode();
    assertEquals(initialHashCode, childColRef.hashCode());
  }

  @Test
  public void testHashCodeWithSameValues() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col1", "db1", "table1", "refCol1");

    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentValues() {
    ChildColRef ref1 = new ChildColRef("col1", "db1", "table1", "refCol1");
    ChildColRef ref2 = new ChildColRef("col2", "db1", "table1", "refCol1");

    assertNotEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testAllFieldsTogether() {
    childColRef.setColName("myColumn");
    childColRef.setRefDbName("myDatabase");
    childColRef.setRefTableName("myTable");
    childColRef.setRefColName("myRefColumn");

    assertEquals("myColumn", childColRef.getColName());
    assertEquals("myDatabase", childColRef.getRefDbName());
    assertEquals("myTable", childColRef.getRefTableName());
    assertEquals("myRefColumn", childColRef.getRefColName());
  }

  @Test
  public void testToString() {
    childColRef.setColName("col1");
    childColRef.setRefDbName("db1");
    childColRef.setRefTableName("table1");
    childColRef.setRefColName("refCol1");

    String result = childColRef.toString();
    assertTrue(result.contains("ChildColRef"));
    assertTrue(result.contains("colName='col1'"));
    assertTrue(result.contains("refDbName='db1'"));
    assertTrue(result.contains("refTableName='table1'"));
    assertTrue(result.contains("refColName='refCol1'"));
  }

  @Test
  public void testToStringWithNullValues() {
    ChildColRef ref = new ChildColRef(null, null, null, null);
    String result = ref.toString();
    assertTrue(result.contains("ChildColRef"));
    assertTrue(result.contains("colName='null'") || result.contains("colName=null"));
    assertTrue(result.contains("refDbName='null'") || result.contains("refDbName=null"));
    assertTrue(result.contains("refTableName='null'") || result.contains("refTableName=null"));
    assertTrue(result.contains("refColName='null'") || result.contains("refColName=null"));
  }
}
