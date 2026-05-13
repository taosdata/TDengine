package com.taosdata.jdbc.ws.tmq.meta;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class ColRefTest {
  private ColRef colRef;

  @Before
  public void setUp() {
    colRef = new ColRef();
  }

  @Test
  public void testDefaultConstructor() {
    ColRef ref = new ColRef();
    assertNull(ref.getRefDbName());
    assertNull(ref.getRefTableName());
    assertNull(ref.getRefColName());
  }

  @Test
  public void testParameterizedConstructor() {
    ColRef ref = new ColRef("db1", "table1", "refCol1");
    assertEquals("db1", ref.getRefDbName());
    assertEquals("table1", ref.getRefTableName());
    assertEquals("refCol1", ref.getRefColName());
  }

  @Test
  public void testRefDbNameGetterAndSetter() {
    colRef.setRefDbName("testDb");
    assertEquals("testDb", colRef.getRefDbName());

    colRef.setRefDbName(null);
    assertNull(colRef.getRefDbName());

    colRef.setRefDbName("");
    assertEquals("", colRef.getRefDbName());
  }

  @Test
  public void testRefTableNameGetterAndSetter() {
    colRef.setRefTableName("testTable");
    assertEquals("testTable", colRef.getRefTableName());

    colRef.setRefTableName(null);
    assertNull(colRef.getRefTableName());

    colRef.setRefTableName("");
    assertEquals("", colRef.getRefTableName());
  }

  @Test
  public void testRefColNameGetterAndSetter() {
    colRef.setRefColName("testRefCol");
    assertEquals("testRefCol", colRef.getRefColName());

    colRef.setRefColName(null);
    assertNull(colRef.getRefColName());

    colRef.setRefColName("");
    assertEquals("", colRef.getRefColName());
  }

  @Test
  public void testEqualsWithSameObject() {
    colRef.setRefDbName("db1");
    colRef.setRefTableName("table1");
    colRef.setRefColName("refCol1");

    assertEquals(colRef, colRef);
  }

  @Test
  public void testEqualsWithNull() {
    colRef.setRefDbName("db1");
    assertNotEquals(null, colRef);
  }

  @Test
  public void testEqualsWithDifferentClass() {
    colRef.setRefDbName("db1");
    assertNotEquals(colRef, new Object());
  }

  @Test
  public void testEqualsWithSameValues() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db1", "table1", "refCol1");

    assertEquals(ref1, ref2);
    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testEqualsWithDifferentRefDbName() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db2", "table1", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithDifferentRefTableName() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db1", "table2", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithDifferentRefColName() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db1", "table1", "refCol2");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testEqualsWithNullValues() {
    ColRef ref1 = new ColRef(null, null, null);
    ColRef ref2 = new ColRef(null, null, null);

    assertEquals(ref1, ref2);
    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testEqualsWithOneNullValue() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef(null, "table1", "refCol1");

    assertNotEquals(ref1, ref2);
  }

  @Test
  public void testHashCodeConsistency() {
    colRef.setRefDbName("db1");
    colRef.setRefTableName("table1");
    colRef.setRefColName("refCol1");

    int initialHashCode = colRef.hashCode();
    assertEquals(initialHashCode, colRef.hashCode());
  }

  @Test
  public void testHashCodeWithSameValues() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db1", "table1", "refCol1");

    assertEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentValues() {
    ColRef ref1 = new ColRef("db1", "table1", "refCol1");
    ColRef ref2 = new ColRef("db2", "table1", "refCol1");

    assertNotEquals(ref1.hashCode(), ref2.hashCode());
  }

  @Test
  public void testAllFieldsTogether() {
    colRef.setRefDbName("myDatabase");
    colRef.setRefTableName("myTable");
    colRef.setRefColName("myRefColumn");

    assertEquals("myDatabase", colRef.getRefDbName());
    assertEquals("myTable", colRef.getRefTableName());
    assertEquals("myRefColumn", colRef.getRefColName());
  }

  @Test
  public void testToString() {
    colRef.setRefDbName("db1");
    colRef.setRefTableName("table1");
    colRef.setRefColName("refCol1");

    String result = colRef.toString();
    assertTrue(result.contains("ColRef"));
    assertTrue(result.contains("refDbName='db1'"));
    assertTrue(result.contains("refTableName='table1'"));
    assertTrue(result.contains("refColName='refCol1'"));
  }

  @Test
  public void testToStringWithNullValues() {
    ColRef ref = new ColRef(null, null, null);
    String result = ref.toString();
    assertTrue(result.contains("ColRef"));
    assertTrue(result.contains("refDbName='null'") || result.contains("refDbName=null"));
    assertTrue(result.contains("refTableName='null'") || result.contains("refTableName=null"));
    assertTrue(result.contains("refColName='null'") || result.contains("refColName=null"));
  }
}
