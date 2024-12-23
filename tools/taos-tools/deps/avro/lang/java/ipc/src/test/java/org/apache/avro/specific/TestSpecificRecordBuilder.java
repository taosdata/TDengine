/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Foo;
import org.apache.avro.Interop;
import org.apache.avro.Kind;
import org.apache.avro.MD5;
import org.apache.avro.Node;
import org.apache.avro.ipc.specific.PageView;
import org.apache.avro.ipc.specific.Person;
import org.apache.avro.ipc.specific.ProductPage;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit test for the SpecificRecordBuilder class.
 */
public class TestSpecificRecordBuilder {
  @Test
  public void testSpecificBuilder() {
    // Create a new builder, and leave some fields with default values empty:
    Person.Builder builder = Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA");
    Assert.assertTrue(builder.hasName());
    Assert.assertEquals("James Gosling", builder.getName());
    Assert.assertTrue(builder.hasYearOfBirth());
    Assert.assertEquals(1955, builder.getYearOfBirth());
    Assert.assertFalse(builder.hasCountry());
    Assert.assertNull(builder.getCountry());
    Assert.assertTrue(builder.hasState());
    Assert.assertEquals("CA", builder.getState());
    Assert.assertFalse(builder.hasFriends());
    Assert.assertNull(builder.getFriends());
    Assert.assertFalse(builder.hasLanguages());
    Assert.assertNull(builder.getLanguages());

    Person person = builder.build();
    Assert.assertEquals("James Gosling", person.getName());
    Assert.assertEquals(1955, person.getYearOfBirth());
    Assert.assertEquals("US", person.getCountry()); // country should default to "US"
    Assert.assertEquals("CA", person.getState());
    Assert.assertNotNull(person.getFriends()); // friends should default to an empty list
    Assert.assertEquals(0, person.getFriends().size());
    Assert.assertNotNull(person.getLanguages()); // Languages should now be "English" and "Java"
    Assert.assertEquals(2, person.getLanguages().size());
    Assert.assertEquals("English", person.getLanguages().get(0));
    Assert.assertEquals("Java", person.getLanguages().get(1));

    // Test copy constructors:
    Assert.assertEquals(builder, Person.newBuilder(builder));
    Assert.assertEquals(person, Person.newBuilder(person).build());

    Person.Builder builderCopy = Person.newBuilder(person);
    Assert.assertEquals("James Gosling", builderCopy.getName());
    Assert.assertEquals(1955, builderCopy.getYearOfBirth());
    Assert.assertEquals("US", builderCopy.getCountry()); // country should default to "US"
    Assert.assertEquals("CA", builderCopy.getState());
    Assert.assertNotNull(builderCopy.getFriends()); // friends should default to an empty list
    Assert.assertEquals(0, builderCopy.getFriends().size());

    // Test clearing fields:
    builderCopy.clearFriends().clearCountry();
    Assert.assertFalse(builderCopy.hasFriends());
    Assert.assertFalse(builderCopy.hasCountry());
    Assert.assertNull(builderCopy.getFriends());
    Assert.assertNull(builderCopy.getCountry());
    Person person2 = builderCopy.build();
    Assert.assertNotNull(person2.getFriends());
    Assert.assertTrue(person2.getFriends().isEmpty());
  }

  @Test
  public void testUnions() {
    long datetime = 1234L;
    String product = "widget";
    PageView p = PageView.newBuilder().setDatetime(1234L)
        .setPageContext(ProductPage.newBuilder().setProduct(product).build()).build();
    Assert.assertEquals(datetime, p.getDatetime());
    Assert.assertEquals(ProductPage.class, p.getPageContext().getClass());
    Assert.assertEquals(product, ((ProductPage) p.getPageContext()).getProduct());

    PageView p2 = PageView.newBuilder(p).build();

    Assert.assertEquals(datetime, p2.getDatetime());
    Assert.assertEquals(ProductPage.class, p2.getPageContext().getClass());
    Assert.assertEquals(product, ((ProductPage) p2.getPageContext()).getProduct());

    Assert.assertEquals(p, p2);

  }

  @Test
  public void testInterop() {
    Interop interop = Interop.newBuilder().setNullField(null).setArrayField(Arrays.asList(3.14159265, 6.022))
        .setBoolField(true).setBytesField(ByteBuffer.allocate(4).put(new byte[] { 3, 2, 1, 0 })).setDoubleField(1.41421)
        .setEnumField(Kind.C).setFixedField(new MD5(new byte[] { 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3 }))
        .setFloatField(1.61803f).setIntField(64).setLongField(1024)
        .setMapField(Collections.singletonMap("Foo1", new Foo())).setRecordField(new Node()).setStringField("MyInterop")
        .setUnionField(2.71828).build();

    Interop copy = Interop.newBuilder(interop).build();
    Assert.assertEquals(interop.getArrayField().size(), copy.getArrayField().size());
    Assert.assertEquals(interop.getArrayField(), copy.getArrayField());
    Assert.assertEquals(interop.getBoolField(), copy.getBoolField());
    Assert.assertEquals(interop.getBytesField(), copy.getBytesField());
    Assert.assertEquals(interop.getDoubleField(), copy.getDoubleField(), 0.001);
    Assert.assertEquals(interop.getEnumField(), copy.getEnumField());
    Assert.assertEquals(interop.getFixedField(), copy.getFixedField());
    Assert.assertEquals(interop.getFloatField(), copy.getFloatField(), 0.001);
    Assert.assertEquals(interop.getIntField(), copy.getIntField());
    Assert.assertEquals(interop.getLongField(), copy.getLongField());
    Assert.assertEquals(interop.getMapField(), copy.getMapField());
    Assert.assertEquals(interop.getRecordField(), copy.getRecordField());
    Assert.assertEquals(interop.getStringField(), copy.getStringField());
    Assert.assertEquals(interop.getUnionField(), copy.getUnionField());
    Assert.assertEquals(interop, copy);
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void attemptToSetNonNullableFieldToNull() {
    Person.newBuilder().setName(null);
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void buildWithoutSettingRequiredFields1() {
    Person.newBuilder().build();
  }

  @Test
  public void buildWithoutSettingRequiredFields2() {
    // Omit required non-primitive field
    try {
      Person.newBuilder().setYearOfBirth(1900).setState("MA").build();
      Assert.fail("Should have thrown " + AvroRuntimeException.class.getCanonicalName());
    } catch (AvroRuntimeException e) {
      // Exception should mention that the 'name' field has not been set
      Assert.assertTrue(e.getMessage().contains("name"));
    }
  }

  @Test
  public void buildWithoutSettingRequiredFields3() {
    // Omit required primitive field
    try {
      Person.newBuilder().setName("Anon").setState("CA").build();
      Assert.fail("Should have thrown " + AvroRuntimeException.class.getCanonicalName());
    } catch (AvroRuntimeException e) {
      // Exception should mention that the 'year_of_birth' field has not been set
      Assert.assertTrue(e.getMessage().contains("year_of_birth"));
    }
  }

  @Ignore
  @Test
  public void testBuilderPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<>(0);
    List<String> languages = new ArrayList<>(Arrays.asList("English", "Java"));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setCountry("US").setState("CA")
          .setFriends(friends).setLanguages(languages).build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }

  @Ignore
  @Test
  public void testBuilderPerformanceWithDefaultValues() {
    int count = 1000000;
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person.newBuilder().setName("James Gosling").setYearOfBirth(1955).setState("CA").build();
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }

  @Ignore
  @Test
  @SuppressWarnings("deprecation")
  public void testManualBuildPerformance() {
    int count = 1000000;
    List<Person> friends = new ArrayList<>(0);
    List<String> languages = new ArrayList<>(Arrays.asList("English", "Java"));
    long startTimeNanos = System.nanoTime();
    for (int ii = 0; ii < count; ii++) {
      Person person = new Person();
      person.setName("James Gosling");
      person.setYearOfBirth(1955);
      person.setState("CA");
      person.setCountry("US");
      person.setFriends(friends);
      person.setLanguages(languages);
    }
    long durationNanos = System.nanoTime() - startTimeNanos;
    double durationMillis = durationNanos / 1e6d;
    System.out.println("Built " + count + " records in " + durationMillis + "ms (" + (count / (durationMillis / 1000d))
        + " records/sec, " + (durationMillis / count) + "ms/record");
  }
}
