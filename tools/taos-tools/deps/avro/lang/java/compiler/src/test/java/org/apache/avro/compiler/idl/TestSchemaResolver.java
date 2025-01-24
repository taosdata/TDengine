/*
 * Copyright 2017 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.compiler.idl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaResolver {

  @Test
  public void testResolving() throws ParseException, MalformedURLException, IOException {
    File file = new File(".");
    String currentWorkPath = file.getAbsolutePath();
    String testIdl = currentWorkPath + File.separator + "src" + File.separator + "test" + File.separator + "idl"
        + File.separator + "cycle.avdl";
    Idl compiler = new Idl(new File(testIdl));
    Protocol protocol = compiler.CompilationUnit();
    System.out.println(protocol);
    Assert.assertEquals(5, protocol.getTypes().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError1() {
    // No "org.apache.avro.compiler.idl.unresolved.name" property
    Schema s = SchemaBuilder.record("R").fields().endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError2() {
    // No "UnresolvedSchema" property
    Schema s = SchemaBuilder.record("R").prop("org.apache.avro.compiler.idl.unresolved.name", "x").fields().endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsUnresolvedSchemaError3() {
    // Namespace not "org.apache.avro.compiler".
    Schema s = SchemaBuilder.record("UnresolvedSchema").prop("org.apache.avro.compiler.idl.unresolved.name", "x")
        .fields().endRecord();
    SchemaResolver.getUnresolvedSchemaName(s);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetUnresolvedSchemaNameError() {
    Schema s = SchemaBuilder.fixed("a").size(10);
    SchemaResolver.getUnresolvedSchemaName(s);
  }
}
