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
package org.apache.avro.compiler.specific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.avro.AvroTypeException;

import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class TestSpecificCompiler {
  private static final Logger LOG = LoggerFactory.getLogger(TestSpecificCompiler.class);

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  @Rule
  public TestName name = new TestName();

  private File outputFile;

  @Before
  public void setUp() {
    this.outputFile = new File(this.OUTPUT_DIR.getRoot(), "SimpleRecord.java");
  }

  private File src = new File("src/test/resources/simple_record.avsc");

  static void assertCompilesWithJavaCompiler(File dstDir, Collection<SpecificCompiler.OutputFile> outputs)
      throws IOException {
    assertCompilesWithJavaCompiler(dstDir, outputs, false);
  }

  /**
   * Uses the system's java compiler to actually compile the generated code.
   */
  static void assertCompilesWithJavaCompiler(File dstDir, Collection<SpecificCompiler.OutputFile> outputs,
      boolean ignoreWarnings) throws IOException {
    if (outputs.isEmpty()) {
      return; // Nothing to compile!
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);

    List<File> javaFiles = new ArrayList<>();
    for (SpecificCompiler.OutputFile o : outputs) {
      javaFiles.add(o.writeToDestination(null, dstDir));
    }

    final List<Diagnostic<?>> warnings = new ArrayList<>();
    DiagnosticListener<JavaFileObject> diagnosticListener = diagnostic -> {
      switch (diagnostic.getKind()) {
      case ERROR:
        // Do not add these to warnings because they will fail the compile, anyway.
        LOG.error("{}", diagnostic);
        break;
      case WARNING:
      case MANDATORY_WARNING:
        LOG.warn("{}", diagnostic);
        warnings.add(diagnostic);
        break;
      case NOTE:
      case OTHER:
        LOG.debug("{}", diagnostic);
        break;
      }
    };
    JavaCompiler.CompilationTask cTask = compiler.getTask(null, fileManager, diagnosticListener,
        Collections.singletonList("-Xlint:all"), null, fileManager.getJavaFileObjects(javaFiles.toArray(new File[0])));
    boolean compilesWithoutError = cTask.call();
    assertTrue(compilesWithoutError);
    if (!ignoreWarnings) {
      assertEquals("Warnings produced when compiling generated code with -Xlint:all", 0, warnings.size());
    }
  }

  private static Schema createSampleRecordSchema(int numStringFields, int numDoubleFields) {
    SchemaBuilder.FieldAssembler<Schema> sb = SchemaBuilder.record("sample.record").fields();
    for (int i = 0; i < numStringFields; i++) {
      sb.name("sf_" + i).type().stringType().noDefault();
    }
    for (int i = 0; i < numDoubleFields; i++) {
      sb.name("df_" + i).type().doubleType().noDefault();
    }
    return sb.endRecord();
  }

  private SpecificCompiler createCompiler() throws IOException {
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(this.src);
    SpecificCompiler compiler = new SpecificCompiler(schema);
    String velocityTemplateDir = "src/main/velocity/org/apache/avro/compiler/specific/templates/java/classic/";
    compiler.setTemplateDir(velocityTemplateDir);
    compiler.setStringType(StringType.CharSequence);
    return compiler;
  }

  @Test
  public void testCanReadTemplateFilesOnTheFilesystem() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(new File(OUTPUT_DIR.getRoot(), "SimpleRecord.java").exists());
  }

  @Test
  public void testPublicFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PUBLIC);
    assertTrue(compiler.publicFields());
    assertFalse(compiler.privateFields());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // No line, once trimmed, should start with a deprecated field declaration
        // nor a private field declaration. Since the nested builder uses private
        // fields, we cannot do the second check.
        line = line.trim();
        assertFalse("Line started with a deprecated field declaration: " + line,
            line.startsWith("@Deprecated public int value"));
      }
    }
  }

  @Test
  public void testCreateAllArgsConstructor() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    boolean foundAllArgsConstructor = false;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while (!foundAllArgsConstructor && (line = reader.readLine()) != null) {
        foundAllArgsConstructor = line.contains("All-args constructor");
      }
    }
    assertTrue(foundAllArgsConstructor);
  }

  @Test
  public void testMaxValidParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), name.getMethodName() + "1"),
        new SpecificCompiler(validSchema1).compile());

    createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), name.getMethodName() + "2"),
        new SpecificCompiler(validSchema1).compile());
  }

  @Test
  public void testInvalidParameterCounts() throws Exception {
    Schema invalidSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    SpecificCompiler compiler = new SpecificCompiler(invalidSchema1);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), name.getMethodName() + "1"), compiler.compile());

    Schema invalidSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 10);
    compiler = new SpecificCompiler(invalidSchema2);
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), name.getMethodName() + "2"), compiler.compile());
  }

  @Test
  public void testMaxParameterCounts() throws Exception {
    Schema validSchema1 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT, 0);
    assertTrue(new SpecificCompiler(validSchema1).compile().size() > 0);

    Schema validSchema2 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 2, 1);
    assertTrue(new SpecificCompiler(validSchema2).compile().size() > 0);

    Schema validSchema3 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT - 1, 1);
    assertTrue(new SpecificCompiler(validSchema3).compile().size() > 0);

    Schema validSchema4 = createSampleRecordSchema(SpecificCompiler.MAX_FIELD_PARAMETER_UNIT_COUNT + 1, 0);
    assertTrue(new SpecificCompiler(validSchema4).compile().size() > 0);
  }

  @Test(expected = RuntimeException.class)
  public void testCalcAllArgConstructorParameterUnitsFailure() {
    Schema nonRecordSchema = SchemaBuilder.array().items().booleanType();
    new SpecificCompiler().calcAllArgConstructorParameterUnits(nonRecordSchema);
  }

  @Test
  public void testPrivateFieldVisibility() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setFieldVisibility(SpecificCompiler.FieldVisibility.PRIVATE);
    assertFalse(compiler.publicFields());
    assertTrue(compiler.privateFields());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        // No line, once trimmed, should start with a public field declaration
        // or with a deprecated public field declaration
        line = line.trim();
        assertFalse("Line started with a public field declaration: " + line, line.startsWith("public int value"));
        assertFalse("Line started with a deprecated field declaration: " + line,
            line.startsWith("@Deprecated public int value"));
      }
    }
  }

  @Test
  public void testSettersCreatedByDefault() throws IOException {
    SpecificCompiler compiler = createCompiler();
    assertTrue(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    int foundSetters = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // We should find the setter in the main class
        line = line.trim();
        if (line.startsWith("public void setValue(")) {
          foundSetters++;
        }
      }
    }
    assertEquals("Found the wrong number of setters", 1, foundSetters);
  }

  @Test
  public void testSettersNotCreatedWhenOptionTurnedOff() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setCreateSetters(false);
    assertFalse(compiler.isCreateSetters());
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // No setter should be found
        line = line.trim();
        assertFalse("No line should include the setter: " + line, line.startsWith("public void setValue("));
      }
    }
  }

  @Test
  public void testSettingOutputCharacterEncoding() throws Exception {
    SpecificCompiler compiler = createCompiler();
    // Generated file in default encoding
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    byte[] fileInDefaultEncoding = new byte[(int) this.outputFile.length()];
    FileInputStream is = new FileInputStream(this.outputFile);
    is.read(fileInDefaultEncoding);
    is.close(); // close input stream otherwise delete might fail
    if (!this.outputFile.delete()) {
      throw new IllegalStateException("unable to delete " + this.outputFile); // delete otherwise compiler might not
                                                                              // overwrite because src timestamp hasn't
                                                                              // changed.
    }
    // Generate file in another encoding (make sure it has different number of bytes
    // per character)
    String differentEncoding = Charset.defaultCharset().equals(Charset.forName("UTF-16")) ? "UTF-32" : "UTF-16";
    compiler.setOutputCharacterEncoding(differentEncoding);
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    byte[] fileInDifferentEncoding = new byte[(int) this.outputFile.length()];
    is = new FileInputStream(this.outputFile);
    is.read(fileInDifferentEncoding);
    is.close();
    // Compare as bytes
    assertThat("Generated file should contain different bytes after setting non-default encoding",
        fileInDefaultEncoding, not(equalTo(fileInDifferentEncoding)));
    // Compare as strings
    assertThat("Generated files should contain the same characters in the proper encodings",
        new String(fileInDefaultEncoding), equalTo(new String(fileInDifferentEncoding, differentEncoding)));
  }

  @Test
  public void testJavaTypeWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema localTimestampSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    Assert.assertEquals("Should use LocalDate for date type", "java.time.LocalDate", compiler.javaType(dateSchema));
    Assert.assertEquals("Should use LocalTime for time-millis type", "java.time.LocalTime",
        compiler.javaType(timeSchema));
    Assert.assertEquals("Should use DateTime for timestamp-millis type", "java.time.Instant",
        compiler.javaType(timestampSchema));
    Assert.assertEquals("Should use LocalDateTime for local-timestamp-millis type", "java.time.LocalDateTime",
        compiler.javaType(localTimestampSchema));
    Assert.assertEquals("Should use Java BigDecimal type", "java.math.BigDecimal", compiler.javaType(decimalSchema));
    Assert.assertEquals("Should use Java CharSequence type", "java.lang.CharSequence", compiler.javaType(uuidSchema));
  }

  @Test
  public void testJavaTypeWithDecimalLogicalTypeDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    // Date/time types should always use upper level java classes
    // Decimal type target class depends on configuration
    // UUID should always be CharSequence since we haven't added its
    // support in SpecificRecord
    Assert.assertEquals("Should use LocalDate for date type", "java.time.LocalDate", compiler.javaType(dateSchema));
    Assert.assertEquals("Should use LocalTime for time-millis type", "java.time.LocalTime",
        compiler.javaType(timeSchema));
    Assert.assertEquals("Should use DateTime for timestamp-millis type", "java.time.Instant",
        compiler.javaType(timestampSchema));
    Assert.assertEquals("Should use ByteBuffer type", "java.nio.ByteBuffer", compiler.javaType(decimalSchema));
    Assert.assertEquals("Should use Java CharSequence type", "java.lang.CharSequence", compiler.javaType(uuidSchema));
  }

  @Test
  public void testJavaTypeWithDateTimeTypes() throws Exception {
    SpecificCompiler compiler = createCompiler();

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicrosSchema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicrosSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    // Date/time types should always use upper level java classes
    Assert.assertEquals("Should use java.time.LocalDate for date type", "java.time.LocalDate",
        compiler.javaType(dateSchema));
    Assert.assertEquals("Should use java.time.LocalTime for time-millis type", "java.time.LocalTime",
        compiler.javaType(timeSchema));
    Assert.assertEquals("Should use java.time.Instant for timestamp-millis type", "java.time.Instant",
        compiler.javaType(timestampSchema));
    Assert.assertEquals("Should use java.time.LocalTime for time-micros type", "java.time.LocalTime",
        compiler.javaType(timeMicrosSchema));
    Assert.assertEquals("Should use java.time.Instant for timestamp-micros type", "java.time.Instant",
        compiler.javaType(timestampMicrosSchema));
  }

  @Test
  public void testJavaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema boolSchema = Schema.create(Schema.Type.BOOLEAN);
    Assert.assertEquals("Should use int for Type.INT", "int", compiler.javaUnbox(intSchema, false));
    Assert.assertEquals("Should use long for Type.LONG", "long", compiler.javaUnbox(longSchema, false));
    Assert.assertEquals("Should use float for Type.FLOAT", "float", compiler.javaUnbox(floatSchema, false));
    Assert.assertEquals("Should use double for Type.DOUBLE", "double", compiler.javaUnbox(doubleSchema, false));
    Assert.assertEquals("Should use boolean for Type.BOOLEAN", "boolean", compiler.javaUnbox(boolSchema, false));

    // see AVRO-2569
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Assert.assertEquals("Should use void for Type.NULL", "void", compiler.javaUnbox(nullSchema, true));

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // Date/time types should always use upper level java classes, even though
    // their underlying representations are primitive types
    Assert.assertEquals("Should use LocalDate for date type", "java.time.LocalDate",
        compiler.javaUnbox(dateSchema, false));
    Assert.assertEquals("Should use LocalTime for time-millis type", "java.time.LocalTime",
        compiler.javaUnbox(timeSchema, false));
    Assert.assertEquals("Should use DateTime for timestamp-millis type", "java.time.Instant",
        compiler.javaUnbox(timestampSchema, false));
  }

  @Test
  public void testJavaUnboxDateTime() throws Exception {
    SpecificCompiler compiler = createCompiler();

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    // Date/time types should always use upper level java classes, even though
    // their underlying representations are primitive types
    Assert.assertEquals("Should use java.time.LocalDate for date type", "java.time.LocalDate",
        compiler.javaUnbox(dateSchema, false));
    Assert.assertEquals("Should use java.time.LocalTime for time-millis type", "java.time.LocalTime",
        compiler.javaUnbox(timeSchema, false));
    Assert.assertEquals("Should use java.time.Instant for timestamp-millis type", "java.time.Instant",
        compiler.javaUnbox(timestampSchema, false));
  }

  @Test
  public void testNullableLogicalTypesJavaUnboxDecimalTypesEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    // Nullable types should return boxed types instead of primitive types
    Schema nullableDecimalSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema nullableDecimalSchema2 = Schema.createUnion(
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDecimalSchema1, false),
        "java.math.BigDecimal");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDecimalSchema2, false),
        "java.math.BigDecimal");
  }

  @Test
  public void testNullableLogicalTypesJavaUnboxDecimalTypesDisabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    // Since logical decimal types are disabled, a ByteBuffer is expected.
    Schema nullableDecimalSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema nullableDecimalSchema2 = Schema.createUnion(
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDecimalSchema1, false),
        "java.nio.ByteBuffer");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDecimalSchema2, false),
        "java.nio.ByteBuffer");
  }

  @Test
  public void testNullableTypesJavaUnbox() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    // Nullable types should return boxed types instead of primitive types
    Schema nullableIntSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    Schema nullableIntSchema2 = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableIntSchema1, false), "java.lang.Integer");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableIntSchema2, false), "java.lang.Integer");

    Schema nullableLongSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG));
    Schema nullableLongSchema2 = Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableLongSchema1, false), "java.lang.Long");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableLongSchema2, false), "java.lang.Long");

    Schema nullableFloatSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT));
    Schema nullableFloatSchema2 = Schema.createUnion(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableFloatSchema1, false), "java.lang.Float");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableFloatSchema2, false), "java.lang.Float");

    Schema nullableDoubleSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.DOUBLE));
    Schema nullableDoubleSchema2 = Schema.createUnion(Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDoubleSchema1, false),
        "java.lang.Double");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableDoubleSchema2, false),
        "java.lang.Double");

    Schema nullableBooleanSchema1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN));
    Schema nullableBooleanSchema2 = Schema.createUnion(Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.NULL));
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableBooleanSchema1, false),
        "java.lang.Boolean");
    Assert.assertEquals("Should return boxed type", compiler.javaUnbox(nullableBooleanSchema2, false),
        "java.lang.Boolean");
  }

  @Test
  public void testGetUsedCustomLogicalTypeFactories() throws Exception {
    LogicalTypes.register("string-custom", new StringCustomLogicalTypeFactory());

    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    final Schema schema = new Schema.Parser().parse("{\"type\":\"record\"," + "\"name\":\"NestedLogicalTypesRecord\","
        + "\"namespace\":\"org.apache.avro.codegentest.testdata\","
        + "\"doc\":\"Test nested types with logical types in generated Java classes\"," + "\"fields\":["
        + "{\"name\":\"nestedRecord\",\"type\":" + "{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":"
        + "[{\"name\":\"nullableDateField\"," + "\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}},"
        + "{\"name\":\"myLogical\",\"type\":{\"type\":\"string\",\"logicalType\":\"string-custom\"}}]}");

    final Map<String, String> usedCustomLogicalTypeFactories = compiler.getUsedCustomLogicalTypeFactories(schema);
    Assert.assertEquals(1, usedCustomLogicalTypeFactories.size());
    final Map.Entry<String, String> entry = usedCustomLogicalTypeFactories.entrySet().iterator().next();
    Assert.assertEquals("string-custom", entry.getKey());
    Assert.assertEquals("org.apache.avro.compiler.specific.TestSpecificCompiler.StringCustomLogicalTypeFactory",
        entry.getValue());
  }

  @Test
  public void testEmptyGetUsedCustomLogicalTypeFactories() throws Exception {
    LogicalTypes.register("string-custom", new StringCustomLogicalTypeFactory());

    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    final Schema schema = new Schema.Parser().parse("{\"type\":\"record\"," + "\"name\":\"NestedLogicalTypesRecord\","
        + "\"namespace\":\"org.apache.avro.codegentest.testdata\","
        + "\"doc\":\"Test nested types with logical types in generated Java classes\"," + "\"fields\":["
        + "{\"name\":\"nestedRecord\"," + "\"type\":{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":"
        + "[{\"name\":\"nullableDateField\","
        + "\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}]}");

    final Map<String, String> usedCustomLogicalTypeFactories = compiler.getUsedCustomLogicalTypeFactories(schema);
    Assert.assertEquals(0, usedCustomLogicalTypeFactories.size());
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypes() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema nullableDecimal1 = Schema.createUnion(Schema.create(Schema.Type.NULL),
        LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES)));
    Schema schemaWithNullableDecimal1 = Schema.createRecord("WithNullableDecimal", "", "", false,
        Collections.singletonList(new Schema.Field("decimal", nullableDecimal1, "", null)));

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schemaWithNullableDecimal1);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.Conversions.DecimalConversion", usedConversionClasses.iterator().next());
  }

  @Test
  public void testGetUsedConversionClassesForNullableTimestamps() throws Exception {
    SpecificCompiler compiler = createCompiler();

    // timestamp-millis and timestamp-micros used to cause collisions when both were
    // present or added as converters (AVRO-2481).
    final Schema tsMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    final Schema tsMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    final Collection<String> conversions = compiler.getUsedConversionClasses(SchemaBuilder.record("WithTimestamps")
        .fields().name("tsMillis").type(tsMillis).noDefault().name("tsMillisOpt").type().unionOf().nullType().and()
        .type(tsMillis).endUnion().noDefault().name("tsMicros").type(tsMicros).noDefault().name("tsMicrosOpt").type()
        .unionOf().nullType().and().type(tsMicros).endUnion().noDefault().endRecord());

    Assert.assertEquals(2, conversions.size());
    assertThat(conversions, hasItem("org.apache.avro.data.TimeConversions.TimestampMillisConversion"));
    assertThat(conversions, hasItem("org.apache.avro.data.TimeConversions.TimestampMicrosConversion"));
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypesInNestedRecord() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesRecord\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"nestedRecord\",\"type\":{\"type\":\"record\",\"name\":\"NestedRecord\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypesInArray() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NullableLogicalTypesArray\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"arrayOfLogicalType\",\"type\":{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypesInArrayOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesArray\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"arrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"RecordInArray\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypesInUnionOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesUnion\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"unionOfRecords\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"RecordInUnion\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]}]}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  @Test
  public void testGetUsedConversionClassesForNullableLogicalTypesInMapOfRecords() throws Exception {
    SpecificCompiler compiler = createCompiler();

    final Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"NestedLogicalTypesMap\",\"namespace\":\"org.apache.avro.codegentest.testdata\",\"doc\":\"Test nested types with logical types in generated Java classes\",\"fields\":[{\"name\":\"mapOfRecords\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"RecordInMap\",\"fields\":[{\"name\":\"nullableDateField\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]}]},\"avro.java.string\":\"String\"}}]}");

    final Collection<String> usedConversionClasses = compiler.getUsedConversionClasses(schema);
    Assert.assertEquals(1, usedConversionClasses.size());
    Assert.assertEquals("org.apache.avro.data.TimeConversions.DateConversion", usedConversionClasses.iterator().next());
  }

  /**
   * Checks that identifiers that may cause problems in Java code will compile
   * correctly when used in a generated specific record.
   *
   * @param schema                         A schema with an identifier __test__
   *                                       that will be replaced.
   * @param throwsTypeExceptionOnPrimitive If true, using a reserved word that is
   *                                       also an Avro primitive type name must
   *                                       throw an exception instead of
   *                                       generating code.
   * @param dstDirPrefix                   Where to generate the java code before
   *                                       compiling.
   */
  public void testManglingReservedIdentifiers(String schema, boolean throwsTypeExceptionOnPrimitive,
      String dstDirPrefix) throws IOException {
    for (String reserved : SpecificData.RESERVED_WORDS) {
      try {
        Schema s = new Schema.Parser().parse(schema.replace("__test__", reserved));
        assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), dstDirPrefix + "_" + reserved),
            new SpecificCompiler(s).compile());
      } catch (AvroTypeException e) {
        if (!(throwsTypeExceptionOnPrimitive && e.getMessage().contains("Schemas may not be named after primitives")))
          throw e;
      }
    }
  }

  @Test
  public void testMangleRecordName() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("__test__").fields().requiredInt("field").endRecord().toString(), true,
        name.getMethodName());
  }

  @Test
  public void testMangleRecordNamespace() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("__test__.Record").fields().requiredInt("field").endRecord().toString(), false,
        name.getMethodName());
  }

  @Test
  public void testMangleField() throws Exception {
    testManglingReservedIdentifiers(
        SchemaBuilder.record("Record").fields().requiredInt("__test__").endRecord().toString(), false,
        name.getMethodName());
  }

  @Test
  public void testMangleEnumName() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.enumeration("__test__").symbols("reserved").toString(), true,
        name.getMethodName());
  }

  @Test
  public void testMangleEnumSymbol() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.enumeration("Enum").symbols("__test__").toString(), false,
        name.getMethodName());
  }

  @Test
  public void testMangleFixedName() throws Exception {
    testManglingReservedIdentifiers(SchemaBuilder.fixed("__test__").size(2).toString(), true, name.getMethodName());
  }

  @Test
  public void testLogicalTypesWithMultipleFields() throws Exception {
    Schema logicalTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/logical_types_with_multiple_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(OUTPUT_DIR.getRoot(), name.getMethodName()),
        new SpecificCompiler(logicalTypesWithMultipleFields).compile(), true);
  }

  @Test
  public void testUnionAndFixedFields() throws Exception {
    Schema unionTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/union_and_fixed_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(this.outputFile, name.getMethodName()),
        new SpecificCompiler(unionTypesWithMultipleFields).compile());
  }

  @Test
  public void testLogicalTypesWithMultipleFieldsDateTime() throws Exception {
    Schema logicalTypesWithMultipleFields = new Schema.Parser()
        .parse(new File("src/test/resources/logical_types_with_multiple_fields.avsc"));
    assertCompilesWithJavaCompiler(new File(this.outputFile, name.getMethodName()),
        new SpecificCompiler(logicalTypesWithMultipleFields).compile());
  }

  @Test
  public void testConversionInstanceWithDecimalLogicalTypeDisabled() throws Exception {
    final SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(false);

    final Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    final Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    final Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    final Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    final Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    Assert.assertEquals("Should use date conversion for date type",
        "new org.apache.avro.data.TimeConversions.DateConversion()", compiler.conversionInstance(dateSchema));
    Assert.assertEquals("Should use time conversion for time type",
        "new org.apache.avro.data.TimeConversions.TimeMillisConversion()", compiler.conversionInstance(timeSchema));
    Assert.assertEquals("Should use timestamp conversion for date type",
        "new org.apache.avro.data.TimeConversions.TimestampMillisConversion()",
        compiler.conversionInstance(timestampSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off", "null",
        compiler.conversionInstance(decimalSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off", "null",
        compiler.conversionInstance(uuidSchema));
  }

  @Test
  public void testConversionInstanceWithDecimalLogicalTypeEnabled() throws Exception {
    SpecificCompiler compiler = createCompiler();
    compiler.setEnableDecimalLogicalType(true);

    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema decimalSchema = LogicalTypes.decimal(9, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));

    Assert.assertEquals("Should use date conversion for date type",
        "new org.apache.avro.data.TimeConversions.DateConversion()", compiler.conversionInstance(dateSchema));
    Assert.assertEquals("Should use time conversion for time type",
        "new org.apache.avro.data.TimeConversions.TimeMillisConversion()", compiler.conversionInstance(timeSchema));
    Assert.assertEquals("Should use timestamp conversion for date type",
        "new org.apache.avro.data.TimeConversions.TimestampMillisConversion()",
        compiler.conversionInstance(timestampSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off",
        "new org.apache.avro.Conversions.DecimalConversion()", compiler.conversionInstance(decimalSchema));
    Assert.assertEquals("Should use null for decimal if the flag is off", "null",
        compiler.conversionInstance(uuidSchema));
  }

  @Test
  public void testPojoWithOptionalTurnedOffByDefault() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        assertFalse(line.contains("Optional"));
      }
    }
  }

  @Test
  public void testPojoWithOptionalCreatedWhenOptionTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setGettersReturnOptional(true);
    // compiler.setCreateOptionalGetters(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(9, optionalFound);
  }

  @Test
  public void testPojoWithOptionalCreateForNullableFieldsWhenOptionTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setGettersReturnOptional(true);
    compiler.setOptionalGettersForNullableFieldsOnly(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(5, optionalFound);
  }

  @Test
  public void testPojoWithOptionalCreatedWhenOptionalForEverythingTurnedOn() throws IOException {
    SpecificCompiler compiler = createCompiler();
    // compiler.setGettersReturnOptional(true);
    compiler.setCreateOptionalGetters(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    int optionalFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("Optional")) {
          optionalFound++;
        }
      }
    }
    assertEquals(17, optionalFound);
  }

  @Test
  public void testPojoWithOptionalOnlyWhenNullableCreatedTurnedOnAndGettersReturnOptionalTurnedOff()
      throws IOException {
    SpecificCompiler compiler = createCompiler();
    compiler.setOptionalGettersForNullableFieldsOnly(true);
    compiler.compileToDestination(this.src, OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        // no optionals since gettersReturnOptionalOnlyForNullable is false
        assertFalse(line.contains("Optional"));
      }
    }
  }

  @Test
  public void testAdditionalToolsAreInjectedIntoTemplate() throws Exception {
    SpecificCompiler compiler = createCompiler();
    List<Object> customTools = new ArrayList<>();
    customTools.add(new String());
    compiler.setAdditionalVelocityTools(customTools);
    compiler.setTemplateDir("src/test/resources/templates_with_custom_tools/");
    compiler.compileToDestination(this.src, this.OUTPUT_DIR.getRoot());
    assertTrue(this.outputFile.exists());
    int itWorksFound = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(this.outputFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.contains("It works!")) {
          itWorksFound++;
        }
      }
    }
    assertEquals(1, itWorksFound);
  }

  public static class StringCustomLogicalTypeFactory implements LogicalTypes.LogicalTypeFactory {
    @Override
    public LogicalType fromSchema(Schema schema) {
      return new LogicalType("string-custom");
    }
  }

}
