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
package org.apache.avro.compiler.idl;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.compiler.schema.SchemaVisitor;
import org.apache.avro.compiler.schema.SchemaVisitorAction;
import org.apache.avro.compiler.schema.Schemas;

/**
 * this visitor will create a clone of the original Schema and will also resolve
 * all unresolved schemas
 *
 * by default. what attributes are copied is customizable.
 */
public final class ResolvingVisitor implements SchemaVisitor<Schema> {

  private final IdentityHashMap<Schema, Schema> replace;
  private final Function<String, Schema> symbolTable;

  private final Schema root;

  public ResolvingVisitor(final Schema root, final IdentityHashMap<Schema, Schema> replace,
      final Function<String, Schema> symbolTable) {
    this.replace = replace;
    this.symbolTable = symbolTable;
    this.root = root;
  }

  @Override
  public SchemaVisitorAction visitTerminal(final Schema terminal) {
    Schema.Type type = terminal.getType();
    Schema newSchema;
    switch (type) {
    case RECORD: // recursion.
    case ARRAY:
    case MAP:
    case UNION:
      if (!replace.containsKey(terminal)) {
        throw new IllegalStateException("Schema " + terminal + " must be already processed");
      }
      return SchemaVisitorAction.CONTINUE;
    case BOOLEAN:
    case BYTES:
    case DOUBLE:
    case FLOAT:
    case INT:
    case LONG:
    case NULL:
    case STRING:
      newSchema = Schema.create(type);
      break;
    case ENUM:
      newSchema = Schema.createEnum(terminal.getName(), terminal.getDoc(), terminal.getNamespace(),
          terminal.getEnumSymbols(), terminal.getEnumDefault());
      break;
    case FIXED:
      newSchema = Schema.createFixed(terminal.getName(), terminal.getDoc(), terminal.getNamespace(),
          terminal.getFixedSize());
      break;
    default:
      throw new IllegalStateException("Unsupported schema " + terminal);
    }
    copyAllProperties(terminal, newSchema);
    replace.put(terminal, newSchema);
    return SchemaVisitorAction.CONTINUE;
  }

  public static void copyAllProperties(final Schema first, final Schema second) {
    Schemas.copyLogicalTypes(first, second);
    Schemas.copyAliases(first, second);
    Schemas.copyProperties(first, second);
  }

  public static void copyAllProperties(final Field first, final Field second) {
    Schemas.copyAliases(first, second);
    Schemas.copyProperties(first, second);
  }

  @Override
  public SchemaVisitorAction visitNonTerminal(final Schema nt) {
    Schema.Type type = nt.getType();
    if (type == Schema.Type.RECORD) {
      if (SchemaResolver.isUnresolvedSchema(nt)) {
        // unresolved schema will get a replacement that we already encountered,
        // or we will attempt to resolve.
        final String unresolvedSchemaName = SchemaResolver.getUnresolvedSchemaName(nt);
        Schema resSchema = symbolTable.apply(unresolvedSchemaName);
        if (resSchema == null) {
          throw new AvroTypeException("Unable to resolve " + unresolvedSchemaName);
        }
        Schema replacement = replace.get(resSchema);
        if (replacement == null) {
          replace.put(nt,
              Schemas.visit(resSchema, new ResolvingVisitor(resSchema, new IdentityHashMap<>(), symbolTable)));
        } else {
          replace.put(nt, replacement);
        }
      } else {
        // create a fieldless clone. Fields will be added in afterVisitNonTerminal.
        Schema newSchema = Schema.createRecord(nt.getName(), nt.getDoc(), nt.getNamespace(), nt.isError());
        copyAllProperties(nt, newSchema);
        replace.put(nt, newSchema);
      }
    }
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public SchemaVisitorAction afterVisitNonTerminal(final Schema nt) {
    Schema.Type type = nt.getType();
    Schema newSchema;
    switch (type) {
    case RECORD:
      if (!SchemaResolver.isUnresolvedSchema(nt)) {
        newSchema = replace.get(nt);
        List<Schema.Field> fields = nt.getFields();
        List<Schema.Field> newFields = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
          Schema.Field newField = new Schema.Field(field.name(), replace.get(field.schema()), field.doc(),
              field.defaultVal(), field.order());
          copyAllProperties(field, newField);
          newFields.add(newField);
        }
        newSchema.setFields(newFields);
      }
      return SchemaVisitorAction.CONTINUE;
    case UNION:
      List<Schema> types = nt.getTypes();
      List<Schema> newTypes = new ArrayList<>(types.size());
      for (Schema sch : types) {
        newTypes.add(replace.get(sch));
      }
      newSchema = Schema.createUnion(newTypes);
      break;
    case ARRAY:
      newSchema = Schema.createArray(replace.get(nt.getElementType()));
      break;
    case MAP:
      newSchema = Schema.createMap(replace.get(nt.getValueType()));
      break;
    default:
      throw new IllegalStateException("Illegal type " + type + ", schema " + nt);
    }
    copyAllProperties(nt, newSchema);
    replace.put(nt, newSchema);
    return SchemaVisitorAction.CONTINUE;
  }

  @Override
  public Schema get() {
    return replace.get(root);
  }

  @Override
  public String toString() {
    return "ResolvingVisitor{" + "replace=" + replace + ", symbolTable=" + symbolTable + ", root=" + root + '}';
  }

}
