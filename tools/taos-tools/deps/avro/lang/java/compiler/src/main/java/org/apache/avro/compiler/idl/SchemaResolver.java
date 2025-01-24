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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.Schemas;

/**
 * Utility class to resolve schemas that are unavailable at the time they are
 * referenced in the IDL.
 */
final class SchemaResolver {

  private SchemaResolver() {
  }

  private static final String UR_SCHEMA_ATTR = "org.apache.avro.compiler.idl.unresolved.name";

  private static final String UR_SCHEMA_NAME = "UnresolvedSchema";

  private static final String UR_SCHEMA_NS = "org.apache.avro.compiler";

  /**
   * Create a schema to represent a "unresolved" schema. (used to represent a
   * schema where the definition is not known at the time) This concept might be
   * generalizable...
   *
   * @param name
   * @return
   */
  static Schema unresolvedSchema(final String name) {
    Schema schema = Schema.createRecord(UR_SCHEMA_NAME, "unresolved schema", UR_SCHEMA_NS, false,
        Collections.EMPTY_LIST);
    schema.addProp(UR_SCHEMA_ATTR, name);
    return schema;
  }

  /**
   * Is this a unresolved schema.
   *
   * @param schema
   * @return
   */
  static boolean isUnresolvedSchema(final Schema schema) {
    return (schema.getType() == Schema.Type.RECORD && schema.getProp(UR_SCHEMA_ATTR) != null
        && UR_SCHEMA_NAME.equals(schema.getName()) && UR_SCHEMA_NS.equals(schema.getNamespace()));
  }

  /**
   * get the unresolved schema name.
   *
   * @param schema
   * @return
   */
  static String getUnresolvedSchemaName(final Schema schema) {
    if (!isUnresolvedSchema(schema)) {
      throw new IllegalArgumentException("Not a unresolved schema: " + schema);
    }
    return schema.getProp(UR_SCHEMA_ATTR);
  }

  /**
   * Will clone the provided protocol while resolving all unreferenced schemas
   *
   * @param protocol
   * @return
   */
  static Protocol resolve(final Protocol protocol) {
    Protocol result = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
    final Collection<Schema> types = protocol.getTypes();
    // replace unresolved schemas.
    List<Schema> newSchemas = new ArrayList<>(types.size());
    IdentityHashMap<Schema, Schema> replacements = new IdentityHashMap<>();
    for (Schema schema : types) {
      newSchemas.add(Schemas.visit(schema, new ResolvingVisitor(schema, replacements, new SymbolTable(protocol))));
    }
    result.setTypes(newSchemas); // replace types with resolved ones

    // Resolve all schemas referenced by protocol Messages.
    for (Map.Entry<String, Protocol.Message> entry : protocol.getMessages().entrySet()) {
      Protocol.Message value = entry.getValue();
      Protocol.Message nvalue;
      if (value.isOneWay()) {
        Schema replacement = resolve(replacements, value.getRequest(), protocol);
        nvalue = result.createMessage(value.getName(), value.getDoc(), value, replacement);
      } else {
        Schema request = resolve(replacements, value.getRequest(), protocol);
        Schema response = resolve(replacements, value.getResponse(), protocol);
        Schema errors = resolve(replacements, value.getErrors(), protocol);
        nvalue = result.createMessage(value.getName(), value.getDoc(), value, request, response, errors);
      }
      result.getMessages().put(entry.getKey(), nvalue);
    }
    Schemas.copyProperties(protocol, result);
    return result;
  }

  private static Schema resolve(final IdentityHashMap<Schema, Schema> replacements, final Schema request,
      final Protocol protocol) {
    Schema replacement = replacements.get(request);
    if (replacement == null) {
      replacement = Schemas.visit(request, new ResolvingVisitor(request, replacements, new SymbolTable(protocol)));
    }
    return replacement;
  }

  private static class SymbolTable implements Function<String, Schema> {

    private final Protocol symbolTable;

    public SymbolTable(Protocol symbolTable) {
      this.symbolTable = symbolTable;
    }

    @Override
    public Schema apply(final String f) {
      return symbolTable.getType(f);
    }
  }

}
