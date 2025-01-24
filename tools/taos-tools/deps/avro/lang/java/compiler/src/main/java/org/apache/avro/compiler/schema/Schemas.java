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
package org.apache.avro.compiler.schema;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.compiler.specific.SpecificCompiler;

/**
 * Avro Schema utilities, to traverse...
 */
public final class Schemas {

  private Schemas() {
  }

  public static void copyAliases(final Schema from, final Schema to) {
    switch (from.getType()) { // only named types.
    case RECORD:
    case ENUM:
    case FIXED:
      Set<String> aliases = from.getAliases();
      for (String alias : aliases) {
        to.addAlias(alias);
      }
    }
  }

  public static void copyAliases(final Schema.Field from, final Schema.Field to) {
    Set<String> aliases = from.aliases();
    for (String alias : aliases) {
      to.addAlias(alias);
    }
  }

  public static void copyLogicalTypes(final Schema from, final Schema to) {
    LogicalType logicalType = from.getLogicalType();
    if (logicalType != null) {
      logicalType.addToSchema(to);
    }
  }

  public static void copyProperties(final JsonProperties from, final JsonProperties to) {
    Map<String, Object> objectProps = from.getObjectProps();
    for (Map.Entry<String, Object> entry : objectProps.entrySet()) {
      to.addProp(entry.getKey(), entry.getValue());
    }
  }

  public static boolean hasGeneratedJavaClass(final Schema schema) {
    Schema.Type type = schema.getType();
    switch (type) {
    case ENUM:
    case RECORD:
    case FIXED:
      return true;
    default:
      return false;
    }
  }

  public static String getJavaClassName(final Schema schema) {
    String namespace = schema.getNamespace();
    if (namespace == null) {
      return SpecificCompiler.mangle(schema.getName());
    } else {
      return namespace + '.' + SpecificCompiler.mangle(schema.getName());
    }
  }

  /**
   * depth first visit.
   *
   * @param start
   * @param visitor
   */
  public static <T> T visit(final Schema start, final SchemaVisitor<T> visitor) {
    // Set of Visited Schemas
    IdentityHashMap<Schema, Schema> visited = new IdentityHashMap<>();
    // Stack that contains the Schams to process and afterVisitNonTerminal
    // functions.
    // Deque<Either<Schema, Supplier<SchemaVisitorAction>>>
    // Using either has a cost which we want to avoid...
    Deque<Object> dq = new ArrayDeque<>();
    dq.addLast(start);
    Object current;
    while ((current = dq.pollLast()) != null) {
      if (current instanceof Supplier) {
        // we are executing a non terminal post visit.
        SchemaVisitorAction action = ((Supplier<SchemaVisitorAction>) current).get();
        switch (action) {
        case CONTINUE:
          break;
        case SKIP_SUBTREE:
          throw new UnsupportedOperationException();
        case SKIP_SIBLINGS:
          while (dq.getLast() instanceof Schema) {
            dq.removeLast();
          }
          break;
        case TERMINATE:
          return visitor.get();
        default:
          throw new UnsupportedOperationException("Invalid action " + action);
        }
      } else {
        Schema schema = (Schema) current;
        boolean terminate;
        if (!visited.containsKey(schema)) {
          Schema.Type type = schema.getType();
          switch (type) {
          case ARRAY:
            terminate = visitNonTerminal(visitor, schema, dq, Collections.singleton(schema.getElementType()));
            visited.put(schema, schema);
            break;
          case RECORD:
            Iterator<Schema> reverseSchemas = schema.getFields().stream().map(Field::schema)
                .collect(Collectors.toCollection(ArrayDeque::new)).descendingIterator();
            terminate = visitNonTerminal(visitor, schema, dq, () -> reverseSchemas);
            visited.put(schema, schema);
            break;
          case UNION:
            terminate = visitNonTerminal(visitor, schema, dq, schema.getTypes());
            visited.put(schema, schema);
            break;
          case MAP:
            terminate = visitNonTerminal(visitor, schema, dq, Collections.singleton(schema.getValueType()));
            visited.put(schema, schema);
            break;
          case NULL:
          case BOOLEAN:
          case BYTES:
          case DOUBLE:
          case ENUM:
          case FIXED:
          case FLOAT:
          case INT:
          case LONG:
          case STRING:
            terminate = visitTerminal(visitor, schema, dq);
            break;
          default:
            throw new UnsupportedOperationException("Invalid type " + type);
          }

        } else {
          terminate = visitTerminal(visitor, schema, dq);
        }
        if (terminate) {
          return visitor.get();
        }
      }
    }
    return visitor.get();
  }

  private static boolean visitNonTerminal(final SchemaVisitor visitor, final Schema schema, final Deque<Object> dq,
      final Iterable<Schema> itSupp) {
    SchemaVisitorAction action = visitor.visitNonTerminal(schema);
    switch (action) {
    case CONTINUE:
      dq.addLast((Supplier<SchemaVisitorAction>) () -> visitor.afterVisitNonTerminal(schema));
      for (Schema child : itSupp) {
        dq.addLast(child);
      }
      break;
    case SKIP_SUBTREE:
      dq.addLast((Supplier<SchemaVisitorAction>) () -> visitor.afterVisitNonTerminal(schema));
      break;
    case SKIP_SIBLINGS:
      while (!dq.isEmpty() && dq.getLast() instanceof Schema) {
        dq.removeLast();
      }
      break;
    case TERMINATE:
      return true;
    default:
      throw new UnsupportedOperationException("Invalid action " + action + " for " + schema);
    }
    return false;
  }

  private static boolean visitTerminal(final SchemaVisitor visitor, final Schema schema, final Deque<Object> dq) {
    SchemaVisitorAction action = visitor.visitTerminal(schema);
    switch (action) {
    case CONTINUE:
      break;
    case SKIP_SUBTREE:
      throw new UnsupportedOperationException("Invalid action " + action + " for " + schema);
    case SKIP_SIBLINGS:
      while (!dq.isEmpty() && dq.getLast() instanceof Schema) {
        dq.removeLast();
      }
      break;
    case TERMINATE:
      return true;
    default:
      throw new UnsupportedOperationException("Invalid action " + action + " for " + schema);
    }
    return false;
  }

}
