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
package org.apache.avro.io.parsing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * The class that generates validating grammar.
 */
public class ValidatingGrammarGenerator {
  /**
   * Returns the non-terminal that is the start symbol for the grammar for the
   * given schema <tt>sc</tt>.
   */
  public Symbol generate(Schema schema) {
    return Symbol.root(generate(schema, new HashMap<>()));
  }

  /**
   * Returns the non-terminal that is the start symbol for the grammar for the
   * given schema <tt>sc</tt>. If there is already an entry for the given schema
   * in the given map <tt>seen</tt> then that entry is returned. Otherwise a new
   * symbol is generated and an entry is inserted into the map.
   * 
   * @param sc   The schema for which the start symbol is required
   * @param seen A map of schema to symbol mapping done so far.
   * @return The start symbol for the schema
   */
  public Symbol generate(Schema sc, Map<LitS, Symbol> seen) {
    switch (sc.getType()) {
    case NULL:
      return Symbol.NULL;
    case BOOLEAN:
      return Symbol.BOOLEAN;
    case INT:
      return Symbol.INT;
    case LONG:
      return Symbol.LONG;
    case FLOAT:
      return Symbol.FLOAT;
    case DOUBLE:
      return Symbol.DOUBLE;
    case STRING:
      return Symbol.STRING;
    case BYTES:
      return Symbol.BYTES;
    case FIXED:
      return Symbol.seq(Symbol.intCheckAction(sc.getFixedSize()), Symbol.FIXED);
    case ENUM:
      return Symbol.seq(Symbol.intCheckAction(sc.getEnumSymbols().size()), Symbol.ENUM);
    case ARRAY:
      return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, generate(sc.getElementType(), seen)), Symbol.ARRAY_START);
    case MAP:
      return Symbol.seq(Symbol.repeat(Symbol.MAP_END, generate(sc.getValueType(), seen), Symbol.STRING),
          Symbol.MAP_START);
    case RECORD: {
      LitS wsc = new LitS(sc);
      Symbol rresult = seen.get(wsc);
      if (rresult == null) {
        Symbol[] production = new Symbol[sc.getFields().size()];

        /**
         * We construct a symbol without filling the array. Please see
         * {@link Symbol#production} for the reason.
         */
        rresult = Symbol.seq(production);
        seen.put(wsc, rresult);

        int i = production.length;
        for (Field f : sc.getFields()) {
          production[--i] = generate(f.schema(), seen);
        }
      }
      return rresult;
    }
    case UNION:
      List<Schema> subs = sc.getTypes();
      Symbol[] symbols = new Symbol[subs.size()];
      String[] labels = new String[subs.size()];

      int i = 0;
      for (Schema b : sc.getTypes()) {
        symbols[i] = generate(b, seen);
        labels[i] = b.getFullName();
        i++;
      }
      return Symbol.seq(Symbol.alt(symbols, labels), Symbol.UNION);

    default:
      throw new RuntimeException("Unexpected schema type");
    }
  }

  /** A wrapper around Schema that does "==" equality. */
  static class LitS {
    public final Schema actual;

    public LitS(Schema actual) {
      this.actual = actual;
    }

    /**
     * Two LitS are equal if and only if their underlying schema is the same (not
     * merely equal).
     */
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof LitS))
        return false;
      return actual.equals(((LitS) o).actual);
    }

    @Override
    public int hashCode() {
      return actual.hashCode();
    }
  }
}
