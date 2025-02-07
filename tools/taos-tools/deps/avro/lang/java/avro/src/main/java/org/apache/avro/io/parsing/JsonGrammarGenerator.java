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
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

/**
 * The class that generates a grammar suitable to parse Avro data in JSON
 * format.
 */

public class JsonGrammarGenerator extends ValidatingGrammarGenerator {
  /**
   * Returns the non-terminal that is the start symbol for the grammar for the
   * grammar for the given schema <tt>sc</tt>.
   */
  @Override
  public Symbol generate(Schema schema) {
    return Symbol.root(generate(schema, new HashMap<>()));
  }

  /**
   * Returns the non-terminal that is the start symbol for grammar of the given
   * schema <tt>sc</tt>. If there is already an entry for the given schema in the
   * given map <tt>seen</tt> then that entry is returned. Otherwise a new symbol
   * is generated and an entry is inserted into the map.
   * 
   * @param sc   The schema for which the start symbol is required
   * @param seen A map of schema to symbol mapping done so far.
   * @return The start symbol for the schema
   */
  @Override
  public Symbol generate(Schema sc, Map<LitS, Symbol> seen) {
    switch (sc.getType()) {
    case NULL:
    case BOOLEAN:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE:
    case STRING:
    case BYTES:
    case FIXED:
    case UNION:
      return super.generate(sc, seen);
    case ENUM:
      return Symbol.seq(Symbol.enumLabelsAction(sc.getEnumSymbols()), Symbol.ENUM);
    case ARRAY:
      return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, Symbol.ITEM_END, generate(sc.getElementType(), seen)),
          Symbol.ARRAY_START);
    case MAP:
      return Symbol.seq(Symbol.repeat(Symbol.MAP_END, Symbol.ITEM_END, generate(sc.getValueType(), seen),
          Symbol.MAP_KEY_MARKER, Symbol.STRING), Symbol.MAP_START);
    case RECORD: {
      LitS wsc = new LitS(sc);
      Symbol rresult = seen.get(wsc);
      if (rresult == null) {
        Symbol[] production = new Symbol[sc.getFields().size() * 3 + 2];
        rresult = Symbol.seq(production);
        seen.put(wsc, rresult);

        int i = production.length;
        int n = 0;
        production[--i] = Symbol.RECORD_START;
        for (Field f : sc.getFields()) {
          production[--i] = Symbol.fieldAdjustAction(n, f.name(), f.aliases());
          production[--i] = generate(f.schema(), seen);
          production[--i] = Symbol.FIELD_END;
          n++;
        }
        production[--i] = Symbol.RECORD_END;
      }
      return rresult;
    }
    default:
      throw new RuntimeException("Unexpected schema type");
    }
  }
}
