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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.internal.Accessor;
import org.apache.avro.util.internal.Accessor.ResolvingGrammarGeneratorAccessor;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The class that generates a resolving grammar to resolve between two schemas.
 */
public class ResolvingGrammarGenerator extends ValidatingGrammarGenerator {

  static {
    Accessor.setAccessor(new ResolvingGrammarGeneratorAccessor() {
      @Override
      protected void encode(Encoder e, Schema s, JsonNode n) throws IOException {
        ResolvingGrammarGenerator.encode(e, s, n);
      }
    });
  }

  /**
   * Resolves the writer schema <tt>writer</tt> and the reader schema
   * <tt>reader</tt> and returns the start symbol for the grammar generated.
   *
   * @param writer The schema used by the writer
   * @param reader The schema used by the reader
   * @return The start symbol for the resolving grammar
   * @throws IOException
   */
  public final Symbol generate(Schema writer, Schema reader) throws IOException {
    Resolver.Action r = Resolver.resolve(writer, reader);
    return Symbol.root(generate(r, new HashMap<>()));
  }

  /**
   * Takes a {@link Resolver.Action} for resolving two schemas and returns the
   * start symbol for a grammar that implements that resolution. If the action is
   * for a record and there's already a symbol for that record in <tt>seen</tt>,
   * then that symbol is returned. Otherwise a new symbol is generated and
   * returned.
   *
   * @param action The resolver to be implemented
   * @param seen   The &lt;Action&gt; to symbol map of start symbols of resolving
   *               grammars so far.
   * @return The start symbol for the resolving grammar
   * @throws IOException
   */
  private Symbol generate(Resolver.Action action, Map<Object, Symbol> seen) throws IOException {
    if (action instanceof Resolver.DoNothing) {
      return simpleGen(action.writer, seen);

    } else if (action instanceof Resolver.ErrorAction) {
      return Symbol.error(action.toString());

    } else if (action instanceof Resolver.Skip) {
      return Symbol.skipAction(simpleGen(action.writer, seen));

    } else if (action instanceof Resolver.Promote) {
      return Symbol.resolve(simpleGen(action.writer, seen), simpleGen(action.reader, seen));

    } else if (action instanceof Resolver.ReaderUnion) {
      Resolver.ReaderUnion ru = (Resolver.ReaderUnion) action;
      Symbol s = generate(ru.actualAction, seen);
      return Symbol.seq(Symbol.unionAdjustAction(ru.firstMatch, s), Symbol.UNION);

    } else if (action.writer.getType() == Schema.Type.ARRAY) {
      Symbol es = generate(((Resolver.Container) action).elementAction, seen);
      return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, es), Symbol.ARRAY_START);

    } else if (action.writer.getType() == Schema.Type.MAP) {
      Symbol es = generate(((Resolver.Container) action).elementAction, seen);
      return Symbol.seq(Symbol.repeat(Symbol.MAP_END, es, Symbol.STRING), Symbol.MAP_START);

    } else if (action.writer.getType() == Schema.Type.UNION) {
      if (((Resolver.WriterUnion) action).unionEquiv) {
        return simpleGen(action.reader, seen);
      }
      Resolver.Action[] branches = ((Resolver.WriterUnion) action).actions;
      Symbol[] symbols = new Symbol[branches.length];
      String[] labels = new String[branches.length];
      int i = 0;
      for (Resolver.Action branch : branches) {
        symbols[i] = generate(branch, seen);
        labels[i] = action.writer.getTypes().get(i).getFullName();
        i++;
      }
      return Symbol.seq(Symbol.alt(symbols, labels), Symbol.WRITER_UNION_ACTION);
    } else if (action instanceof Resolver.EnumAdjust) {
      Resolver.EnumAdjust e = (Resolver.EnumAdjust) action;
      Object[] adjs = new Object[e.adjustments.length];
      for (int i = 0; i < adjs.length; i++) {
        adjs[i] = (0 <= e.adjustments[i] ? Integer.valueOf(e.adjustments[i])
            : "No match for " + e.writer.getEnumSymbols().get(i));
      }
      return Symbol.seq(Symbol.enumAdjustAction(e.reader.getEnumSymbols().size(), adjs), Symbol.ENUM);

    } else if (action instanceof Resolver.RecordAdjust) {
      Symbol result = seen.get(action);
      if (result == null) {
        final Resolver.RecordAdjust ra = (Resolver.RecordAdjust) action;
        int defaultCount = ra.readerOrder.length - ra.firstDefault;
        int count = 1 + ra.fieldActions.length + 3 * defaultCount;
        final Symbol[] production = new Symbol[count];
        result = Symbol.seq(production);
        seen.put(action, result);
        production[--count] = Symbol.fieldOrderAction(ra.readerOrder);

        final Resolver.Action[] actions = ra.fieldActions;
        for (Resolver.Action wfa : actions) {
          production[--count] = generate(wfa, seen);
        }
        for (int i = ra.firstDefault; i < ra.readerOrder.length; i++) {
          final Schema.Field rf = ra.readerOrder[i];
          byte[] bb = getBinary(rf.schema(), Accessor.defaultValue(rf));
          production[--count] = Symbol.defaultStartAction(bb);
          production[--count] = simpleGen(rf.schema(), seen);
          production[--count] = Symbol.DEFAULT_END_ACTION;
        }
      }
      return result;
    }

    throw new IllegalArgumentException("Unrecognized Resolver.Action: " + action);
  }

  private Symbol simpleGen(Schema s, Map<Object, Symbol> seen) {
    switch (s.getType()) {
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
    case BYTES:
      return Symbol.BYTES;
    case STRING:
      return Symbol.STRING;

    case FIXED:
      return Symbol.seq(Symbol.intCheckAction(s.getFixedSize()), Symbol.FIXED);

    case ENUM:
      return Symbol.seq(Symbol.enumAdjustAction(s.getEnumSymbols().size(), null), Symbol.ENUM);

    case ARRAY:
      return Symbol.seq(Symbol.repeat(Symbol.ARRAY_END, simpleGen(s.getElementType(), seen)), Symbol.ARRAY_START);

    case MAP:
      return Symbol.seq(Symbol.repeat(Symbol.MAP_END, simpleGen(s.getValueType(), seen), Symbol.STRING),
          Symbol.MAP_START);

    case UNION: {
      final List<Schema> subs = s.getTypes();
      final Symbol[] symbols = new Symbol[subs.size()];
      final String[] labels = new String[subs.size()];
      int i = 0;
      for (Schema b : s.getTypes()) {
        symbols[i] = simpleGen(b, seen);
        labels[i++] = b.getFullName();
      }
      return Symbol.seq(Symbol.alt(symbols, labels), Symbol.UNION);
    }

    case RECORD: {
      Symbol result = seen.get(s);
      if (result == null) {
        final Symbol[] production = new Symbol[s.getFields().size() + 1];
        result = Symbol.seq(production);
        seen.put(s, result);
        int i = production.length;
        production[--i] = Symbol.fieldOrderAction(s.getFields().toArray(new Schema.Field[0]));
        for (Field f : s.getFields()) {
          production[--i] = simpleGen(f.schema(), seen);
        }
        // FieldOrderAction is needed even though the field-order hasn't changed,
        // because the _reader_ doesn't know the field order hasn't changed, and
        // thus it will probably call {@ ResolvingDecoder.fieldOrder} to find out.
      }
      return result;
    }

    default:
      throw new IllegalArgumentException("Unexpected schema: " + s);
    }
  }

  private static EncoderFactory factory = new EncoderFactory().configureBufferSize(32);

  /**
   * Returns the Avro binary encoded version of <tt>n</tt> according to the schema
   * <tt>s</tt>.
   *
   * @param s The schema for encoding
   * @param n The Json node that has the value to be encoded.
   * @return The binary encoded version of <tt>n</tt>.
   * @throws IOException
   */
  private static byte[] getBinary(Schema s, JsonNode n) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder e = factory.binaryEncoder(out, null);
    encode(e, s, n);
    e.flush();
    return out.toByteArray();
  }

  /**
   * Encodes the given Json node <tt>n</tt> on to the encoder <tt>e</tt> according
   * to the schema <tt>s</tt>.
   *
   * @param e The encoder to encode into.
   * @param s The schema for the object being encoded.
   * @param n The Json node to encode.
   * @throws IOException
   */
  public static void encode(Encoder e, Schema s, JsonNode n) throws IOException {
    switch (s.getType()) {
    case RECORD:
      for (Field f : s.getFields()) {
        String name = f.name();
        JsonNode v = n.get(name);
        if (v == null) {
          v = Accessor.defaultValue(f);
        }
        if (v == null) {
          throw new AvroTypeException("No default value for: " + name);
        }
        encode(e, f.schema(), v);
      }
      break;
    case ENUM:
      e.writeEnum(s.getEnumOrdinal(n.textValue()));
      break;
    case ARRAY:
      e.writeArrayStart();
      e.setItemCount(n.size());
      Schema i = s.getElementType();
      for (JsonNode node : n) {
        e.startItem();
        encode(e, i, node);
      }
      e.writeArrayEnd();
      break;
    case MAP:
      e.writeMapStart();
      e.setItemCount(n.size());
      Schema v = s.getValueType();
      for (Iterator<String> it = n.fieldNames(); it.hasNext();) {
        e.startItem();
        String key = it.next();
        e.writeString(key);
        encode(e, v, n.get(key));
      }
      e.writeMapEnd();
      break;
    case UNION:
      e.writeIndex(0);
      encode(e, s.getTypes().get(0), n);
      break;
    case FIXED:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for fixed: " + n);
      byte[] bb = n.textValue().getBytes(StandardCharsets.ISO_8859_1);
      if (bb.length != s.getFixedSize()) {
        bb = Arrays.copyOf(bb, s.getFixedSize());
      }
      e.writeFixed(bb);
      break;
    case STRING:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for string: " + n);
      e.writeString(n.textValue());
      break;
    case BYTES:
      if (!n.isTextual())
        throw new AvroTypeException("Non-string default value for bytes: " + n);
      e.writeBytes(n.textValue().getBytes(StandardCharsets.ISO_8859_1));
      break;
    case INT:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for int: " + n);
      e.writeInt(n.intValue());
      break;
    case LONG:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for long: " + n);
      e.writeLong(n.longValue());
      break;
    case FLOAT:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for float: " + n);
      e.writeFloat((float) n.doubleValue());
      break;
    case DOUBLE:
      if (!n.isNumber())
        throw new AvroTypeException("Non-numeric default value for double: " + n);
      e.writeDouble(n.doubleValue());
      break;
    case BOOLEAN:
      if (!n.isBoolean())
        throw new AvroTypeException("Non-boolean default for boolean: " + n);
      e.writeBoolean(n.booleanValue());
      break;
    case NULL:
      if (!n.isNull())
        throw new AvroTypeException("Non-null default value for null type: " + n);
      e.writeNull();
      break;
    }
  }
}
