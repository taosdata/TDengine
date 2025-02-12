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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Objects;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;

/**
 * An {@link Encoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link EncoderFactory}.
 * </p>
 * JsonEncoder buffers output, and data may not appear on the output until
 * {@link Encoder#flush()} is called.
 * </p>
 * JsonEncoder is not thread-safe.
 */
public class JsonEncoder extends ParsingEncoder implements Parser.ActionHandler {
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  final Parser parser;
  private JsonGenerator out;
  private boolean includeNamespace = true;

  /**
   * Has anything been written into the collections?
   */
  protected BitSet isEmpty = new BitSet();

  JsonEncoder(Schema sc, OutputStream out) throws IOException {
    this(sc, getJsonGenerator(out, false));
  }

  JsonEncoder(Schema sc, OutputStream out, boolean pretty) throws IOException {
    this(sc, getJsonGenerator(out, pretty));
  }

  JsonEncoder(Schema sc, JsonGenerator out) throws IOException {
    configure(out);
    this.parser = new Parser(new JsonGrammarGenerator().generate(sc), this);
  }

  @Override
  public void flush() throws IOException {
    parser.processImplicitActions();
    if (out != null) {
      out.flush();
    }
  }

  // by default, one object per line.
  // with pretty option use default pretty printer with root line separator.
  private static JsonGenerator getJsonGenerator(OutputStream out, boolean pretty) throws IOException {
    Objects.requireNonNull(out, "OutputStream cannot be null");
    JsonGenerator g = new JsonFactory().createGenerator(out, JsonEncoding.UTF8);
    if (pretty) {
      DefaultPrettyPrinter pp = new DefaultPrettyPrinter() {
        @Override
        public void writeRootValueSeparator(JsonGenerator jg) throws IOException {
          jg.writeRaw(LINE_SEPARATOR);
        }
      };
      g.setPrettyPrinter(pp);
    } else {
      MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
      pp.setRootValueSeparator(LINE_SEPARATOR);
      g.setPrettyPrinter(pp);
    }
    return g;
  }

  public boolean isIncludeNamespace() {
    return includeNamespace;
  }

  public void setIncludeNamespace(final boolean includeNamespace) {
    this.includeNamespace = includeNamespace;
  }

  /**
   * Reconfigures this JsonEncoder to use the output stream provided.
   * <p/>
   * If the OutputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use a default UTF8 JsonGenerator that writes to the
   * provided OutputStream.
   *
   * @param out The OutputStream to direct output to. Cannot be null.
   * @throws IOException
   * @throws NullPointerException if {@code out} is {@code null}
   * @return this JsonEncoder
   */
  public JsonEncoder configure(OutputStream out) throws IOException {
    this.configure(getJsonGenerator(out, false));
    return this;
  }

  /**
   * Reconfigures this JsonEncoder to output to the JsonGenerator provided.
   * <p/>
   * If the JsonGenerator provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonEncoder will flush its current output and then
   * reconfigure its output to use the provided JsonGenerator.
   *
   * @param generator The JsonGenerator to direct output to. Cannot be null.
   * @throws IOException
   * @throws NullPointerException if {@code generator} is {@code null}
   * @return this JsonEncoder
   */
  private JsonEncoder configure(JsonGenerator generator) throws IOException {
    Objects.requireNonNull(generator, "JsonGenerator cannot be null");
    if (null != parser) {
      flush();
    }
    this.out = generator;
    return this;
  }

  @Override
  public void writeNull() throws IOException {
    parser.advance(Symbol.NULL);
    out.writeNull();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    parser.advance(Symbol.BOOLEAN);
    out.writeBoolean(b);
  }

  @Override
  public void writeInt(int n) throws IOException {
    parser.advance(Symbol.INT);
    out.writeNumber(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    out.writeNumber(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    out.writeNumber(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    out.writeNumber(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    writeString(utf8.toString());
  }

  @Override
  public void writeString(String str) throws IOException {
    parser.advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      out.writeFieldName(str);
    } else {
      out.writeString(str);
    }
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    if (bytes.hasArray()) {
      writeBytes(bytes.array(), bytes.position(), bytes.remaining());
    } else {
      byte[] b = new byte[bytes.remaining()];
      bytes.duplicate().get(b);
      writeBytes(b);
    }
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.BYTES);
    writeByteArray(bytes, start, len);
  }

  private void writeByteArray(byte[] bytes, int start, int len) throws IOException {
    out.writeString(new String(bytes, start, len, StandardCharsets.ISO_8859_1));
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (len != top.size) {
      throw new AvroTypeException(
          "Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.");
    }
    writeByteArray(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    parser.advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException("Enumeration out of range: max is " + top.size + " but received " + e);
    }
    out.writeString(top.getLabel(e));
  }

  @Override
  public void writeArrayStart() throws IOException {
    parser.advance(Symbol.ARRAY_START);
    out.writeStartArray();
    push();
    isEmpty.set(depth());
  }

  @Override
  public void writeArrayEnd() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    pop();
    parser.advance(Symbol.ARRAY_END);
    out.writeEndArray();
  }

  @Override
  public void writeMapStart() throws IOException {
    push();
    isEmpty.set(depth());

    parser.advance(Symbol.MAP_START);
    out.writeStartObject();
  }

  @Override
  public void writeMapEnd() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    pop();

    parser.advance(Symbol.MAP_END);
    out.writeEndObject();
  }

  @Override
  public void startItem() throws IOException {
    if (!isEmpty.get(pos)) {
      parser.advance(Symbol.ITEM_END);
    }
    super.startItem();
    isEmpty.clear(depth());
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbol.NULL && includeNamespace) {
      out.writeStartObject();
      out.writeFieldName(top.getLabel(unionIndex));
      parser.pushSymbol(Symbol.UNION_END);
    }
    parser.pushSymbol(symbol);
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      out.writeFieldName(fa.fname);
    } else if (top == Symbol.RECORD_START) {
      out.writeStartObject();
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      out.writeEndObject();
    } else if (top != Symbol.FIELD_END) {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }
}
