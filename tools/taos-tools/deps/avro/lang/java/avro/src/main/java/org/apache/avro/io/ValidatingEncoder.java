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
import java.nio.ByteBuffer;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.ValidatingGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

/**
 * An implementation of {@link Encoder} that wraps another Encoder and ensures
 * that the sequence of operations conforms to the provided schema.
 * <p/>
 * Use {@link EncoderFactory#validatingEncoder(Schema, Encoder)} to construct
 * and configure.
 * <p/>
 * ValidatingEncoder is not thread-safe.
 * 
 * @see Encoder
 * @see EncoderFactory
 */
public class ValidatingEncoder extends ParsingEncoder implements Parser.ActionHandler {
  protected Encoder out;
  protected final Parser parser;

  ValidatingEncoder(Symbol root, Encoder out) throws IOException {
    this.out = out;
    this.parser = new Parser(root, this);
  }

  ValidatingEncoder(Schema schema, Encoder in) throws IOException {
    this(new ValidatingGrammarGenerator().generate(schema), in);
  }

  @Override
  public void flush() throws IOException {
    out.flush();
  }

  /**
   * Reconfigures this ValidatingEncoder to wrap the encoder provided.
   * 
   * @param encoder The Encoder to wrap for validation.
   * @return This ValidatingEncoder.
   */
  public ValidatingEncoder configure(Encoder encoder) {
    this.parser.reset();
    this.out = encoder;
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
    out.writeInt(n);
  }

  @Override
  public void writeLong(long n) throws IOException {
    parser.advance(Symbol.LONG);
    out.writeLong(n);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    parser.advance(Symbol.FLOAT);
    out.writeFloat(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    parser.advance(Symbol.DOUBLE);
    out.writeDouble(d);
  }

  @Override
  public void writeString(Utf8 utf8) throws IOException {
    parser.advance(Symbol.STRING);
    out.writeString(utf8);
  }

  @Override
  public void writeString(String str) throws IOException {
    parser.advance(Symbol.STRING);
    out.writeString(str);
  }

  @Override
  public void writeString(CharSequence charSequence) throws IOException {
    parser.advance(Symbol.STRING);
    out.writeString(charSequence);
  }

  @Override
  public void writeBytes(ByteBuffer bytes) throws IOException {
    parser.advance(Symbol.BYTES);
    out.writeBytes(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.BYTES);
    out.writeBytes(bytes, start, len);
  }

  @Override
  public void writeFixed(byte[] bytes, int start, int len) throws IOException {
    parser.advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (len != top.size) {
      throw new AvroTypeException(
          "Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.");
    }
    out.writeFixed(bytes, start, len);
  }

  @Override
  public void writeEnum(int e) throws IOException {
    parser.advance(Symbol.ENUM);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException("Enumeration out of range: max is " + top.size + " but received " + e);
    }
    out.writeEnum(e);
  }

  @Override
  public void writeArrayStart() throws IOException {
    push();
    parser.advance(Symbol.ARRAY_START);
    out.writeArrayStart();
  }

  @Override
  public void writeArrayEnd() throws IOException {
    parser.advance(Symbol.ARRAY_END);
    out.writeArrayEnd();
    pop();
  }

  @Override
  public void writeMapStart() throws IOException {
    push();
    parser.advance(Symbol.MAP_START);
    out.writeMapStart();
  }

  @Override
  public void writeMapEnd() throws IOException {
    parser.advance(Symbol.MAP_END);
    out.writeMapEnd();
    pop();
  }

  @Override
  public void setItemCount(long itemCount) throws IOException {
    super.setItemCount(itemCount);
    out.setItemCount(itemCount);
  }

  @Override
  public void startItem() throws IOException {
    super.startItem();
    out.startItem();
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    parser.pushSymbol(top.getSymbol(unionIndex));
    out.writeIndex(unionIndex);
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    return null;
  }

}
