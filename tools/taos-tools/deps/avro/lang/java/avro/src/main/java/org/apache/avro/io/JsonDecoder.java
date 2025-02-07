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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.util.TokenBuffer;

/**
 * A {@link Decoder} for Avro's JSON data encoding.
 * </p>
 * Construct using {@link DecoderFactory}.
 * </p>
 * JsonDecoder is not thread-safe.
 */
public class JsonDecoder extends ParsingDecoder implements Parser.ActionHandler {
  private JsonParser in;
  private static JsonFactory jsonFactory = new JsonFactory();
  Stack<ReorderBuffer> reorderBuffers = new Stack<>();
  ReorderBuffer currentReorderBuffer;

  private static class ReorderBuffer {
    public Map<String, TokenBuffer> savedFields = new HashMap<>();
    public JsonParser origParser = null;
  }

  private JsonDecoder(Symbol root, InputStream in) throws IOException {
    super(root);
    configure(in);
  }

  private JsonDecoder(Symbol root, String in) throws IOException {
    super(root);
    configure(in);
  }

  JsonDecoder(Schema schema, InputStream in) throws IOException {
    this(getSymbol(schema), in);
  }

  JsonDecoder(Schema schema, String in) throws IOException {
    this(getSymbol(schema), in);
  }

  private static Symbol getSymbol(Schema schema) {
    Objects.requireNonNull(schema, "Schema cannot be null");
    return new JsonGrammarGenerator().generate(schema);
  }

  /**
   * Reconfigures this JsonDecoder to use the InputStream provided.
   * <p/>
   * If the InputStream provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then reconfigure its
   * input.
   * 
   * @param in The InputStream to read from. Cannot be null.
   * @throws IOException
   * @throws NullPointerException if {@code in} is {@code null}
   * @return this JsonDecoder
   */
  public JsonDecoder configure(InputStream in) throws IOException {
    Objects.requireNonNull(in, "InputStream cannot be null");
    parser.reset();
    reorderBuffers.clear();
    currentReorderBuffer = null;
    this.in = jsonFactory.createParser(in);
    this.in.nextToken();
    return this;
  }

  /**
   * Reconfigures this JsonDecoder to use the String provided for input.
   * <p/>
   * If the String provided is null, a NullPointerException is thrown.
   * <p/>
   * Otherwise, this JsonDecoder will reset its state and then reconfigure its
   * input.
   * 
   * @param in The String to read from. Cannot be null.
   * @throws IOException
   * @throws NullPointerException if {@code in} is {@code null}
   * @return this JsonDecoder
   */
  public JsonDecoder configure(String in) throws IOException {
    Objects.requireNonNull(in, "String to read from cannot be null");
    parser.reset();
    reorderBuffers.clear();
    currentReorderBuffer = null;
    this.in = new JsonFactory().createParser(in);
    this.in.nextToken();
    return this;
  }

  private void advance(Symbol symbol) throws IOException {
    this.parser.processTrailingImplicitActions();
    if (in.getCurrentToken() == null && this.parser.depth() == 1)
      throw new EOFException();
    parser.advance(symbol);
  }

  @Override
  public void readNull() throws IOException {
    advance(Symbol.NULL);
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      in.nextToken();
    } else {
      throw error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    JsonToken t = in.getCurrentToken();
    if (t == JsonToken.VALUE_TRUE || t == JsonToken.VALUE_FALSE) {
      in.nextToken();
      return t == JsonToken.VALUE_TRUE;
    } else {
      throw error("boolean");
    }
  }

  @Override
  public int readInt() throws IOException {
    advance(Symbol.INT);
    if (in.getCurrentToken().isNumeric()) {
      int result = in.getIntValue();
      in.nextToken();
      return result;
    } else {
      throw error("int");
    }
  }

  @Override
  public long readLong() throws IOException {
    advance(Symbol.LONG);
    if (in.getCurrentToken().isNumeric()) {
      long result = in.getLongValue();
      in.nextToken();
      return result;
    } else {
      throw error("long");
    }
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    if (in.getCurrentToken().isNumeric()) {
      float result = in.getFloatValue();
      in.nextToken();
      return result;
    } else {
      throw error("float");
    }
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    if (in.getCurrentToken().isNumeric()) {
      double result = in.getDoubleValue();
      in.nextToken();
      return result;
    } else {
      throw error("double");
    }
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
        throw error("string");
      }
    }
    String result = in.getText();
    in.nextToken();
    return result;
  }

  @Override
  public void skipString() throws IOException {
    advance(Symbol.STRING);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER);
      if (in.getCurrentToken() != JsonToken.FIELD_NAME) {
        throw error("map-key");
      }
    } else {
      if (in.getCurrentToken() != JsonToken.VALUE_STRING) {
        throw error("string");
      }
    }
    in.nextToken();
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    advance(Symbol.BYTES);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      return ByteBuffer.wrap(result);
    } else {
      throw error("bytes");
    }
  }

  private byte[] readByteArray() throws IOException {
    byte[] result = in.getText().getBytes(StandardCharsets.ISO_8859_1);
    return result;
  }

  @Override
  public void skipBytes() throws IOException {
    advance(Symbol.BYTES);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      in.nextToken();
    } else {
      throw error("bytes");
    }
  }

  private void checkFixed(int size) throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    if (size != top.size) {
      throw new AvroTypeException(
          "Incorrect length for fixed binary: expected " + top.size + " but received " + size + " bytes.");
    }
  }

  @Override
  public void readFixed(byte[] bytes, int start, int len) throws IOException {
    checkFixed(len);
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != len) {
        throw new AvroTypeException("Expected fixed length " + len + ", but got" + result.length);
      }
      System.arraycopy(result, 0, bytes, start, len);
    } else {
      throw error("fixed");
    }
  }

  @Override
  public void skipFixed(int length) throws IOException {
    checkFixed(length);
    doSkipFixed(length);
  }

  private void doSkipFixed(int length) throws IOException {
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      byte[] result = readByteArray();
      in.nextToken();
      if (result.length != length) {
        throw new AvroTypeException("Expected fixed length " + length + ", but got" + result.length);
      }
    } else {
      throw error("fixed");
    }
  }

  @Override
  protected void skipFixed() throws IOException {
    advance(Symbol.FIXED);
    Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
    doSkipFixed(top.size);
  }

  @Override
  public int readEnum() throws IOException {
    advance(Symbol.ENUM);
    Symbol.EnumLabelsAction top = (Symbol.EnumLabelsAction) parser.popSymbol();
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      in.getText();
      int n = top.findLabel(in.getText());
      if (n >= 0) {
        in.nextToken();
        return n;
      }
      throw new AvroTypeException("Unknown symbol in enum " + in.getText());
    } else {
      throw error("fixed");
    }
  }

  @Override
  public long readArrayStart() throws IOException {
    advance(Symbol.ARRAY_START);
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.nextToken();
      return doArrayNext();
    } else {
      throw error("array-start");
    }
  }

  @Override
  public long arrayNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doArrayNext();
  }

  private long doArrayNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_ARRAY) {
      parser.advance(Symbol.ARRAY_END);
      in.nextToken();
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipArray() throws IOException {
    advance(Symbol.ARRAY_START);
    if (in.getCurrentToken() == JsonToken.START_ARRAY) {
      in.skipChildren();
      in.nextToken();
      advance(Symbol.ARRAY_END);
    } else {
      throw error("array-start");
    }
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    advance(Symbol.MAP_START);
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.nextToken();
      return doMapNext();
    } else {
      throw error("map-start");
    }
  }

  @Override
  public long mapNext() throws IOException {
    advance(Symbol.ITEM_END);
    return doMapNext();
  }

  private long doMapNext() throws IOException {
    if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      in.nextToken();
      advance(Symbol.MAP_END);
      return 0;
    } else {
      return 1;
    }
  }

  @Override
  public long skipMap() throws IOException {
    advance(Symbol.MAP_START);
    if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      in.skipChildren();
      in.nextToken();
      advance(Symbol.MAP_END);
    } else {
      throw error("map-start");
    }
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    advance(Symbol.UNION);
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    if (in.getCurrentToken() == JsonToken.VALUE_NULL) {
      label = "null";
    } else if (in.getCurrentToken() == JsonToken.START_OBJECT && in.nextToken() == JsonToken.FIELD_NAME) {
      label = in.getText();
      in.nextToken();
      parser.pushSymbol(Symbol.UNION_END);
    } else {
      throw error("start-union");
    }
    int n = a.findLabel(label);
    if (n < 0)
      throw new AvroTypeException("Unknown union branch " + label);
    parser.pushSymbol(a.getSymbol(n));
    return n;
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      String name = fa.fname;
      if (currentReorderBuffer != null) {
        try (TokenBuffer tokenBuffer = currentReorderBuffer.savedFields.get(name)) {
          if (tokenBuffer != null) {
            currentReorderBuffer.savedFields.remove(name);
            currentReorderBuffer.origParser = in;
            in = tokenBuffer.asParser();
            in.nextToken();
            return null;
          }
        }
      }
      if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
        do {
          String fn = in.getText();
          in.nextToken();
          if (name.equals(fn) || fa.aliases.contains(fn)) {
            return null;
          } else {
            if (currentReorderBuffer == null) {
              currentReorderBuffer = new ReorderBuffer();
            }
            try (TokenBuffer tokenBuffer = new TokenBuffer(in)) {
              // Moves the parser to the end of the current event e.g. END_OBJECT
              tokenBuffer.copyCurrentStructure(in);
              currentReorderBuffer.savedFields.put(fn, tokenBuffer);
            }
            in.nextToken();
          }
        } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
        throw new AvroTypeException("Expected field name not found: " + fa.fname);
      }
    } else if (top == Symbol.FIELD_END) {
      if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
        in = currentReorderBuffer.origParser;
        currentReorderBuffer.origParser = null;
      }
    } else if (top == Symbol.RECORD_START) {
      if (in.getCurrentToken() == JsonToken.START_OBJECT) {
        in.nextToken();
        reorderBuffers.push(currentReorderBuffer);
        currentReorderBuffer = null;
      } else {
        throw error("record-start");
      }
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      // AVRO-2034 advance to the end of our object
      while (in.getCurrentToken() != JsonToken.END_OBJECT) {
        in.nextToken();
      }

      if (top == Symbol.RECORD_END) {
        if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
          throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
        }
        currentReorderBuffer = reorderBuffers.pop();
      }

      // AVRO-2034 advance beyond the end object for the next record.
      in.nextToken();

    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  private AvroTypeException error(String type) {
    return new AvroTypeException("Expected " + type + ". Got " + in.getCurrentToken());
  }

}
