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

import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.AvroTypeException;

/**
 * Parser is the class that maintains the stack for parsing. This class is used
 * by encoders, which are not required to skip.
 */
public class Parser {
  /**
   * The parser knows how to handle the terminal and non-terminal symbols. But it
   * needs help from outside to handle implicit and explicit actions. The clients
   * implement this interface to provide this help.
   */
  public interface ActionHandler {
    /**
     * Handle the action symbol <tt>top</tt> when the <tt>input</tt> is sought to be
     * taken off the stack.
     *
     * @param input The input symbol from the caller of advance
     * @param top   The symbol at the top the stack.
     * @return <tt>null</tt> if advance() is to continue processing the stack. If
     *         not <tt>null</tt> the return value will be returned by advance().
     * @throws IOException
     */
    Symbol doAction(Symbol input, Symbol top) throws IOException;
  }

  protected final ActionHandler symbolHandler;
  protected Symbol[] stack;
  protected int pos;

  public Parser(Symbol root, ActionHandler symbolHandler) {
    this.symbolHandler = symbolHandler;
    this.stack = new Symbol[5]; // Start small to make sure expansion code works
    this.stack[0] = root;
    this.pos = 1;
  }

  /**
   * If there is no sufficient room in the stack, use this expand it.
   */
  private void expandStack() {
    stack = Arrays.copyOf(stack, stack.length + Math.max(stack.length, 1024));
  }

  /**
   * Recursively replaces the symbol at the top of the stack with its production,
   * until the top is a terminal. Then checks if the top symbol matches the
   * terminal symbol suppled <tt>terminal</tt>.
   *
   * @param input The symbol to match against the terminal at the top of the
   *              stack.
   * @return The terminal symbol at the top of the stack unless an implicit action
   *         resulted in another symbol, in which case that symbol is returned.
   */
  public final Symbol advance(Symbol input) throws IOException {
    for (;;) {
      Symbol top = stack[--pos];
      if (top == input) {
        return top; // A common case
      }

      Symbol.Kind k = top.kind;
      if (k == Symbol.Kind.IMPLICIT_ACTION) {
        Symbol result = symbolHandler.doAction(input, top);
        if (result != null) {
          return result;
        }
      } else if (k == Symbol.Kind.TERMINAL) {
        throw new AvroTypeException("Attempt to process a " + input + " when a " + top + " was expected.");
      } else if (k == Symbol.Kind.REPEATER && input == ((Symbol.Repeater) top).end) {
        return input;
      } else {
        pushProduction(top);
      }
    }
  }

  /**
   * Performs any implicit actions at the top the stack, expanding any production
   * (other than the root) that may be encountered. This method will fail if there
   * are any repeaters on the stack.
   *
   * @throws IOException
   */
  public final void processImplicitActions() throws IOException {
    while (pos > 1) {
      Symbol top = stack[pos - 1];
      if (top.kind == Symbol.Kind.IMPLICIT_ACTION) {
        pos--;
        symbolHandler.doAction(null, top);
      } else if (top.kind != Symbol.Kind.TERMINAL) {
        pos--;
        pushProduction(top);
      } else {
        break;
      }
    }
  }

  /**
   * Performs any "trailing" implicit actions at the top the stack.
   */
  public final void processTrailingImplicitActions() throws IOException {
    while (pos >= 1) {
      Symbol top = stack[pos - 1];
      if (top.kind == Symbol.Kind.IMPLICIT_ACTION && ((Symbol.ImplicitAction) top).isTrailing) {
        pos--;
        symbolHandler.doAction(null, top);
      } else {
        break;
      }
    }
  }

  /**
   * Pushes the production for the given symbol <tt>sym</tt>. If <tt>sym</tt> is a
   * repeater and <tt>input</tt> is either {@link Symbol#ARRAY_END} or
   * {@link Symbol#MAP_END} pushes nothing.
   *
   * @param sym
   */
  public final void pushProduction(Symbol sym) {
    Symbol[] p = sym.production;
    while (pos + p.length > stack.length) {
      expandStack();
    }
    System.arraycopy(p, 0, stack, pos, p.length);
    pos += p.length;
  }

  /**
   * Pops and returns the top symbol from the stack.
   */
  public Symbol popSymbol() {
    return stack[--pos];
  }

  /**
   * Returns the top symbol from the stack.
   */
  public Symbol topSymbol() {
    return stack[pos - 1];
  }

  /**
   * Pushes <tt>sym</tt> on to the stack.
   */
  public void pushSymbol(Symbol sym) {
    if (pos == stack.length) {
      expandStack();
    }
    stack[pos++] = sym;
  }

  /**
   * Returns the depth of the stack.
   */
  public int depth() {
    return pos;
  }

  public void reset() {
    pos = 1;
  }
}
