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

/**
 * A parser that capable of skipping as well read and write. This class is used
 * by decoders who (unlink encoders) are required to implement methods to skip.
 */
public class SkipParser extends Parser {
  /**
   * The clients implement this interface to skip symbols and actions.
   */
  public interface SkipHandler {
    /**
     * Skips the action at the top of the stack.
     */
    void skipAction() throws IOException;

    /**
     * Skips the symbol at the top of the stack.
     */
    void skipTopSymbol() throws IOException;
  }

  private final SkipHandler skipHandler;

  public SkipParser(Symbol root, ActionHandler symbolHandler, SkipHandler skipHandler) throws IOException {
    super(root, symbolHandler);
    this.skipHandler = skipHandler;
  }

  /**
   * Skips data by calling <code>skipXyz</code> or <code>readXyz</code> methods on
   * <code>this</code>, until the parser stack reaches the target level.
   */
  public final void skipTo(int target) throws IOException {
    outer: while (target < pos) {
      Symbol top = stack[pos - 1];
      while (top.kind != Symbol.Kind.TERMINAL) {
        if (top.kind == Symbol.Kind.IMPLICIT_ACTION || top.kind == Symbol.Kind.EXPLICIT_ACTION) {
          skipHandler.skipAction();
        } else {
          --pos;
          pushProduction(top);
        }
        continue outer;
      }
      skipHandler.skipTopSymbol();
    }
  }

  /**
   * Skips the repeater at the top the stack.
   */
  public final void skipRepeater() throws IOException {
    int target = pos;
    Symbol repeater = stack[--pos];
    assert repeater.kind == Symbol.Kind.REPEATER;
    pushProduction(repeater);
    skipTo(target);
  }

  /**
   * Pushes the given symbol on to the skip and skips it.
   * 
   * @param symToSkip The symbol that should be skipped.
   */
  public final void skipSymbol(Symbol symToSkip) throws IOException {
    int target = pos;
    pushSymbol(symToSkip);
    skipTo(target);
  }
}
