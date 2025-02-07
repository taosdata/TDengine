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

package org.apache.avro.perf.test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

public abstract class BasicState {

  public static final int BATCH_SIZE = 10000;

  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();
  private static final EncoderFactory ENCODER_FACTORY = new EncoderFactory();

  private static final OutputStream NULL_OUTPUTSTREAM = new NullOutputStream();

  private final Random random = new Random(13L);
  private final int batchSize = BATCH_SIZE;

  private BinaryDecoder reuseDecoder;
  private BinaryEncoder reuseEncoder;
  private BinaryEncoder reuseBlockingEncoder;

  public BasicState() {
    this.reuseDecoder = null;
  }

  protected Random getRandom() {
    return this.random;
  }

  protected Decoder newDecoder(final byte[] buf) {
    this.reuseDecoder = DECODER_FACTORY.binaryDecoder(buf, this.reuseDecoder);
    return this.reuseDecoder;
  }

  protected Encoder newEncoder(boolean direct, OutputStream out) throws IOException {
    this.reuseEncoder = (direct ? ENCODER_FACTORY.directBinaryEncoder(out, this.reuseEncoder)
        : ENCODER_FACTORY.binaryEncoder(out, this.reuseEncoder));
    return this.reuseEncoder;
  }

  protected Encoder newEncoder(int blockSize, OutputStream out) throws IOException {
    this.reuseBlockingEncoder = ENCODER_FACTORY.configureBlockSize(blockSize).blockingBinaryEncoder(out,
        this.reuseBlockingEncoder);
    return this.reuseBlockingEncoder;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  protected OutputStream getNullOutputStream() {
    return NULL_OUTPUTSTREAM;
  }

  private static class NullOutputStream extends OutputStream {
    @Override
    public void write(int b) throws IOException {
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
    }

    @Override
    public void write(byte[] b) throws IOException {
    }
  }

}
