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
package org.apache.avro.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.NoPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;

/* causes lazier classloader initialization of ZStandard libraries, so that
 * we get NoClassDefFoundError when we try and use the Codec's compress
 * or decompress methods rather than when we instantiate it */
final class ZstandardLoader {

  static InputStream input(InputStream compressed, boolean useBufferPool) throws IOException {
    BufferPool pool = useBufferPool ? RecyclingBufferPool.INSTANCE : NoPool.INSTANCE;
    return new ZstdInputStreamNoFinalizer(compressed, pool);
  }

  static OutputStream output(OutputStream compressed, int level, boolean checksum, boolean useBufferPool)
      throws IOException {
    int bounded = Math.max(Math.min(level, Zstd.maxCompressionLevel()), Zstd.minCompressionLevel());
    BufferPool pool = useBufferPool ? RecyclingBufferPool.INSTANCE : NoPool.INSTANCE;
    ZstdOutputStreamNoFinalizer zstdOutputStream = new ZstdOutputStreamNoFinalizer(compressed, pool).setLevel(bounded);
    zstdOutputStream.setCloseFrameOnFlush(false);
    zstdOutputStream.setChecksum(checksum);
    return zstdOutputStream;
  }
}
