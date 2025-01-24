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
package org.apache.avro.tool;

import java.lang.reflect.Method;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.ZstandardCodec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class TestUtil {

  @Rule
  public TestName name = new TestName();

  private void zstandardCompressionLevel(int level) throws Exception {
    OptionParser optParser = new OptionParser();
    OptionSpec<String> codecOpt = Util.compressionCodecOption(optParser);
    OptionSpec<Integer> levelOpt = Util.compressionLevelOption(optParser);

    OptionSet opts = optParser.parse(new String[] { "--codec", "zstandard", "--level", String.valueOf(level) });
    CodecFactory codecFactory = Util.codecFactory(opts, codecOpt, levelOpt);
    Method createInstance = CodecFactory.class.getDeclaredMethod("createInstance");
    createInstance.setAccessible(true);
    Codec codec = (ZstandardCodec) createInstance.invoke(codecFactory);
    Assert.assertEquals(String.format("zstandard[%d]", level), codec.toString());
  }

  @Test
  public void testCodecFactoryZstandardCompressionLevel() throws Exception {
    zstandardCompressionLevel(1);
    zstandardCompressionLevel(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
  }
}
