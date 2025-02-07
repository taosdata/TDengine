/**
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
using System;
using System.IO;
using NUnit.Framework;
using Avro.File;
using Avro.Generic;

namespace Avro.Test.Interop
{
    [TestFixture]
    [Category("Interop")]
    public class InteropDataTests
    {
        [TestCase("../../../../../../../../build/interop/data")]
        public void TestInterop(string inputDir)
        {
            // Resolve inputDir relative to the TestDirectory
            inputDir = Path.Combine(TestContext.CurrentContext.TestDirectory, inputDir);

            Assert.True(Directory.Exists(inputDir),
                "Input directory does not exist. Run `build.sh interop-data-generate` first.");

            foreach (var avroFile in Directory.EnumerateFiles(inputDir, "*.avro"))
            {
                var codec = Path.GetFileNameWithoutExtension(avroFile).Split('_');
                if (1 < codec.Length && !InteropDataConstants.SupportedCodecNames.Contains(codec[1]))
                {
                    Console.WriteLine($"Skipped: {avroFile}");
                    continue;
                }

                using(var reader = DataFileReader<GenericRecord>.OpenReader(avroFile))
                {
                    int i = 0;
                    foreach (var record in reader.NextEntries)
                    {
                        i++;
                        Assert.IsNotNull(record);
                    }
                    Assert.AreNotEqual(0, i);
                }

                Console.WriteLine($"Succeeded: {avroFile}");
            }
        }
    }
}