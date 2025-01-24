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
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using System.IO;
using Avro.Test.Utils;

namespace Avro.Test
{
    [TestFixture]
    public class SchemaNormalizationTests
    {
        private const long Empty64 = -4513414715797952619;
        private static readonly long One = -9223372036854775808;
        private static readonly byte[] Postfix = { 0, 0, 0, 0, 0, 0, 0, 0 };

        [Test, TestCaseSource("ProvideCanonicalTestCases")]
        public void CanonicalTest(string input, string expectedOutput)
        {
            Assert.AreEqual(expectedOutput, SchemaNormalization.ToParsingForm(Schema.Parse(input)));
        }

        [Test, TestCaseSource("ProvideFingerprintTestCases")]
        public void FingerprintTest(string input, string expectedOutput)
        {
            Schema s = Schema.Parse(input);
            long carefulFP = AltFingerprint(SchemaNormalization.ToParsingForm(s));
            Assert.AreEqual(long.Parse(expectedOutput), carefulFP);
            Assert.AreEqual(carefulFP, SchemaNormalization.ParsingFingerprint64(s));
        }

        private static List<object[]> ProvideFingerprintTestCases()
        {
            var dir = Path.GetDirectoryName(new Uri(typeof(SchemaNormalizationTests).Assembly.Location).LocalPath);
            var testsPath = Path.Combine(dir, "../../../../../../../../share/test/data/schema-tests.txt");
            using (StreamReader reader = new StreamReader(testsPath))
            {
                return CaseFinder.Find(reader, "fingerprint", new List<object[]>());
            }
        }

        private static List<object[]> ProvideCanonicalTestCases()
        {
            var dir = Path.GetDirectoryName(new Uri(typeof(SchemaNormalizationTests).Assembly.Location).LocalPath);
            var testsPath = Path.Combine(dir, "../../../../../../../../share/test/data/schema-tests.txt");
            using (StreamReader reader = new StreamReader(testsPath))
            {
                return CaseFinder.Find(reader, "canonical", new List<object[]>());
            }
        }

        private static long AltFingerprint(string s)
        {
            long tmp = AltExtended(Empty64, 64, One, Encoding.UTF8.GetBytes(s));
            return AltExtended(Empty64, 64, tmp, Postfix);
        }

        private static long AltExtended(long poly, int degree, long fp, byte[] b)
        {
            long overflowBit = 1L << (64 - degree);
            for (int i = 0; i < b.Length; i++)
            {
                for (int j = 1; j < 129; j = j << 1)
                {
                    bool overflow = (0 != (fp & overflowBit));
                    fp = (long) (((ulong) fp) >> 1);
                    if (0 != (j & b[i]))
                    {
                        fp |= One;
                    }
                    if (overflow)
                    {
                        fp ^= poly;
                    }
                }
            }
            return fp;
        }
    }
}
