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
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.IO;

namespace Avro.Test.Utils
{
    [TestFixture]
    public class CaseFinderTests
    {
        [Test]
        public void TestBadDocLabel1()
        {
            List<Object[]> result = new List<Object[]>();
            Assert.Throws<ArgumentException>(
                () => CaseFinder.Find(Mk("<<INPUT blah"), "", result)
                );
        }

        [Test]
        public void TestBadDocLabel2()
        {
            List<Object[]> result = new List<Object[]>();
            Assert.Throws<ArgumentException>(
                () => CaseFinder.Find(Mk("<<INPUT blah"), "kill-er", result)
                );
        }

        [Test]
        public void TestBadSingleLineHeredoc()
        {
            List<Object[]> result = new List<Object[]>();
            Assert.Throws<IOException>(
                () => CaseFinder.Find(Mk("<<INPUTblah"), "foo", result)
                );
        }

        [Test]
        public void TestUnterminatedHeredoc()
        {
            List<Object[]> result = new List<Object[]>();
            Assert.Throws<IOException>(
                () => CaseFinder.Find(Mk("<<INPUT"), "foo", result)
                );
        }

        [Test, TestCaseSource("OutputTestCases")]
        public void TestOutput(string input, string label, List<object[]> expectedOutput)
        {
            List<Object[]> result = new List<Object[]>();
            CaseFinder.Find(Mk(input), label, result);
            Assert.True(Eq(result, expectedOutput), Pr(result));
        }

        private static List<Object[]> OutputTestCases()
        {
            List<Object[]> result = new List<Object[]>();
            result.Add(new Object[] { "", "foo", new List<object[]> { } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT b", "OUTPUT", new List<object[]> { new object[] {"a","b"} } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT b\n", "OUTPUT", new List<object[]> { new object[] { "a", "b" } } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT b\n\n", "OUTPUT", new List<object[]> { new object[] { "a", "b" } } });
            result.Add(new Object[] { "<<INPUT a\r<<OUTPUT b", "OUTPUT", new List<object[]> { new object[] { "a", "b" } } });
            result.Add(new Object[] { "// This is a test\n<<INPUT a\n\n\n<<OUTPUT b", "OUTPUT", new List<object[]> { new object[] { "a", "b" } } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT\nb\nOUTPUT", "OUTPUT", new List<object[]> { new object[] { "a", "b" } } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT\nb\n\nOUTPUT", "OUTPUT", new List<object[]> { new object[] { "a", "b\n" } } });
            result.Add(new Object[] { "<<INPUT a\n<<OUTPUT\n\n  b  \n\nOUTPUT", "OUTPUT", new List<object[]> { new object[] { "a", "\n  b  \n" } } });
            result.Add(new Object[] { "<<INPUT a\n<<O b\n<<INPUT c\n<<O d", "O", new List<object[]> { new object[] { "a", "b" }, new object[] { "c", "d" } } });
            result.Add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT c\n<<O d", "O", new List<object[]> { new object[] { "a", "b" }, new object[] { "c", "d" } } });
            result.Add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT c\n<<O d", "F", new List<object[]> { new object[] { "a", "z" } } });
            result.Add(new Object[] { "<<INPUT a\n<<O b\n<<F z\n<<INPUT\nc\nINPUT\n<<O d\n<<INPUT e", "INPUT", new List<object[]> { new object[] { "a", null }, new object[] { "c", null }, new object[] { "e", null } } });
            return result;
        }

        private StreamReader Mk(string s)
        {
            byte[] byteArray = Encoding.ASCII.GetBytes(s);
            MemoryStream stream = new MemoryStream(byteArray);
            return new StreamReader(stream);
        }

        private string Pr(List<object[]> t)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("{ ");
            bool firstTime = true;
            foreach (var obj in t)
            {
                if (!firstTime)
                {
                    sb.Append(", ");
                }
                else
                {
                    firstTime = false;
                }
                sb.Append("{ \"").Append(obj[0]).Append("\", \"").Append(obj[1]).Append("\" }");
            }
            sb.Append("}");
            return sb.ToString();
        }

        private bool Eq(List<object []> l1, List<object []> l2)
        {
            if (l1 == null || l2 == null)
            {
                return l1 == l2;
            }
            if (l1.Count != l2.Count)
            {
                return false;
            }
            for (int i = 0; i < l1.Count; i++)
            {
                if (!ArraysEqual(l1[i], l2[i]))
                {
                    return false;
                }
            }
            return true;
        }

        static bool ArraysEqual<T>(T[] a1, T[] a2)
        {
            if (ReferenceEquals(a1, a2))
                return true;

            if (a1 == null || a2 == null)
                return false;

            if (a1.Length != a2.Length)
                return false;

            EqualityComparer<T> comparer = EqualityComparer<T>.Default;
            for (int i = 0; i < a1.Length; i++)
            {
                if (!comparer.Equals(a1[i], a2[i])) return false;
            }
            return true;
        }

    }
}
