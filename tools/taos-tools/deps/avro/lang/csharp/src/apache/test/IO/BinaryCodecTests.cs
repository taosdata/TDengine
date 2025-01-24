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
using System.Buffers;
using NUnit.Framework;
using System.IO;
using System.Linq;
using System.Text;
using Avro.IO;

namespace Avro.Test
{
    using Decoder = Avro.IO.Decoder;
    using Encoder = Avro.IO.Encoder;
    delegate T Decode<T>(Decoder d);
    delegate void Skip<T>(Decoder d);
    delegate void Encode<T>(Encoder e, T t);

    /// <summary>
    /// Tests the BinaryEncoder and BinaryDecoder. This is pertty general set of test cases and hence
    /// can be used for any encoder and its corresponding decoder.
    /// </summary>
    [TestFixture]
    public class BinaryCodecTests
    {

        /// <summary>
        /// Writes an avro type T with value t into a stream using the encode method e
        /// and reads it back using the decode method d and verifies that
        /// the value read back is the same as the one written in.
        /// </summary>
        /// <typeparam name="T">Avro type to test</typeparam>
        /// <param name="t">Value for the Avro type to test.</param>
        /// <param name="r">The decode method</param>
        /// <param name="w">The encode method</param>
        private void TestRead<T>(T t, Decode<T> r, Encode<T> w, int size)
        {
            MemoryStream iostr = new MemoryStream();
            Encoder e = new BinaryEncoder(iostr);
            w(e, t);
            iostr.Flush();
            Assert.AreEqual(size, iostr.Length);
            iostr.Position = 0;
            Decoder d = new BinaryDecoder(iostr);
            T actual = r(d);
            Assert.AreEqual(t, actual);
            Assert.AreEqual(-1, iostr.ReadByte());
            iostr.Close();
        }

        /// <summary>
        /// Writes an avro type T with value t into a stream using the encode method e
        /// and reads it back using the decode method d and verifies that
        /// the value read back is the same as the one written in.
        /// </summary>
        /// <typeparam name="T">Avro type to test</typeparam>
        /// <param name="t">Value for the Avro type to test.</param>
        /// <param name="r">The skip method</param>
        /// <param name="w">The encode method</param>
        private void TestSkip<T>(T t, Skip<T> s, Encode<T> w, int size)
        {
            MemoryStream iostr = new MemoryStream();
            Encoder e = new BinaryEncoder(iostr);
            w(e, t);
            iostr.Flush();
            Assert.AreEqual(size, iostr.Length);
            iostr.Position = 0;
            Decoder d = new BinaryDecoder(iostr);
            s(d);
            Assert.AreEqual(-1, iostr.ReadByte());
            iostr.Close();
        }


        [TestCase(true)]
        [TestCase(false)]
        public void TestBoolean(bool b)
        {
            TestRead(b, (Decoder d) => d.ReadBoolean(), (Encoder e, bool t) => e.WriteBoolean(t), 1);
            TestSkip(b, (Decoder d) => d.SkipBoolean(), (Encoder e, bool t) => e.WriteBoolean(t), 1);
        }

        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(63, 1)]
        [TestCase(64, 2)]
        [TestCase(8191, 2)]
        [TestCase(8192, 3)]
        [TestCase(1048575, 3)]
        [TestCase(1048576, 4)]
        [TestCase(134217727, 4)]
        [TestCase(134217728, 5)]
        [TestCase(2147483647, 5)]
        [TestCase(-1, 1)]
        [TestCase(-64, 1)]
        [TestCase(-65, 2)]
        [TestCase(-8192, 2)]
        [TestCase(-8193, 3)]
        [TestCase(-1048576, 3)]
        [TestCase(-1048577, 4)]
        [TestCase(-134217728, 4)]
        [TestCase(-134217729, 5)]
        [TestCase(-2147483648, 5)]
        public void TestInt(int n, int size)
        {
            TestRead(n, (Decoder d) => d.ReadInt(), (Encoder e, int t) => e.WriteInt(t), size);
            TestSkip(n, (Decoder d) => d.SkipInt(), (Encoder e, int t) => e.WriteInt(t), size);
        }

        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(63, 1)]
        [TestCase(64, 2)]
        [TestCase(8191, 2)]
        [TestCase(8192, 3)]
        [TestCase(1048575, 3)]
        [TestCase(1048576, 4)]
        [TestCase(134217727, 4)]
        [TestCase(134217728, 5)]
        [TestCase(17179869183L, 5)]
        [TestCase(17179869184L, 6)]
        [TestCase(2199023255551L, 6)]
        [TestCase(2199023255552L, 7)]
        [TestCase(281474976710655L, 7)]
        [TestCase(281474976710656L, 8)]
        [TestCase(36028797018963967L, 8)]
        [TestCase(36028797018963968L, 9)]
        [TestCase(4611686018427387903L, 9)]
        [TestCase(4611686018427387904L, 10)]
        [TestCase(9223372036854775807L, 10)]
        [TestCase(-1, 1)]
        [TestCase(-64, 1)]
        [TestCase(-65, 2)]
        [TestCase(-8192, 2)]
        [TestCase(-8193, 3)]
        [TestCase(-1048576, 3)]
        [TestCase(-1048577, 4)]
        [TestCase(-134217728, 4)]
        [TestCase(-134217729, 5)]
        [TestCase(-17179869184L, 5)]
        [TestCase(-17179869185L, 6)]
        [TestCase(-2199023255552L, 6)]
        [TestCase(-2199023255553L, 7)]
        [TestCase(-281474976710656L, 7)]
        [TestCase(-281474976710657L, 8)]
        [TestCase(-36028797018963968L, 8)]
        [TestCase(-36028797018963969L, 9)]
        [TestCase(-4611686018427387904L, 9)]
        [TestCase(-4611686018427387905L, 10)]
        [TestCase(-9223372036854775808L, 10)]
        public void TestLong(long n, int size)
        {
            TestRead(n, (Decoder d) => d.ReadLong(), (Encoder e, long t) => e.WriteLong(t), size);
            TestSkip(n, (Decoder d) => d.SkipLong(), (Encoder e, long t) => e.WriteLong(t), size);
        }

        [TestCase(0.0f)]
        [TestCase(Single.MaxValue, Description = "Max value")]
        [TestCase(1.17549435E-38f, Description = "Min 'normal' value")]
        [TestCase(1.4e-45f, Description = "Min value")]
        public void TestFloat(float n)
        {
            TestRead(n, (Decoder d) => d.ReadFloat(), (Encoder e, float t) => e.WriteFloat(t), 4);
            TestSkip(n, (Decoder d) => d.SkipFloat(), (Encoder e, float t) => e.WriteFloat(t), 4);
        }

        [TestCase(0.0)]
        [TestCase(1.7976931348623157e+308, Description = "Max value")]
        [TestCase(2.2250738585072014E-308, Description = "Min 'normal' value")]
        [TestCase(4.9e-324, Description = "Min value")]
        public void TestDouble(double n)
        {
            TestRead(n, (Decoder d) => d.ReadDouble(), (Encoder e, double t) => e.WriteDouble(t), 8);
            TestSkip(n, (Decoder d) => d.SkipDouble(), (Encoder e, double t) => e.WriteDouble(t), 8);
        }


        [TestCase(0, 1)]
        [TestCase(5, 1)]
        [TestCase(63, 1)]
        [TestCase(64, 2)]
        [TestCase(8191, 2)]
        [TestCase(8192, 3)]
        public void TestBytes(int length, int overhead)
        {
            Random r = new Random();
            byte[] b = new byte[length];
            r.NextBytes(b);
            TestRead(b, (Decoder d) => d.ReadBytes(), (Encoder e, byte[] t) => e.WriteBytes(t), overhead + b.Length);
            TestSkip(b, (Decoder d) => d.SkipBytes(), (Encoder e, byte[] t) => e.WriteBytes(t), overhead + b.Length);
        }

        [TestCase("", 1)]
        [TestCase("hello", 1)]
        [TestCase("1234567890123456789012345678901234567890123456789012345678901234", 2)]
        [TestCase("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456", 2)]
        public void TestString(string n, int overhead)
        {
            TestRead(n, (Decoder d) => d.ReadString(), (Encoder e, string t) => e.WriteString(t), overhead + n.Length);
            TestSkip(n, (Decoder d) => d.SkipString(), (Encoder e, string t) => e.WriteString(t), overhead + n.Length);
        }

#if NETCOREAPP3_1_OR_GREATER
        [Test]
        public void TestStringReadIntoArrayPool()
        {
            const int maxFastReadLength = 4096;

            // Create a 16KB buffer in the Array Pool
            var largeBufferToSeedPool = ArrayPool<byte>.Shared.Rent(2 << 14);
            ArrayPool<byte>.Shared.Return(largeBufferToSeedPool);

            var n = string.Concat(Enumerable.Repeat("A", maxFastReadLength));
            var overhead = 2;

            TestRead(n, (Decoder d) => d.ReadString(), (Encoder e, string t) => e.WriteString(t), overhead + n.Length);
        }

        [Test]
        public void TestStringReadByBinaryReader()
        {
            const int overhead = 2;
            const int maxFastReadLength = 4096;
            const int expectedStringLength = maxFastReadLength + 1;
            var n = string.Concat(Enumerable.Repeat("A", expectedStringLength));

            TestRead(n, (Decoder d) => d.ReadString(), (Encoder e, string t) => e.WriteString(t), expectedStringLength + overhead);
        }
#endif

        [Test]
        public void TestInvalidInputWithNegativeStringLength()
        {
            using (MemoryStream iostr = new MemoryStream())
            {
                Encoder e = new BinaryEncoder(iostr);

                e.WriteLong(-1);

                iostr.Flush();
                iostr.Position = 0;
                Decoder d = new BinaryDecoder(iostr);

                var exception = Assert.Throws<AvroException>(() => d.ReadString());

                Assert.NotNull(exception);
                Assert.AreEqual("Can not deserialize a string with negative length!", exception.Message);
                iostr.Close();
            }
        }

        [Test]
        public void TestInvalidInputWithMaxIntAsStringLength()
        {
            using (MemoryStream iostr = new MemoryStream())
            {
                Encoder e = new BinaryEncoder(iostr);

                e.WriteLong(int.MaxValue);
                e.WriteBytes(Encoding.UTF8.GetBytes("SomeSmallString"));

                iostr.Flush();
                iostr.Position = 0;
                Decoder d = new BinaryDecoder(iostr);

                var exception = Assert.Throws<AvroException>(() => d.ReadString());

                Assert.NotNull(exception);
                Assert.AreEqual("String length is not supported!", exception.Message);
                iostr.Close();
            }
        }

        [Test]
        public void TestInvalidInputWithMaxArrayLengthAsStringLength()
        {
            using (MemoryStream iostr = new MemoryStream())
            {
                Encoder e = new BinaryEncoder(iostr);

#if NETCOREAPP3_1_OR_GREATER
                const int maximumArrayLength = 0x7FFFFFC7;
#else
                const int maximumArrayLength = 0x7FFFFFFF / 2;
#endif

                e.WriteLong(maximumArrayLength);
                e.WriteBytes(Encoding.UTF8.GetBytes("SomeSmallString"));

                iostr.Flush();
                iostr.Position = 0;
                Decoder d = new BinaryDecoder(iostr);

                var exception = Assert.Throws<AvroException>(() => d.ReadString());

                Assert.NotNull(exception);
                Assert.AreEqual("Could not read as many bytes from stream as expected!", exception.Message);
                iostr.Close();
            }
        }

        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(64, 2)]
        public void TestEnum(int n, int size)
        {
            TestRead(n, (Decoder d) => d.ReadEnum(), (Encoder e, int t) => e.WriteEnum(t), size);
            TestSkip(n, (Decoder d) => d.SkipEnum(), (Encoder e, int t) => e.WriteEnum(t), size);
        }

        [TestCase(1, new int[] { })]
        [TestCase(3, new int[] { 0 })]
        [TestCase(4, new int[] { 64 })]
        public void TestArray(int size, int[] entries)
        {
            TestRead(entries, (Decoder d) =>
            {
                int[] t = new int[entries.Length];
                int j = 0;
                for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                {
                    for (int i = 0; i < n; i++) { t[j++] = d.ReadInt(); }

                }
                return t;
            },
                (Encoder e, int[] t) =>
                {
                    e.WriteArrayStart();
                    e.SetItemCount(t.Length);
                    foreach (int i in t) { e.StartItem(); e.WriteInt(i); } e.WriteArrayEnd();
                }, size);

            TestSkip(entries, (Decoder d) =>
            {
                for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                {
                    for (int i = 0; i < n; i++) { d.SkipInt(); }

                }
            },
                (Encoder e, int[] t) =>
                {
                    e.WriteArrayStart();
                    e.SetItemCount(t.Length);
                    foreach (int i in t) { e.StartItem(); e.WriteInt(i); } e.WriteArrayEnd();
                }, size);
        }

        [TestCase(1, new string[] { })]
        [TestCase(6, new string[] { "a", "b" })]
        [TestCase(9, new string[] { "a", "b", "c", "" })]
        public void TestMap(int size, string[] entries)
        {
            TestRead(entries, (Decoder d) =>
            {
                string[] t = new string[entries.Length];
                int j = 0;
                for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                {
                    for (int i = 0; i < n; i++) { t[j++] = d.ReadString(); t[j++] = d.ReadString(); }

                }
                return t;
            },
                (Encoder e, string[] t) =>
                {
                    e.WriteArrayStart();
                    e.SetItemCount(t.Length / 2);
                    for (int i = 0; i < t.Length; i += 2)
                    {
                        e.StartItem(); e.WriteString(t[i]); e.WriteString(t[i + 1]);
                    }
                    e.WriteArrayEnd();
                }, size);

            TestSkip(entries, (Decoder d) =>
            {
                for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                {
                    for (int i = 0; i < n; i++) { d.SkipString(); d.SkipString(); }

                }
            },
                (Encoder e, string[] t) =>
                {
                    e.WriteArrayStart();
                    e.SetItemCount(t.Length / 2);
                    for (int i = 0; i < t.Length; i += 2)
                    {
                        e.StartItem(); e.WriteString(t[i]); e.WriteString(t[i + 1]);
                    }
                    e.WriteArrayEnd();
                }, size);
        }

        [TestCase(0, 1)]
        [TestCase(1, 1)]
        [TestCase(64, 2)]
        public void TestUnionIndex(int n, int size)
        {
            TestRead(n, (Decoder d) => d.ReadUnionIndex(), (Encoder e, int t) => e.WriteUnionIndex(t), size);
            TestSkip(n, (Decoder d) => d.SkipUnionIndex(), (Encoder e, int t) => e.WriteUnionIndex(t), size);
        }

        [TestCase(0)]
        [TestCase(1)]
        [TestCase(64)]
        public void TestFixed(int size)
        {
            byte[] b = new byte[size];
            new Random().NextBytes(b);
            TestRead(b, (Decoder d) => { byte[] t = new byte[size]; d.ReadFixed(t); return t; },
                (Encoder e, byte[] t) => e.WriteFixed(t), size);
            TestSkip(b, (Decoder d) => d.SkipFixed(size),
                (Encoder e, byte[] t) => e.WriteFixed(t), size);
        }
    }
}
