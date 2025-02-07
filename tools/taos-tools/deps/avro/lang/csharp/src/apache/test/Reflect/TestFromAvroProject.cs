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
using System.Collections.Generic;
using Avro;
using Avro.IO;
using Avro.Generic;
using Avro.Specific;
using Avro.Reflect;
using NUnit.Framework;

namespace Avro.Test
{
    public enum MyEnum
    {
        A,
        B,
        C
    }
    public class A
    {
        public long f1 { get; set; }
    }
    public class newRec
    {
        public long f1 { get; set; }
    }

    public class Z
    {
        public int? myUInt { get; set; }

        public long? myULong { get; set; }

        public bool? myUBool { get; set; }

        public double? myUDouble { get; set; }

        public float? myUFloat { get; set; }

        public byte[] myUBytes { get; set; }

        public string myUString { get; set; }

        public int myInt { get; set; }

        public long myLong { get; set; }

        public bool myBool { get; set; }

        public double myDouble { get; set; }

        public float myFloat { get; set; }

        public byte[] myBytes { get; set; }

        public string myString { get; set; }

        public object myNull { get; set; }

        public byte[] myFixed { get; set; }

        public A myA { get; set; }

        public A myNullableA { get; set; }

        public MyEnum myE { get; set; }

        public List<byte[]> myArray { get; set; }

        public List<newRec> myArray2 { get; set; }

        public Dictionary<string, string> myMap { get; set; }

        public Dictionary<string, newRec> myMap2 { get; set; }

        public object myObject { get; set; }

        public List<List<object>> myArray3 { get; set; }
    }
    [TestFixture]
    public class TestFromAvroProject
    {
        private const string _avroTestSchemaV1 = @"{
        ""protocol"" : ""MyProtocol"",
        ""namespace"" : ""com.foo"",
        ""types"" :
        [
            {
                ""type"" : ""record"",
                ""name"" : ""A"",
                ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long"" } ]
            },
            {
                ""type"" : ""enum"",
                ""name"" : ""MyEnum"",
                ""symbols"" : [ ""A"", ""B"", ""C"" ]
            },
            {
                ""type"": ""fixed"",
                ""size"": 16,
                ""name"": ""MyFixed""
            },
            {
                ""type"" : ""record"",
                ""name"" : ""Z"",
                ""fields"" :
                [
                    { ""name"" : ""myUInt"", ""type"" : [ ""int"", ""null"" ] },
                    { ""name"" : ""myULong"", ""type"" : [ ""long"", ""null"" ] },
                    { ""name"" : ""myUBool"", ""type"" : [ ""boolean"", ""null"" ] },
                    { ""name"" : ""myUDouble"", ""type"" : [ ""double"", ""null"" ] },
                    { ""name"" : ""myUFloat"", ""type"" : [ ""float"", ""null"" ] },
                    { ""name"" : ""myUBytes"", ""type"" : [ ""bytes"", ""null"" ] },
                    { ""name"" : ""myUString"", ""type"" : [ ""string"", ""null"" ] },
                    { ""name"" : ""myInt"", ""type"" : ""int"" },
                    { ""name"" : ""myLong"", ""type"" : ""long"" },
                    { ""name"" : ""myBool"", ""type"" : ""boolean"" },
                    { ""name"" : ""myDouble"", ""type"" : ""double"" },
                    { ""name"" : ""myFloat"", ""type"" : ""float"" },
                    { ""name"" : ""myBytes"", ""type"" : ""bytes"" },
                    { ""name"" : ""myString"", ""type"" : ""string"" },
                    { ""name"" : ""myNull"", ""type"" : ""null"" },
                    { ""name"" : ""myFixed"", ""type"" : ""MyFixed"" },
                    { ""name"" : ""myA"", ""type"" : ""A"" },
                    { ""name"" : ""myNullableA"", ""type"" : [ ""null"", ""A"" ] },
                    { ""name"" : ""myE"", ""type"" : ""MyEnum"" },
                    { ""name"" : ""myArray"", ""type"" : { ""type"" : ""array"", ""items"" : ""bytes"" } },
                    { ""name"" : ""myArray2"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""record"", ""name"" : ""newRec"", ""fields"" : [ { ""name"" : ""f1"", ""type"" : ""long""} ] } } },
                    { ""name"" : ""myMap"", ""type"" : { ""type"" : ""map"", ""values"" : ""string"" } },
                    { ""name"" : ""myMap2"", ""type"" : { ""type"" : ""map"", ""values"" : ""newRec"" } },
                    { ""name"" : ""myObject"", ""type"" : [ ""MyEnum"", ""A"", ""null"" ] },
                    { ""name"" : ""myArray3"", ""type"" : { ""type"" : ""array"", ""items"" : { ""type"" : ""array"", ""items"" : [ ""double"", ""string"", ""null"" ] } } }
                ]
            }
        ]
        }";

        public Z SerializeDeserialize(Z z)
        {
            try
            {
                Protocol protocol = Protocol.Parse(_avroTestSchemaV1);
                Schema schema = null;
                foreach (var s in protocol.Types)
                {
                    if (s.Name == "Z")
                    {
                        schema = s;
                    }
                }

                var avroWriter = new ReflectWriter<Z>(schema);
                var avroReader = new ReflectReader<Z>(schema, schema);

                byte[] serialized;

                using (var stream = new MemoryStream(256))
                {
                    avroWriter.Write(z, new BinaryEncoder(stream));
                    serialized = stream.ToArray();
                }

                Z deserialized = null;
                using (var stream = new MemoryStream(serialized))
                {
                    deserialized = avroReader.Read(default(Z), new BinaryDecoder(stream));
                }

                return deserialized;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                throw;
            }
        }

        [TestCase]
        public void DefaultZ()
        {
            var z = new Z()
            {
                myBytes = new byte[10],
                myString = "123",
                myFixed = new byte[16],
                myA = new A(),
                myArray = new List<byte[]>(),
                myArray2 = new List<newRec>(),
                myMap = new Dictionary<string, string>(),
                myMap2 = new Dictionary<string, newRec>(),
                myArray3 = new List<List<object>>()
            };

            var zz = SerializeDeserialize(z);
            DoAssertions(z, zz);

        }

        private void DoAssertions(Z z, Z zz)
        {
                        Assert.IsNotNull(zz);
            Assert.AreEqual(z.myUInt, zz.myUInt);
            Assert.AreEqual(z.myULong, zz.myULong);
            Assert.AreEqual(z.myUBool, zz.myUBool);
            Assert.AreEqual(z.myUDouble, zz.myUDouble);
            Assert.AreEqual(z.myUFloat, zz.myUFloat);
            if (z.myUBytes == null)
            {
                Assert.IsNull(zz.myUBytes);
            }
            else
            {
                Assert.IsNotNull(zz.myUBytes);
                Assert.IsTrue(z.myUBytes.SequenceEqual(zz.myUBytes));
            }
            Assert.AreEqual(z.myUString, zz.myUString);
            Assert.AreEqual(z.myInt, zz.myInt);
            Assert.AreEqual(z.myLong, zz.myLong);
            Assert.AreEqual(z.myBool, zz.myBool);
            Assert.AreEqual(z.myDouble, zz.myDouble);
            Assert.AreEqual(z.myFloat, zz.myFloat);
            if (z.myBytes == null)
            {
                Assert.IsNull(zz.myBytes);
            }
            else
            {
                Assert.IsNotNull(zz.myBytes);
                Assert.IsTrue(z.myBytes.SequenceEqual(zz.myBytes));
            }
            Assert.AreEqual(z.myString, zz.myString);
            Assert.AreEqual(z.myNull, zz.myNull);
            if (z.myFixed == null)
            {
                Assert.IsNull(zz.myFixed);
            }
            else
            {
                Assert.IsNotNull(zz.myFixed);
                Assert.AreEqual(z.myFixed.Length, zz.myFixed.Length);
                Assert.IsTrue(z.myFixed.SequenceEqual(zz.myFixed));
            }
            if (z.myA == null)
            {
                Assert.IsNull(zz.myA);
            }
            else
            {
                Assert.IsNotNull(zz.myA);
                Assert.AreEqual(z.myA.f1, zz.myA.f1);
            }
            if (z.myNullableA == null)
            {
                Assert.IsNull(zz.myNullableA);
            }
            else
            {
                Assert.IsNotNull(zz.myNullableA);
                Assert.AreEqual(z.myNullableA.f1, zz.myNullableA.f1);
            }
            Assert.AreEqual(z.myE, zz.myE);
            if (z.myArray == null)
            {
                Assert.IsNull(zz.myArray);
            }
            else
            {
                Assert.IsNotNull(zz.myArray);
                Assert.AreEqual(z.myArray.Count, zz.myArray.Count);
                z.myArray.ForEach(zz.myArray, (i1,i2)=>Assert.IsTrue(i1.SequenceEqual(i2)));
            }
            if (z.myArray2 == null)
            {
                Assert.IsNull(zz.myArray2);
            }
            else
            {
                Assert.IsNotNull(zz.myArray2);
                Assert.AreEqual(z.myArray2.Count, zz.myArray2.Count);
                z.myArray2.ForEach(zz.myArray2, (i1,i2)=>Assert.AreEqual(i1.f1, i2.f1));
            }
            if (z.myArray3 == null)
            {
                Assert.IsNull(zz.myArray3);
            }
            else
            {
                Assert.IsNotNull(zz.myArray3);
                Assert.AreEqual(z.myArray3.Count, zz.myArray3.Count);
                z.myArray3.ForEach(zz.myArray3, (i1,i2)=>i1.ForEach(i2, (j1,j2)=>Assert.AreEqual(j1,j2)));
            }
            if (z.myMap == null)
            {
                Assert.IsNull(zz.myMap);
            }
            else
            {
                Assert.IsNotNull(zz.myMap);
                Assert.AreEqual(z.myMap.Count, zz.myMap.Count);
                z.myMap.ForEach(zz.myMap, (i1,i2)=>{Assert.AreEqual(i1.Key, i2.Key); Assert.AreEqual(i1.Value, i2.Value);});
            }
            if (z.myMap2 == null)
            {
                Assert.IsNull(zz.myMap2);
            }
            else
            {
                Assert.IsNotNull(zz.myMap2);
                Assert.AreEqual(z.myMap2.Count, zz.myMap2.Count);
                z.myMap2.ForEach(zz.myMap2, (i1,i2)=>{Assert.AreEqual(i1.Key, i2.Key); Assert.AreEqual(i1.Value.f1, i2.Value.f1);});
            }
             if (z.myObject == null)
            {
                Assert.IsNull(zz.myObject);
            }
            else
            {
                Assert.IsNotNull(zz.myObject);
                Assert.IsTrue(z.myObject.GetType() == zz.myObject.GetType());
            }
        }
        [TestCase]
        public void PopulatedZ()
        {
            var z = new Z()
            {
                myUInt = 1,
                myULong = 2L,
                myUBool = true,
                myUDouble = 3.14,
                myUFloat = (float)1.59E-3,
                myUBytes = new byte[3] { 0x01, 0x02, 0x03 },
                myUString = "abc",
                myInt = 1,
                myLong = 2L,
                myBool = true,
                myDouble = 3.14,
                myFloat = (float)1.59E-2,
                myBytes = new byte[3] { 0x01, 0x02, 0x03 },
                myString = "def",
                myNull = null,
                myFixed = new byte[16] { 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04 },
                myA = new A() { f1 = 3L },
                myNullableA = new A() { f1 = 4L },
                myE = MyEnum.B,
                myArray = new List<byte[]>() { new byte[] { 0x01, 0x02, 0x03, 0x04 } },
                myArray2 = new List<newRec>() { new newRec() { f1 = 4L } },
                myMap = new Dictionary<string, string>()
                {
                    ["abc"] = "123"
                },
                myMap2 = new Dictionary<string, newRec>()
                {
                    ["abc"] = new newRec() { f1 = 5L }
                },
                myObject = new A() { f1 = 6L },
                myArray3 = new List<List<object>>() { new List<object>() { 7.0, "def" } }
            };

            var zz = SerializeDeserialize(z);
            DoAssertions(z, zz);
        }
    }
}
