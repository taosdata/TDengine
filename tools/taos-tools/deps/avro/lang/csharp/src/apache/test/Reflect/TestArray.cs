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
using System.Collections.Concurrent;
using System.IO;
using Avro.IO;
using Avro.Reflect;
using NUnit.Framework;
using System.Collections;

namespace Avro.Test
{


    [TestFixture]
    public class TestArray
    {
        private class ListRec
        {
            public string S { get; set; }
        }

        private const string _simpleList = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""array"",
            ""doc"": ""A simple list with a string."",
            ""name"": ""A"",
            ""items"": ""string""
        }";

        private const string _recordList = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""array"",
            ""helper"": ""arrayOfA"",
            ""items"": {
                ""type"": ""record"",
                ""doc"": ""A simple type with a fixed."",
                ""name"": ""A"",
                ""fields"": [
                    { ""name"" : ""S"", ""type"" : ""string"" }
                ]
            }
        }";



        [TestCase]
        public void ListTest()
        {
            var schema = Schema.Parse(_simpleList);
            var fixedRecWrite = new List<string>() {"value"};

            var writer = new ReflectWriter<List<string>>(schema);
            var reader = new ReflectReader<List<string>>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                Assert.AreEqual(fixedRecWrite[0],fixedRecRead[0]);
            }
        }

        [TestCase]
        public void ListRecTest()
        {
            var schema = Schema.Parse(_recordList);
            var fixedRecWrite = new List<ListRec>() { new ListRec() { S = "hello"}};

            var writer = new ReflectWriter<List<ListRec>>(schema);
            var reader = new ReflectReader<List<ListRec>>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                Assert.AreEqual(fixedRecWrite[0].S,fixedRecRead[0].S);
            }
        }

        public class ConcurrentQueueHelper<T> : ArrayHelper
        {

            /// <summary>
            /// Return the number of elements in the array.
            /// </summary>
            /// <value></value>
            public override int Count()
            {
                ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
                return e.Count;
            }
            /// <summary>
            /// Add an element to the array.
            /// </summary>
            /// <value></value>
            public override void Add(object o)
            {
                ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
                e.Enqueue((T)o);
            }
            /// <summary>
            /// Clear the array.
            /// </summary>
            /// <value></value>
            public override void Clear()
            {
                ConcurrentQueue<T> e = (ConcurrentQueue<T>)Enumerable;
#if NET461
                while (e.TryDequeue(out _)) { }
#else
                e.Clear();
#endif
            }

            /// <summary>
            /// Type of the array to create when deserializing
            /// </summary>
            /// <value></value>
            public override Type ArrayType
            {
                get => typeof(ConcurrentQueue<>);
            }

            /// <summary>
            /// Constructor
            /// </summary>
            public ConcurrentQueueHelper(IEnumerable enumerable) : base(enumerable)
            {
                Enumerable = enumerable;
            }
        }

        private class ConcurrentQueueRec
        {
            public string S { get; set; }
        }


        [TestCase]
        public void ConcurrentQueueTest()
        {
            var schema = Schema.Parse(_recordList);
            var fixedRecWrite = new ConcurrentQueue<ConcurrentQueueRec>();
            fixedRecWrite.Enqueue(new ConcurrentQueueRec() { S = "hello"});
            var cache = new ClassCache();
            cache.AddArrayHelper("arrayOfA", typeof(ConcurrentQueueHelper<ConcurrentQueueRec>));
            var writer = new ReflectWriter<ConcurrentQueue<ConcurrentQueueRec>>(schema, cache);
            var reader = new ReflectReader<ConcurrentQueue<ConcurrentQueueRec>>(schema, schema, cache);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.Count == 1);
                ConcurrentQueueRec wRec = null;
                fixedRecWrite.TryDequeue(out wRec);
                Assert.NotNull(wRec);
                ConcurrentQueueRec rRec = null;
                fixedRecRead.TryDequeue(out rRec);
                Assert.NotNull(rRec);
                Assert.AreEqual(wRec.S,rRec.S);
            }
        }

        private const string _multiList = @"
        {
            ""type"": ""record"",
            ""doc"": ""Multiple arrays."",
            ""name"": ""A"",
            ""fields"": [
                { ""name"" : ""one"", ""type"" :
                    {
                        ""type"": ""array"",
                        ""items"": ""string""
                    }
                },
                { ""name"" : ""two"", ""type"" :
                    {
                        ""type"": ""array"",
                        ""helper"": ""twoArray"",
                        ""items"": ""string""
                    }
                }
            ]
        }";

        private class MultiList
        {
            public List<string> one {get;set;}
            public ConcurrentQueue<string> two {get;set;}
        }
        [TestCase]
        public void MultiQueueTest()
        {
            var schema = Schema.Parse(_multiList);
            var fixedRecWrite = new MultiList() { one = new List<string>(), two = new ConcurrentQueue<string>() };
            fixedRecWrite.one.Add("hola");
            fixedRecWrite.two.Enqueue("hello");
            var cache = new ClassCache();
            cache.AddArrayHelper("twoArray", typeof(ConcurrentQueueHelper<string>));
            var writer = new ReflectWriter<MultiList>(schema, cache);
            var reader = new ReflectReader<MultiList>(schema, schema, cache);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                var fixedRecRead = reader.Read(new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.one.Count == 1);
                Assert.IsTrue(fixedRecRead.two.Count == 1);
                Assert.AreEqual(fixedRecWrite.one[0], fixedRecRead.one[0]);
                string wRec = null;
                fixedRecWrite.two.TryDequeue(out wRec);
                Assert.NotNull(wRec);
                string rRec = null;
                fixedRecRead.two.TryDequeue(out rRec);
                Assert.NotNull(rRec);
                Assert.AreEqual(wRec, rRec);
            }
        }
    }
}
