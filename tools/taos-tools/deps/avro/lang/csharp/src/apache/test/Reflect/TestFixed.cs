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

using System.IO;
using Avro.IO;
using Avro.Generic;
using Avro.Reflect;
using NUnit.Framework;

namespace Avro.Test
{

    [TestFixture]
    public class TestFixed
    {
            public class ByteArrayFixedRec
        {
            public byte[] myFixed { get; set; }
        }

        public class GenericFixedRec
        {
            public GenericFixed myFixed { get; set; }
        }

        public class GenericFixedConverter : TypedFieldConverter<byte[],GenericFixed>
        {
            public override GenericFixed From(byte[] o, Schema s)
            {
                return new GenericFixed(s as FixedSchema, o);
            }

            public override byte[] To(GenericFixed o, Schema s)
            {
                return o.Value;
            }
        }

        public class GenericFixedConverterRec
        {
            [AvroField(typeof(GenericFixedConverter))]
            public GenericFixed myFixed { get; set; }
        }
        private const string _fixedSchema = @"
        {
            ""namespace"": ""MessageTypes"",
            ""type"": ""record"",
            ""doc"": ""A simple type with a fixed."",
            ""name"": ""A"",
            ""fields"": [
                { ""name"" : ""myFixed"", ""type"" :
                    {
                        ""type"": ""fixed"",
                        ""size"": 16,
                        ""name"": ""MyFixed""
                    }
                }
            ]
        }";

        [TestCase]
        public void ByteArray()
        {
            var schema = Schema.Parse(_fixedSchema);
            var fixedRecWrite = new ByteArrayFixedRec() { myFixed = new byte[16] {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6} };
            var fixedRecBad = new ByteArrayFixedRec() { myFixed = new byte[10] };
            ByteArrayFixedRec fixedRecRead = null;

            var writer = new ReflectWriter<ByteArrayFixedRec>(schema);
            var reader = new ReflectReader<ByteArrayFixedRec>(schema, schema);

            Assert.Throws(typeof(AvroException), ()=> {
                using (var stream = new MemoryStream(256))
                {
                    writer.Write(fixedRecBad, new BinaryEncoder(stream));
                }
            });
            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                fixedRecRead = reader.Read(null, new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.myFixed.Length == 16);
                Assert.IsTrue(fixedRecWrite.myFixed.SequenceEqual(fixedRecRead.myFixed));
            }
        }

        [TestCase]
        public void GenericFixedConverterTest()
        {
            var schema = Schema.Parse(_fixedSchema);
            var rs = schema as RecordSchema;
            FixedSchema fs = null;
            foreach (var f in rs.Fields)
            {
                if (f.Name == "myFixed")
                {
                    fs = f.Schema as FixedSchema;
                }
            }
            var fixedRecWrite = new GenericFixedConverterRec() { myFixed = new GenericFixed(fs) {Value = new byte[16] {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6} }};
            GenericFixedConverterRec fixedRecRead = null;

            var writer = new ReflectWriter<GenericFixedConverterRec>(schema);
            var reader = new ReflectReader<GenericFixedConverterRec>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                fixedRecRead = reader.Read(null, new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.myFixed.Value.Length == 16);
                Assert.IsTrue(fixedRecWrite.myFixed.Value.SequenceEqual(fixedRecRead.myFixed.Value));
            }
        }

        [TestCase]
        public void GenericFixedDefaultConverter()
        {
            var schema = Schema.Parse(_fixedSchema);
            var rs = schema as RecordSchema;
            FixedSchema fs = null;
            foreach (var f in rs.Fields)
            {
                if (f.Name == "myFixed")
                {
                    fs = f.Schema as FixedSchema;
                }
            }
            var fixedRecWrite = new GenericFixedRec() { myFixed = new GenericFixed(fs) {Value = new byte[16] {1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6} }};
            GenericFixedRec fixedRecRead = null;

            ClassCache.AddDefaultConverter<byte[], GenericFixed>((a,s)=>new GenericFixed(s as FixedSchema, a), (p,s)=>p.Value);
            var writer = new ReflectWriter<GenericFixedRec>(schema);
            var reader = new ReflectReader<GenericFixedRec>(schema, schema);

            using (var stream = new MemoryStream(256))
            {
                writer.Write(fixedRecWrite, new BinaryEncoder(stream));
                stream.Seek(0, SeekOrigin.Begin);
                fixedRecRead = reader.Read(null, new BinaryDecoder(stream));
                Assert.IsTrue(fixedRecRead.myFixed.Value.Length == 16);
                Assert.IsTrue(fixedRecWrite.myFixed.Value.SequenceEqual(fixedRecRead.myFixed.Value));
            }
        }
    }
}
