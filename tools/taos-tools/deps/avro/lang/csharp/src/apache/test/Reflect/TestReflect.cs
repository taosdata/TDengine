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

using System.Collections;
using System.IO;
using NUnit.Framework;
using Avro.IO;
using Avro.Reflect;

namespace Avro.Test
{
    [TestFixture]
    class TestReflect
    {

        enum EnumResolutionEnum
        {
            THIRD,
            FIRST,
            SECOND
        }

        class EnumResolutionRecord
        {
            public EnumResolutionEnum enumType { get; set; }
        }

        [TestCase]
        public void TestEnumResolution()
        {
            Schema writerSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," +
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\": \"EnumType\", \"symbols\": [\"FIRST\", \"SECOND\"]} }]}");

            var testRecord = new EnumResolutionRecord();

            Schema readerSchema = Schema.Parse("{\"type\":\"record\",\"name\":\"EnumRecord\",\"namespace\":\"Avro.Test\"," +
                                        "\"fields\":[{\"name\":\"enumType\",\"type\": { \"type\": \"enum\", \"name\":" +
                                        " \"EnumType\", \"symbols\": [\"THIRD\", \"FIRST\", \"SECOND\"]} }]}");;
            testRecord.enumType = EnumResolutionEnum.SECOND;

            // serialize
            var stream = serialize(writerSchema, testRecord);

            // deserialize
            var rec2 = deserialize<EnumResolutionRecord>(stream, writerSchema, readerSchema);
            Assert.AreEqual( EnumResolutionEnum.SECOND, rec2.enumType );
        }

        private static S deserialize<S>(Stream ms, Schema ws, Schema rs) where S : class
        {
            long initialPos = ms.Position;
            var r = new ReflectReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(ms);
            S output = r.Read(null, d);
            Assert.AreEqual(ms.Length, ms.Position); // Ensure we have read everything.
            checkAlternateDeserializers(output, ms, initialPos, ws, rs);
            return output;
        }

        private static void checkAlternateDeserializers<S>(S expected, Stream input, long startPos, Schema ws, Schema rs) where S : class
        {
            input.Position = startPos;
            var reader = new ReflectReader<S>(ws, rs);
            Decoder d = new BinaryDecoder(input);
            S output = reader.Read(null, d);
            Assert.AreEqual(input.Length, input.Position); // Ensure we have read everything.
            AssertReflectRecordEqual(rs, expected, ws, output, reader.Reader.ClassCache);
        }

        private static Stream serialize<T>(Schema ws, T actual)
        {
            var ms = new MemoryStream();
            Encoder e = new BinaryEncoder(ms);
            var w = new ReflectWriter<T>(ws);
            w.Write(actual, e);
            ms.Flush();
            ms.Position = 0;
            checkAlternateSerializers(ms.ToArray(), actual, ws);
            return ms;
        }

        private static void checkAlternateSerializers<T>(byte[] expected, T value, Schema ws)
        {
            var ms = new MemoryStream();
            var writer = new ReflectWriter<T>(ws);
            var e = new BinaryEncoder(ms);
            writer.Write(value, e);
            var output = ms.ToArray();

            Assert.AreEqual(expected.Length, output.Length);
            Assert.True(expected.SequenceEqual(output));
        }

        private static void AssertReflectRecordEqual(Schema schema1, object rec1, Schema schema2, object rec2, ClassCache cache)
        {
            var recordSchema = (RecordSchema) schema1;
            foreach (var f in recordSchema.Fields)
            {
                var rec1Val = cache.GetClass(recordSchema).GetValue(rec1, f);
                var rec2Val = cache.GetClass(recordSchema).GetValue(rec2, f);
                if (rec1Val.GetType().IsClass)
                {
                    AssertReflectRecordEqual(f.Schema, rec1Val, f.Schema, rec2Val, cache);
                }
                else if (rec1Val is IList)
                {
                    var schema1List = f.Schema as ArraySchema;
                    var rec1List = (IList) rec1Val;
                    if( rec1List.Count > 0 )
                    {
                        var rec2List = (IList) rec2Val;
                        Assert.AreEqual(rec1List.Count, rec2List.Count);
                        for (int j = 0; j < rec1List.Count; j++)
                        {
                            AssertReflectRecordEqual(schema1List.ItemSchema, rec1List[j], schema1List.ItemSchema, rec2List[j], cache);
                        }
                    }
                    else
                    {
                        Assert.AreEqual(rec1Val, rec2Val);
                    }
                }
                else if (rec1Val is IDictionary)
                {
                    var schema1Map = f.Schema as MapSchema;
                    var rec1Dict = (IDictionary) rec1Val;
                    var rec2Dict = (IDictionary) rec2Val;
                    Assert.AreEqual(rec2Dict.Count, rec2Dict.Count);
                    foreach (var key in rec1Dict.Keys)
                    {
                        var val1 = rec1Dict[key];
                        var val2 = rec2Dict[key];
                        if (f.Schema is RecordSchema)
                        {
                            AssertReflectRecordEqual(f.Schema as RecordSchema, val1, f.Schema as RecordSchema, val2, cache);
                        }
                        else
                        {
                            Assert.AreEqual(val1, val2);
                        }
                    }
                }
                else
                {
                    Assert.AreEqual(rec1Val, rec2Val);
                }
            }
        }
    }
}
