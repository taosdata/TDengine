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
using System.IO;
using System.Text;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using com.foo;

namespace Avro.perf
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Out.WriteLine("type\timpl\taction\ttotal_items\tbatches\tbatch_size\ttime(ms)");
            PerfTest( "simple", BuildSimple(), Simple._SCHEMA);
            PerfTest( "complex", BuildComplex(), Complex._SCHEMA);
            PerfTest( "narrow", BuildNarrow(), Narrow._SCHEMA);
            PerfTest( "wide", BuildWide(), Wide._SCHEMA);
        }

        private static Simple BuildSimple()
        {
            var bytes = "bytes sample text";
            var encoding = new UTF8Encoding();
            var simp = new Simple
                           {
                               myInt = 1,
                               myLong = 2,
                               myBool = true,
                               myDouble = (double) 3,
                               myFloat = (float) 4.5,
                               myBytes = encoding.GetBytes( bytes ),
                               myString = "Hello",
                               myNull = null,
                           };
            return simp;
        }

        private static Complex BuildComplex()
        {
            var bytes = "bytes sample text";
            var encoding = new UTF8Encoding();

            var c = new Complex
                        {
                            myUInt = 1,
                            myULong = 2,
                            myUBool = true,
                            myUDouble = (double) 3,
                            myUFloat = (float) 4.5,
                            myUBytes = encoding.GetBytes( bytes ),
                            myUString = "Hello",
                            myInt = 1,
                            myLong = 2,
                            myBool = true,
                            myDouble = (double) 3,
                            myFloat = (float) 4.5,
                            myBytes = encoding.GetBytes( bytes ),
                            myString = "Hello",
                            myNull = null,
                            myFixed = new MyFixed() { Value = encoding.GetBytes( "My fixed record0" ) },
                            myA = new A() { f1 = 10 },
                            myE = com.foo.MyEnum.C
                        };

            c.myArray = new List<byte[]>();
            c.myArray.Add( encoding.GetBytes( "a" ) );

            c.myArray2 = new List<com.foo.newRec>();
            var rec = new com.foo.newRec();
            rec.f1 = 50;
            c.myArray2.Add( rec );

            c.myMap = new Dictionary<string, string>();
            c.myMap.Add( "key", "value" );
            c.myMap2 = new Dictionary<string, com.foo.newRec>();
            var newrec = new com.foo.newRec();
            newrec.f1 = 1200;
            c.myMap2.Add( "A", newrec );
            c.myObject = c.myA;

            var o1 = new List<System.Object>();

            o1.Add( (double) 1123123121 );
            o1.Add( (double) 2 );
            o1.Add( null );
            o1.Add( "fred" );

            var o2 = new List<System.Object>();

            o2.Add( (double) 1 );
            o2.Add( (double) 32531 );
            o2.Add( (double) 4 );
            o2.Add( (double) 555 );
            o2.Add( (double) 0 );

            c.myArray3 = new List<IList<System.Object>>();
            c.myArray3.Add( o1 );
            c.myArray3.Add( o2 );
            return c;
        }

        private static Narrow BuildNarrow()
        {
            return new Narrow
                       {
                           myInt = 5000000,
                           myLong = 99999999999,
                           myString = "narrow"
                       };
        }

        private static Wide BuildWide()
        {
            return new Wide()
                       {
                           myA = new A { f1 = 9995885 },
                           myA2 = new A { f1 = 29995885 },
                           myA3 = new A { f1 = 39995885 },
                           myA4 = new A { f1 = 49995885 },
                           myFloat = 11111.11f,
                           myFloat2 = 22222.22f,
                           myFloat3 = 33333.33f,
                           myFloat4 = 44444.44f,
                           myE = MyEnum.A,
                           myE2 = MyEnum.B,
                           myE3 = MyEnum.C,
                           myE4 = MyEnum.C,
                           myBool = true,
                           myBool2 = false,
                           myBool3 = true,
                           myBool4 = false,
                           myDouble = 11111111111.11,
                           myDouble2 = 22222222222.22,
                           myDouble3 = 33333333333.33,
                           myDouble4 = 44444444444.44,
                           myInt = 1111111,
                           myInt2 = 2222222,
                           myInt3 = 3333333,
                           myInt4 = 4444444,
                           myLong = 1111111111111,
                           myLong2 = 2222222222222,
                           myLong3 = 3333333333333,
                           myLong4 = 4444444444444,
                           myString = "wide record 1",
                           myString2 = "wide record 2",
                           myString3 = "wide record 3",
                           myString4 = "wide record 4",
                           myBytes = new byte[] { 1, 1, 1, 1 },
                           myBytes2 = new byte[] { 2, 2, 2, 2 },
                           myBytes3 = new byte[] { 3, 3, 3, 3 },
                           myBytes4 = new byte[] { 4, 4, 4, 4 }
                       };
        }

        private static void PerfTest<T>(string testName, T testObj, Schema schema)
        {
            var generic = ConvertSpecificToGeneric(testObj, schema);
            PerfTest(testName, "default_specific", testObj, schema, s => new SpecificWriter<T>(s), s => new SpecificReader<T>(s, s));
            PerfTest(testName, "preresolved_specific", testObj, schema, s => new SpecificDatumWriter<T>(s), s => new SpecificDatumReader<T>(s, s));
            PerfTest(testName, "default_generic", generic, schema, s => new GenericWriter<GenericRecord>(s), s => new GenericReader<GenericRecord>(s, s));
            PerfTest(testName, "preresolved_generic", generic, schema, s => new GenericDatumWriter<GenericRecord>(s), s => new GenericDatumReader<GenericRecord>(s, s));
        }

        private static GenericRecord ConvertSpecificToGeneric<T>(T obj, Schema schema)
        {
            var stream = new MemoryStream();
            var encoder = new BinaryEncoder( stream );
            var decoder = new BinaryDecoder( stream );

            var writer = new SpecificWriter<T>(schema);
            writer.Write(obj, encoder);
            encoder.Flush();
            stream.Position = 0;

            return new GenericReader<GenericRecord>(schema, schema).Read(null, decoder);
        }

        private static void PerfTest<T>(string name, string impl, T z, Schema schema, Func<Schema,DatumWriter<T>> writerFactory, Func<Schema,DatumReader<T>> readerFactory)
        {
            var stream = new MemoryStream();
            var binEncoder = new BinaryEncoder( stream );
            var decoder = new BinaryDecoder( stream );

            var totalItems = 1000000;

            foreach (int itemsPerBatch in new List<int> { 1000 } )
            {
                int serialized = 0;
                int batches = totalItems / itemsPerBatch;
                var startTime = Environment.TickCount;
                for (int batch = 0; batch < batches; batch++ )
                {
                    var writer = writerFactory( schema );
                    for( int i = 0; i < itemsPerBatch; i++ )
                    {
                        stream.Position = 0;
                        writer.Write( z, binEncoder );
                        serialized++;
                    }
                }
                Console.Out.WriteLine("{0}\t{1}\tserialize\t{2}\t{3}\t{4}\t{5}", name, impl, serialized, batches, itemsPerBatch, Environment.TickCount - startTime);

                int deserialized = 0;
                startTime = Environment.TickCount;
                for (int batch = 0; batch < batches; batch++ )
                {
                    var reader = readerFactory(schema);
                    for (int i = 0; i < itemsPerBatch; i++)
                    {
                        stream.Position = 0;
                        reader.Read( z, decoder );
                        deserialized++;
                    }
                }
                Console.Out.WriteLine("{0}\t{1}\tdeserialize\t{2}\t{3}\t{4}\t{5}", name, impl, deserialized, batches, itemsPerBatch, Environment.TickCount - startTime);
            }
        }
    }
}