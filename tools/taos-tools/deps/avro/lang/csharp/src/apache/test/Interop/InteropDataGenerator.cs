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
using System.Collections.Generic;
using System.IO;
using System.Text;
using Avro.File;
using Avro.Generic;

namespace Avro.Test.Interop
{
    public class InteropDataGenerator
    {
        static void GenerateInteropData(string schemaPath, string outputDir)
        {
            RecordSchema schema = null;
            using (var reader = new StreamReader(schemaPath))
            {
                schema = Schema.Parse(reader.ReadToEnd()) as RecordSchema;
            }

            var mapFieldSchema = (schema.Fields.Find(x => x.Name == "mapField").Schema as MapSchema).ValueSchema as RecordSchema;
            var mapFieldRecord0 = new GenericRecord(mapFieldSchema);
            var mapFieldRecord1 = new GenericRecord(mapFieldSchema);
            mapFieldRecord0.Add("label", "a");
            mapFieldRecord1.Add("label", "cee");
            var mapFieldValue = new Dictionary<string, GenericRecord>
            {
                { "a", mapFieldRecord0 },
                { "bee", mapFieldRecord1 }
            };

            var enumFieldValue = new GenericEnum(schema.Fields.Find(x => x.Name == "enumField").Schema as EnumSchema, "C");

            var fixedFieldValue = new GenericFixed(
                schema.Fields.Find(x => x.Name == "fixedField").Schema as FixedSchema,
                Encoding.ASCII.GetBytes("1019181716151413"));

            var nodeSchema = schema.Fields.Find(x => x.Name == "recordField").Schema as RecordSchema;
            var recordFieldValue = new GenericRecord(nodeSchema);
            var innerRecordFieldValue = new GenericRecord(nodeSchema);
            innerRecordFieldValue.Add("label", "inner");
            innerRecordFieldValue.Add("children", new GenericRecord[] { });
            recordFieldValue.Add("label", "blah");
            recordFieldValue.Add("children", new GenericRecord[] { innerRecordFieldValue });

            GenericRecord record = new GenericRecord(schema);
            record.Add("intField", 12);
            record.Add("longField", 15234324L);
            record.Add("stringField", "hey");
            record.Add("boolField", true);
            record.Add("floatField", 1234.0f);
            record.Add("doubleField", -1234.0);
            record.Add("bytesField", Encoding.UTF8.GetBytes("12312adf"));
            record.Add("nullField", null);
            record.Add("arrayField", new double[] { 5.0, 0.0, 12.0 });
            record.Add("mapField", mapFieldValue);
            record.Add("unionField", 12.0);
            record.Add("enumField", enumFieldValue);
            record.Add("fixedField", fixedFieldValue);
            record.Add("recordField", recordFieldValue);

            var datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            foreach (var codecName in InteropDataConstants.SupportedCodecNames)
            {
                var outputFile = "csharp.avro";
                if (codecName != DataFileConstants.NullCodec)
                {
                    outputFile = string.Format("csharp_{0}.avro", codecName);
                }
                var outputPath = Path.Combine(outputDir, outputFile);
                var codec = Codec.CreateCodecFromString(codecName);
                using (var dataFileWriter = DataFileWriter<GenericRecord>.OpenWriter(datumWriter, outputPath, codec))
                {
                    dataFileWriter.Append(record);
                }
            }
        }

        static void Main(string[] args)
        {
            GenerateInteropData(args[0], args[1]);
        }
    }
}