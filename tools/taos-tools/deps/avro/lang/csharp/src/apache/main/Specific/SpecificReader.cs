/*
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
using System.Reflection;
using System.IO;
using Avro;
using Avro.IO;
using Avro.Generic;

namespace Avro.Specific
{
    /// <summary>
    /// Reader wrapper class for reading data and storing into specific classes
    /// </summary>
    /// <typeparam name="T">Specific class type</typeparam>
    public class SpecificReader<T> : DatumReader<T>
    {
        /// <summary>
        /// Reader class for reading data and storing into specific classes
        /// </summary>
        private readonly SpecificDefaultReader reader;

        /// <summary>
        /// Schema for the writer class
        /// </summary>
        public Schema WriterSchema { get { return reader.WriterSchema; } }

        /// <summary>
        /// Schema for the reader class
        /// </summary>
        public Schema ReaderSchema { get { return reader.ReaderSchema; } }

        /// <summary>
        /// Constructs a generic reader for the given schemas using the DefaultReader. If the
        /// reader's and writer's schemas are different this class performs the resolution.
        /// </summary>
        /// <param name="writerSchema">The schema used while generating the data</param>
        /// <param name="readerSchema">The schema desired by the reader</param>
        public SpecificReader(Schema writerSchema, Schema readerSchema)
        {
            reader = new SpecificDefaultReader(writerSchema, readerSchema);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpecificReader{T}"/> class using an
        /// existing <see cref="SpecificDefaultReader"/>.
        /// </summary>
        /// <param name="reader">Default reader to use.</param>
        public SpecificReader(SpecificDefaultReader reader)
        {
            this.reader = reader;
        }

        /// <summary>
        /// Generic read function
        /// </summary>
        /// <param name="reuse">object to store data read</param>
        /// <param name="dec">decorder to use for reading data</param>
        /// <returns></returns>
        public T Read(T reuse, Decoder dec)
        {
            return reader.Read(reuse, dec);
        }
    }

    /// <summary>
    /// Reader class for reading data and storing into specific classes
    /// </summary>
    public class SpecificDefaultReader : DefaultReader
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="writerSchema">schema of the object that wrote the data</param>
        /// <param name="readerSchema">schema of the object that will store the data</param>
        public SpecificDefaultReader(Schema writerSchema, Schema readerSchema) : base(writerSchema,readerSchema)
        {
        }

        /// <summary>
        /// Deserializes a record from the stream.
        /// </summary>
        /// <param name="reuse">If not null, a record object that could be reused for returning the result</param>
        /// <param name="writerSchema">The writer's RecordSchema</param>
        /// <param name="readerSchema">The reader's schema, must be RecordSchema too.</param>
        /// <param name="dec">The decoder for deserialization</param>
        /// <returns>The record object just read</returns>
        protected override object ReadRecord(object reuse, RecordSchema writerSchema, Schema readerSchema, Decoder dec)
        {
            RecordSchema rs = (RecordSchema)readerSchema;

            if (rs.Name == null)
                return base.ReadRecord(reuse, writerSchema, readerSchema, dec);

            ISpecificRecord rec = (reuse != null ? reuse : ObjectCreator.Instance.New(rs.Fullname, Schema.Type.Record)) as ISpecificRecord;
            object obj;
            foreach (Field wf in writerSchema)
            {
                try
                {
                    Field rf;
                    if (rs.TryGetField(wf.Name, out rf))
                    {
                        obj = rec.Get(rf.Pos);
                        rec.Put(rf.Pos, Read(obj, wf.Schema, rf.Schema, dec));
                    }
                    else
                        Skip(wf.Schema, dec);
                }
                catch (Exception ex)
                {
                    throw new AvroException(ex.Message + " in field " + wf.Name, ex);
                }
            }

            var defaultStream = new MemoryStream();
            var defaultEncoder = new BinaryEncoder(defaultStream);
            var defaultDecoder = new BinaryDecoder(defaultStream);
            foreach (Field rf in rs)
            {
                if (writerSchema.Contains(rf.Name)) continue;

                defaultStream.Position = 0; // reset for writing
                Resolver.EncodeDefaultValue(defaultEncoder, rf.Schema, rf.DefaultValue);
                defaultStream.Flush();
                defaultStream.Position = 0; // reset for reading

                obj = rec.Get(rf.Pos);
                rec.Put(rf.Pos, Read(obj, rf.Schema, rf.Schema, defaultDecoder));
            }

            return rec;
        }

        /// <summary>
        /// Deserializes a fixed object and returns the object. The default implementation uses CreateFixed()
        /// and GetFixedBuffer() and returns what CreateFixed() returned.
        /// </summary>
        /// <param name="reuse">If appropriate, uses this object instead of creating a new one.</param>
        /// <param name="writerSchema">The FixedSchema the writer used during serialization.</param>
        /// <param name="readerSchema">The schema that the readr uses. Must be a FixedSchema with the same
        /// size as the writerSchema.</param>
        /// <param name="d">The decoder for deserialization.</param>
        /// <returns>The deserilized object.</returns>
        protected override object ReadFixed(object reuse, FixedSchema writerSchema, Schema readerSchema, Decoder d)
        {
            FixedSchema rs = readerSchema as FixedSchema;
            if (rs.Size != writerSchema.Size)
            {
                throw new AvroException("Size mismatch between reader and writer fixed schemas. Writer: " + writerSchema +
                    ", reader: " + readerSchema);
            }

            SpecificFixed fixedrec = (reuse != null ? reuse : ObjectCreator.Instance.New(rs.Fullname, Schema.Type.Fixed)) as SpecificFixed;
            d.ReadFixed(fixedrec.Value);
            return fixedrec;
        }

        /// <summary>
        /// Reads an enum from the given decoder
        /// </summary>
        /// <param name="reuse">object to store data read</param>
        /// <param name="writerSchema">schema of the object that wrote the data</param>
        /// <param name="readerSchema">schema of the object that will store the data</param>
        /// <param name="dec">decoder object that contains the data to be read</param>
        /// <returns>enum value</returns>
        protected override object ReadEnum(object reuse, EnumSchema writerSchema, Schema readerSchema, Decoder dec)
        {
            EnumSchema rs = readerSchema as EnumSchema;
            return rs.Ordinal(writerSchema[dec.ReadEnum()]);
        }

        /// <summary>
        /// Reads an array from the given decoder
        /// </summary>
        /// <param name="reuse">object to store data read</param>
        /// <param name="writerSchema">schema of the object that wrote the data</param>
        /// <param name="readerSchema">schema of the object that will store the data</param>
        /// <param name="dec">decoder object that contains the data to be read</param>
        /// <returns>array</returns>
        protected override object ReadArray(object reuse, ArraySchema writerSchema, Schema readerSchema, Decoder dec)
        {
            ArraySchema rs = readerSchema as ArraySchema;
            System.Collections.IList array;
            if (reuse != null)
            {
                array = reuse as System.Collections.IList;
                if (array == null)
                    throw new AvroException("array object does not implement non-generic IList");

                array.Clear();
            }
            else
                array = ObjectCreator.Instance.New(getTargetType(readerSchema), Schema.Type.Array) as System.Collections.IList;

            int i = 0;
            for (int n = (int)dec.ReadArrayStart(); n != 0; n = (int)dec.ReadArrayNext())
            {
                for (int j = 0; j < n; j++, i++)
                    array.Add(Read(null, writerSchema.ItemSchema, rs.ItemSchema, dec));
            }
            return array;
        }

        /// <summary>
        /// Deserialized an avro map. The default implemenation creats a new map using CreateMap() and then
        /// adds elements to the map using AddMapEntry().
        /// </summary>
        /// <param name="reuse">If appropriate, use this instead of creating a new map object.</param>
        /// <param name="writerSchema">The schema the writer used to write the map.</param>
        /// <param name="readerSchema">The schema the reader is using.</param>
        /// <param name="d">The decoder for serialization.</param>
        /// <returns>The deserialized map object.</returns>
        protected override object ReadMap(object reuse, MapSchema writerSchema, Schema readerSchema, Decoder d)
        {
            MapSchema rs = readerSchema as MapSchema;
            System.Collections.IDictionary map;
            if (reuse != null)
            {
                map = reuse as System.Collections.IDictionary;
                if (map == null)
                    throw new AvroException("map object does not implement non-generic IList");

                map.Clear();
            }
            else
                map = ObjectCreator.Instance.New(getTargetType(readerSchema), Schema.Type.Map) as System.Collections.IDictionary;

            for (int n = (int)d.ReadMapStart(); n != 0; n = (int)d.ReadMapNext())
            {
                for (int j = 0; j < n; j++)
                {
                    string k = d.ReadString();
                    map[k] = Read(null, writerSchema.ValueSchema, rs.ValueSchema, d);   // always create new map item
                }
            }
            return map;
        }

        /// <summary>
        /// Gets the target type name in the given schema
        /// </summary>
        /// <param name="schema">schema containing the type to be determined</param>
        /// <returns>Name of the type</returns>
        protected virtual string getTargetType(Schema schema)
        {
            bool nEnum = false;
            string type = Avro.CodeGen.getType(schema, false, ref nEnum);
            if (schema.Tag == Schema.Type.Array)
            {
                type = type.Remove(0, 6);              // remove IList<
                type = type.Remove(type.Length - 1);   // remove >
            }
            else if (schema.Tag == Schema.Type.Map)
            {
                type = type.Remove(0, 19);             // remove IDictionary<string,
                type = type.Remove(type.Length - 1);   // remove >
            }
            return type;
        }
    }
}
