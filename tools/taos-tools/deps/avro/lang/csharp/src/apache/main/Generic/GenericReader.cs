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
using Avro.IO;
using System.IO;

namespace Avro.Generic
{
    /// <summary>
    /// A function that can read the Avro type from the stream.
    /// </summary>
    /// <typeparam name="T">Type to read.</typeparam>
    /// <returns>The read object.</returns>
    public delegate T Reader<T>();

    /// <summary>
    /// A general purpose reader of data from avro streams. This can optionally resolve if the reader's and writer's
    /// schemas are different. This class is a wrapper around DefaultReader and offers a little more type safety. The default reader
    /// has the flexibility to return any type of object for each read call because the Read() method is generic. This
    /// class on the other hand can only return a single type because the type is a parameter to the class. Any
    /// user defined extension should, however, be done to DefaultReader. This class is sealed.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class GenericReader<T> : DatumReader<T>
    {
        private readonly DefaultReader reader;

        /// <summary>
        /// Constructs a generic reader for the given schemas using the DefaultReader. If the
        /// reader's and writer's schemas are different this class performs the resolution.
        /// </summary>
        /// <param name="writerSchema">The schema used while generating the data</param>
        /// <param name="readerSchema">The schema desired by the reader</param>
        public GenericReader(Schema writerSchema, Schema readerSchema)
            : this(new DefaultReader(writerSchema, readerSchema))
        {
        }

        /// <summary>
        /// Constructs a generic reader by directly using the given DefaultReader
        /// </summary>
        /// <param name="reader">The actual reader to use</param>
        public GenericReader(DefaultReader reader)
        {
            this.reader = reader;
        }

        /// <summary>
        /// Schema used to write the data.
        /// </summary>
        public Schema WriterSchema { get { return reader.WriterSchema; } }

        /// <summary>
        /// Schema used to read the data.
        /// </summary>
        public Schema ReaderSchema { get { return reader.ReaderSchema; } }

        /// <summary>
        /// Reads an object off the stream.
        /// </summary>
        /// <param name="reuse">
        /// If not null, the implemenation will try to use to return the object
        /// </param>
        /// <param name="d">Decoder to read from.</param>
        /// <returns>Object we read from the decoder.</returns>
        public T Read(T reuse, Decoder d)
        {
            return reader.Read(reuse, d);
        }
    }

    /// <summary>
    /// The default implementation for the generic reader. It constructs new .NET objects for avro objects on the
    /// stream and returns the .NET object. Users can directly use this class or, if they want to customize the
    /// object types for differnt Avro schema types, can derive from this class. There are enough hooks in this
    /// class to allow customization.
    /// </summary>
    /// <remarks>
    /// <list type="table">
    /// <listheader><term>Avro Type</term><description>.NET Type</description></listheader>
    /// <item><term>null</term><description>null reference</description></item>
    /// </list>
    /// </remarks>
    public class DefaultReader
    {
        /// <summary>
        /// Schema to use when reading data with this reader.
        /// </summary>
        public Schema ReaderSchema { get; private set; }

        /// <summary>
        /// Schema used to write data that we are reading with this reader.
        /// </summary>
        public Schema WriterSchema { get; private set; }


        /// <summary>
        /// Constructs the default reader for the given schemas using the DefaultReader. If the
        /// reader's and writer's schemas are different this class performs the resolution.
        /// This default implemenation maps Avro types to .NET types as follows:
        /// </summary>
        /// <param name="writerSchema">The schema used while generating the data</param>
        /// <param name="readerSchema">The schema desired by the reader</param>
        public DefaultReader(Schema writerSchema, Schema readerSchema)
        {
            this.ReaderSchema = readerSchema;
            this.WriterSchema = writerSchema;
        }

        /// <summary>
        /// Reads an object off the stream.
        /// </summary>
        /// <typeparam name="T">The type of object to read. A single schema typically returns an object of a single .NET class.
        /// The only exception is UnionSchema, which can return a object of different types based on the branch selected.
        /// </typeparam>
        /// <param name="reuse">If not null, the implemenation will try to use to return the object</param>
        /// <param name="decoder">The decoder for deserialization</param>
        /// <returns>Object read from the decoder.</returns>
        public T Read<T>(T reuse, Decoder decoder)
        {
            if (!ReaderSchema.CanRead(WriterSchema))
                throw new AvroException("Schema mismatch. Reader: " + ReaderSchema + ", writer: " + WriterSchema);

            return (T)Read(reuse, WriterSchema, ReaderSchema, decoder);
        }

        /// <summary>
        /// Reads an object off the stream.
        /// </summary>
        /// <param name="reuse">
        /// If not null, the implemenation will try to use to return the object.
        /// </param>
        /// <param name="writerSchema">Schema used to write the data.</param>
        /// <param name="readerSchema">Schema to use when reading the data.</param>
        /// <param name="d">Decoder to read from.</param>
        /// <returns>Object read from the decoder.</returns>
        public object Read(object reuse, Schema writerSchema, Schema readerSchema, Decoder d)
        {
            if (readerSchema.Tag == Schema.Type.Union && writerSchema.Tag != Schema.Type.Union)
            {
                readerSchema = findBranch(readerSchema as UnionSchema, writerSchema);
            }
            /*
            if (!readerSchema.CanRead(writerSchema))
            {
                throw new AvroException("Schema mismatch. Reader: " + readerSchema + ", writer: " + writerSchema);
            }
            */
            switch (writerSchema.Tag)
            {
                case Schema.Type.Null:
                    return ReadNull(readerSchema, d);
                case Schema.Type.Boolean:
                    return Read<bool>(writerSchema.Tag, readerSchema, d.ReadBoolean);
                case Schema.Type.Int:
                    {
                        int i = Read<int>(writerSchema.Tag, readerSchema, d.ReadInt);
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Long:
                                return (long)i;
                            case Schema.Type.Float:
                                return (float)i;
                            case Schema.Type.Double:
                                return (double)i;
                            default:
                                return i;
                        }
                    }
                case Schema.Type.Long:
                    {
                        long l = Read<long>(writerSchema.Tag, readerSchema, d.ReadLong);
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Float:
                                return (float)l;
                            case Schema.Type.Double:
                                return (double)l;
                            default:
                                return l;
                        }
                    }
                case Schema.Type.Float:
                    {
                        float f = Read<float>(writerSchema.Tag, readerSchema, d.ReadFloat);
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Double:
                                return (double)f;
                            default:
                                return f;
                        }
                    }
                case Schema.Type.Double:
                    return Read<double>(writerSchema.Tag, readerSchema, d.ReadDouble);
                case Schema.Type.String:
                    return Read<string>(writerSchema.Tag, readerSchema, d.ReadString);
                case Schema.Type.Bytes:
                    return Read<byte[]>(writerSchema.Tag, readerSchema, d.ReadBytes);
                case Schema.Type.Error:
                case Schema.Type.Record:
                    return ReadRecord(reuse, (RecordSchema)writerSchema, readerSchema, d);
                case Schema.Type.Enumeration:
                    return ReadEnum(reuse, (EnumSchema)writerSchema, readerSchema, d);
                case Schema.Type.Fixed:
                    return ReadFixed(reuse, (FixedSchema)writerSchema, readerSchema, d);
                case Schema.Type.Array:
                    return ReadArray(reuse, (ArraySchema)writerSchema, readerSchema, d);
                case Schema.Type.Map:
                    return ReadMap(reuse, (MapSchema)writerSchema, readerSchema, d);
                case Schema.Type.Union:
                    return ReadUnion(reuse, (UnionSchema)writerSchema, readerSchema, d);
                case Schema.Type.Logical:
                    return ReadLogical(reuse, (LogicalSchema)writerSchema, readerSchema, d);
                default:
                    throw new AvroException("Unknown schema type: " + writerSchema);
            }
        }

        /// <summary>
        /// Deserializes a null from the stream.
        /// </summary>
        /// <param name="readerSchema">Reader's schema, which should be a NullSchema</param>
        /// <param name="d">The decoder for deserialization</param>
        /// <returns></returns>
        protected virtual object ReadNull(Schema readerSchema, Decoder d)
        {
            d.ReadNull();
            return null;
        }

        /// <summary>
        /// A generic function to read primitive types
        /// </summary>
        /// <typeparam name="T">The .NET type to read</typeparam>
        /// <param name="tag">The Avro type tag for the object on the stream</param>
        /// <param name="readerSchema">A schema compatible to the Avro type</param>
        /// <param name="reader">A function that can read the avro type from the stream</param>
        /// <returns>The primitive type just read</returns>
        protected T Read<T>(Schema.Type tag, Schema readerSchema, Reader<T> reader)
        {
            return reader();
        }

        /// <summary>
        /// Deserializes a record from the stream.
        /// </summary>
        /// <param name="reuse">If not null, a record object that could be reused for returning the result</param>
        /// <param name="writerSchema">The writer's RecordSchema</param>
        /// <param name="readerSchema">The reader's schema, must be RecordSchema too.</param>
        /// <param name="dec">The decoder for deserialization</param>
        /// <returns>The record object just read</returns>
        protected virtual object ReadRecord(object reuse, RecordSchema writerSchema, Schema readerSchema, Decoder dec)
        {
            RecordSchema rs = (RecordSchema)readerSchema;

            object rec = CreateRecord(reuse, rs);
            foreach (Field wf in writerSchema)
            {
                try
                {
                    Field rf;
                    if (rs.TryGetFieldAlias(wf.Name, out rf))
                    {
                        object obj = null;
                        TryGetField(rec, wf.Name, rf.Pos, out obj);
                        AddField(rec, wf.Name, rf.Pos, Read(obj, wf.Schema, rf.Schema, dec));
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

                object obj = null;
                TryGetField(rec, rf.Name, rf.Pos, out obj);
                AddField(rec, rf.Name, rf.Pos, Read(obj, rf.Schema, rf.Schema, defaultDecoder));
            }

            return rec;
        }

        /// <summary>
        /// Creates a new record object. Derived classes can override this to return an object of their choice.
        /// </summary>
        /// <param name="reuse">If appropriate, will reuse this object instead of constructing a new one</param>
        /// <param name="readerSchema">The schema the reader is using</param>
        /// <returns></returns>
        protected virtual object CreateRecord(object reuse, RecordSchema readerSchema)
        {
            GenericRecord ru = (reuse == null || !(reuse is GenericRecord) || !(reuse as GenericRecord).Schema.Equals(readerSchema)) ?
                new GenericRecord(readerSchema) :
                reuse as GenericRecord;
            return ru;
        }

        /// <summary>
        /// Used by the default implementation of ReadRecord() to get the existing field of a record object. The derived
        /// classes can override this to make their own interpretation of the record object.
        /// </summary>
        /// <param name="record">The record object to be probed into. This is guaranteed to be one that was returned
        /// by a previous call to CreateRecord.</param>
        /// <param name="fieldName">The name of the field to probe.</param>
        /// <param name="fieldPos">Position of the field in the schema - not used in the base implementation.</param>
        /// <param name="value">The value of the field, if found. Null otherwise.</param>
        /// <returns>True if and only if a field with the given name is found.</returns>
        protected virtual bool TryGetField(object record, string fieldName, int fieldPos, out object value)
        {
            return (record as GenericRecord).TryGetValue(fieldPos, out value);
        }

        /// <summary>
        /// Used by the default implementation of ReadRecord() to add a field to a record object. The derived
        /// classes can override this to suit their own implementation of the record object.
        /// </summary>
        /// <param name="record">The record object to be probed into. This is guaranteed to be one that was returned
        /// by a previous call to CreateRecord.</param>
        /// <param name="fieldName">The name of the field to probe.</param>
        /// <param name="fieldPos">Position of the field in the schema - not used in the base implementation.</param>
        /// <param name="fieldValue">The value to be added for the field</param>
        protected virtual void AddField(object record, string fieldName, int fieldPos, object fieldValue)
        {
            (record as GenericRecord).Add(fieldPos, fieldValue);
        }

        /// <summary>
        /// Deserializes a enum. Uses CreateEnum to construct the new enum object.
        /// </summary>
        /// <param name="reuse">If appropirate, uses this instead of creating a new enum object.</param>
        /// <param name="writerSchema">The schema the writer used while writing the enum</param>
        /// <param name="readerSchema">The schema the reader is using</param>
        /// <param name="d">The decoder for deserialization.</param>
        /// <returns>An enum object.</returns>
        protected virtual object ReadEnum(object reuse, EnumSchema writerSchema, Schema readerSchema, Decoder d)
        {
            return CreateEnum(reuse, readerSchema as EnumSchema, writerSchema[d.ReadEnum()]);
        }

        /// <summary>
        /// Used by the default implementation of ReadEnum to construct a new enum object.
        /// </summary>
        /// <param name="reuse">If appropriate, use this enum object instead of a new one.</param>
        /// <param name="es">The enum schema used by the reader.</param>
        /// <param name="symbol">The symbol that needs to be used.</param>
        /// <returns>The default implemenation returns a GenericEnum.</returns>
        protected virtual object CreateEnum(object reuse, EnumSchema es, string symbol)
        {
            if (reuse is GenericEnum)
            {
                GenericEnum ge = reuse as GenericEnum;
                if (ge.Schema.Equals(es))
                {
                    ge.Value = symbol;
                    return ge;
                }
            }
            return new GenericEnum(es, symbol);
        }

        /// <summary>
        /// Deserializes an array and returns an array object. It uses CreateArray() and works on it before returning it.
        /// It also uses GetArraySize(), ResizeArray(), SetArrayElement() and GetArrayElement() methods. Derived classes can
        /// override these methods to customize their behavior.
        /// </summary>
        /// <param name="reuse">If appropriate, uses this instead of creating a new array object.</param>
        /// <param name="writerSchema">The schema used by the writer.</param>
        /// <param name="readerSchema">The schema that the reader uses.</param>
        /// <param name="d">The decoder for deserialization.</param>
        /// <returns>The deserialized array object.</returns>
        protected virtual object ReadArray(object reuse, ArraySchema writerSchema, Schema readerSchema, Decoder d)
        {

            ArraySchema rs = (ArraySchema)readerSchema;
            object result = CreateArray(reuse, rs);
            int i = 0;
            for (int n = (int)d.ReadArrayStart(); n != 0; n = (int)d.ReadArrayNext())
            {
                if (GetArraySize(result) < (i + n)) ResizeArray(ref result, i + n);
                for (int j = 0; j < n; j++, i++)
                {
                    SetArrayElement(result, i, Read(GetArrayElement(result, i), writerSchema.ItemSchema, rs.ItemSchema, d));
                }
            }
            if (GetArraySize(result) != i) ResizeArray(ref result, i);
            return result;
        }

        /// <summary>
        /// Creates a new array object. The initial size of the object could be anything. The users
        /// should use GetArraySize() to determine the size. The default implementation creates an <c>object[]</c>.
        /// </summary>
        /// <param name="reuse">If appropriate use this instead of creating a new one.</param>
        /// <param name="rs">Array schema, not used in base implementation</param>
        /// <returns>An object suitable to deserialize an avro array</returns>
        protected virtual object CreateArray(object reuse, ArraySchema rs)
        {
            return (reuse != null && reuse is object[]) ? (object[])reuse : new object[0];
        }

        /// <summary>
        /// Returns the size of the given array object.
        /// </summary>
        /// <param name="array">Array object whose size is required. This is guaranteed to be somthing returned by
        /// a previous call to CreateArray().</param>
        /// <returns>The size of the array</returns>
        protected virtual int GetArraySize(object array)
        {
            return (array as object[]).Length;
        }

        /// <summary>
        /// Resizes the array to the new value.
        /// </summary>
        /// <param name="array">Array object whose size is required. This is guaranteed to be somthing returned by
        /// a previous call to CreateArray().</param>
        /// <param name="n">The new size.</param>
        protected virtual void ResizeArray(ref object array, int n)
        {
            object[] o = array as object[];
            Array.Resize(ref o, n);
            array = o;
        }

        /// <summary>
        /// Assigns a new value to the object at the given index
        /// </summary>
        /// <param name="array">Array object whose size is required. This is guaranteed to be somthing returned by
        /// a previous call to CreateArray().</param>
        /// <param name="index">The index to reassign to.</param>
        /// <param name="value">The value to assign.</param>
        protected virtual void SetArrayElement(object array, int index, object value)
        {
            object[] a = array as object[];
            a[index] = value;
        }

        /// <summary>
        /// Returns the element at the given index.
        /// </summary>
        /// <param name="array">Array object whose size is required. This is guaranteed to be somthing returned by
        /// a previous call to CreateArray().</param>
        /// <param name="index">The index to look into.</param>
        /// <returns>The object the given index. Null if no object has been assigned to that index.</returns>
        protected virtual object GetArrayElement(object array, int index)
        {
            return (array as object[])[index];
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
        protected virtual object ReadMap(object reuse, MapSchema writerSchema, Schema readerSchema, Decoder d)
        {
            MapSchema rs = (MapSchema)readerSchema;
            object result = CreateMap(reuse, rs);
            for (int n = (int)d.ReadMapStart(); n != 0; n = (int)d.ReadMapNext())
            {
                for (int j = 0; j < n; j++)
                {
                    string k = d.ReadString();
                    AddMapEntry(result, k, Read(null, writerSchema.ValueSchema, rs.ValueSchema, d));
                }
            }
            return result;
        }

        /// <summary>
        /// Used by the default implementation of ReadMap() to create a fresh map object. The default
        /// implementaion of this method returns a IDictionary&lt;string, map&gt;.
        /// </summary>
        /// <param name="reuse">If appropriate, use this map object instead of creating a new one.</param>
        /// <param name="ms">Map schema to use when creating the object.</param>
        /// <returns>An empty map object.</returns>
        protected virtual object CreateMap(object reuse, MapSchema ms)
        {
            if (reuse != null && reuse is IDictionary<string, object>)
            {
                IDictionary<string, object> result = reuse as IDictionary<string, object>;
                result.Clear();
                return result;
            }
            return new Dictionary<string, object>();
        }

        /// <summary>
        /// Adds an entry to the map.
        /// </summary>
        /// <param name="map">A map object, which is guaranteed to be one returned by a previous call to CreateMap().</param>
        /// <param name="key">The key to add.</param>
        /// <param name="value">The value to add.</param>
        protected virtual void AddMapEntry(object map, string key, object value)
        {
            (map as IDictionary<string, object>).Add(key, value);
        }

        /// <summary>
        /// Deserialized an object based on the writer's uninon schema.
        /// </summary>
        /// <param name="reuse">If appropriate, uses this object instead of creating a new one.</param>
        /// <param name="writerSchema">The UnionSchema that the writer used.</param>
        /// <param name="readerSchema">The schema the reader uses.</param>
        /// <param name="d">The decoder for serialization.</param>
        /// <returns>The deserialized object.</returns>
        protected virtual object ReadUnion(object reuse, UnionSchema writerSchema, Schema readerSchema, Decoder d)
        {
            int index = d.ReadUnionIndex();
            Schema ws = writerSchema[index];

            if (readerSchema is UnionSchema)
                readerSchema = findBranch(readerSchema as UnionSchema, ws);
            else
                if (!readerSchema.CanRead(ws))
                    throw new AvroException("Schema mismatch. Reader: " + ReaderSchema + ", writer: " + WriterSchema);

            return Read(reuse, ws, readerSchema, d);
        }

        /// <summary>
        /// Deserializes an object based on the writer's logical schema. Uses the underlying logical type to convert
        /// the value to the logical type.
        /// </summary>
        /// <param name="reuse">If appropriate, uses this object instead of creating a new one.</param>
        /// <param name="writerSchema">The UnionSchema that the writer used.</param>
        /// <param name="readerSchema">The schema the reader uses.</param>
        /// <param name="d">The decoder for serialization.</param>
        /// <returns>The deserialized object.</returns>
        protected virtual object ReadLogical(object reuse, LogicalSchema writerSchema, Schema readerSchema, Decoder d)
        {
            LogicalSchema ls = (LogicalSchema)readerSchema;

            return writerSchema.LogicalType.ConvertToLogicalValue(Read(reuse, writerSchema.BaseSchema, ls.BaseSchema, d), ls);
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
        protected virtual object ReadFixed(object reuse, FixedSchema writerSchema, Schema readerSchema, Decoder d)
        {
            FixedSchema rs = (FixedSchema)readerSchema;
            if (rs.Size != writerSchema.Size)
            {
                throw new AvroException("Size mismatch between reader and writer fixed schemas. Writer: " + writerSchema +
                    ", reader: " + readerSchema);
            }

            object ru = CreateFixed(reuse, rs);
            byte[] bb = GetFixedBuffer(ru);
            d.ReadFixed(bb);
            return ru;
        }

        /// <summary>
        /// Returns a fixed object.
        /// </summary>
        /// <param name="reuse">If appropriate, uses this object instead of creating a new one.</param>
        /// <param name="rs">The reader's FixedSchema.</param>
        /// <returns>A fixed object with an appropriate buffer.</returns>
        protected virtual object CreateFixed(object reuse, FixedSchema rs)
        {
            return (reuse != null && reuse is GenericFixed && (reuse as GenericFixed).Schema.Equals(rs)) ?
                (GenericFixed)reuse : new GenericFixed(rs);
        }

        /// <summary>
        /// Returns a buffer of appropriate size to read data into.
        /// </summary>
        /// <param name="f">The fixed object. It is guaranteed that this is something that has been previously
        /// returned by CreateFixed</param>
        /// <returns>A byte buffer of fixed's size.</returns>
        protected virtual byte[] GetFixedBuffer(object f)
        {
            return (f as GenericFixed).Value;
        }

        /// <summary>
        /// Skip an instance of a schema.
        /// </summary>
        /// <param name="writerSchema">Schema to skip.</param>
        /// <param name="d">Decoder we're reading from.</param>
        protected virtual void Skip(Schema writerSchema, Decoder d)
        {
            switch (writerSchema.Tag)
            {
                case Schema.Type.Null:
                    d.SkipNull();
                    break;
                case Schema.Type.Boolean:
                    d.SkipBoolean();
                    break;
                case Schema.Type.Int:
                    d.SkipInt();
                    break;
                case Schema.Type.Long:
                    d.SkipLong();
                    break;
                case Schema.Type.Float:
                    d.SkipFloat();
                    break;
                case Schema.Type.Double:
                    d.SkipDouble();
                    break;
                case Schema.Type.String:
                    d.SkipString();
                    break;
                case Schema.Type.Bytes:
                    d.SkipBytes();
                    break;
                case Schema.Type.Record:
                    foreach (Field f in writerSchema as RecordSchema) Skip(f.Schema, d);
                    break;
                case Schema.Type.Enumeration:
                    d.SkipEnum();
                    break;
                case Schema.Type.Fixed:
                    d.SkipFixed((writerSchema as FixedSchema).Size);
                    break;
                case Schema.Type.Array:
                    {
                        Schema s = (writerSchema as ArraySchema).ItemSchema;
                        for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                        {
                            for (long i = 0; i < n; i++) Skip(s, d);
                        }
                    }
                    break;
                case Schema.Type.Map:
                    {
                        Schema s = (writerSchema as MapSchema).ValueSchema;
                        for (long n = d.ReadMapStart(); n != 0; n = d.ReadMapNext())
                        {
                            for (long i = 0; i < n; i++) { d.SkipString(); Skip(s, d); }
                        }
                    }
                    break;
                case Schema.Type.Union:
                    Skip((writerSchema as UnionSchema)[d.ReadUnionIndex()], d);
                    break;
                case Schema.Type.Logical:
                    Skip((writerSchema as LogicalSchema).BaseSchema, d);
                    break;
                default:
                    throw new AvroException("Unknown schema type: " + writerSchema);
            }
        }

        /// <summary>
        /// Finds the branch of the union schema associated with the given schema.
        /// </summary>
        /// <param name="us">Union schema.</param>
        /// <param name="s">Schema to find in the union schema.</param>
        /// <returns>Schema branch in the union schema.</returns>
        protected static Schema findBranch(UnionSchema us, Schema s)
        {
            int index = us.MatchingBranch(s);
            if (index >= 0) return us[index];
            throw new AvroException("No matching schema for " + s + " in " + us);
        }

    }
}
