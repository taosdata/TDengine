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
using System.Collections.Generic;
using System.IO;
using Avro.IO;

namespace Avro.Generic
{
    /// <summary>
    /// A general purpose reader of data from avro streams. This reader analyzes and resolves the reader and writer schemas
    /// when constructed so that reads can be more efficient. Once constructed, a reader can be reused or shared among threads
    /// to avoid incurring more resolution costs.
    /// </summary>
    public abstract class PreresolvingDatumReader<T> : DatumReader<T>
    {
        /// <inheritdoc/>
        public Schema ReaderSchema { get; private set; }

        /// <inheritdoc/>
        public Schema WriterSchema { get; private set; }

        /// <summary>
        /// Defines the signature for a function that reads an item from a decoder.
        /// </summary>
        /// <param name="reuse">Optional object to deserialize the datum into. May be null.</param>
        /// <param name="dec">Decoder to read data from.</param>
        /// <returns>Deserialized datum.</returns>
        protected delegate object ReadItem(object reuse, Decoder dec);

        // read a specific field from a decoder
        private delegate object DecoderRead(Decoder dec);
        // skip specific field(s) from a decoder
        private delegate void DecoderSkip(Decoder dec);
        // read & set fields on a record
        private delegate void FieldReader(object record, Decoder decoder);

        private readonly ReadItem _reader;
        private readonly Dictionary<SchemaPair,ReadItem> _recordReaders = new Dictionary<SchemaPair,ReadItem>();

        /// <summary>
        /// Initializes a new instance of the <see cref="PreresolvingDatumReader{T}"/> class.
        /// </summary>
        /// <param name="writerSchema">Schema that was used to write the data.</param>
        /// <param name="readerSchema">Schema to use to read the data.</param>
        protected PreresolvingDatumReader(Schema writerSchema, Schema readerSchema)
        {
            ReaderSchema = readerSchema;
            WriterSchema = writerSchema;
            if (!ReaderSchema.CanRead(WriterSchema))
                throw new AvroException("Schema mismatch. Reader: " + ReaderSchema + ", writer: " + WriterSchema);
            _reader = ResolveReader(writerSchema, readerSchema);
        }

        /// <inheritdoc/>
        public T Read(T reuse, Decoder decoder)
        {
            return (T)_reader(reuse, decoder);
        }

        /// <summary>
        /// Returns an <see cref="ArrayAccess"/> implementation for the given schema.
        /// </summary>
        /// <param name="readerSchema">Schema for the array.</param>
        /// <returns>An <see cref="ArrayAccess"/> implementation.</returns>
        protected abstract ArrayAccess GetArrayAccess(ArraySchema readerSchema);

        /// <summary>
        /// Returns an <see cref="EnumAccess"/> implementation for the given schema.
        /// </summary>
        /// <param name="readerSchema">Schema for the enum.</param>
        /// <returns>An <see cref="EnumAccess"/> implementation.</returns>
        protected abstract EnumAccess GetEnumAccess(EnumSchema readerSchema);

        /// <summary>
        /// Returns a <see cref="MapAccess"/> implementation for the given schema.
        /// </summary>
        /// <param name="readerSchema">Schema for the map.</param>
        /// <returns>A <see cref="MapAccess"/> implementation.</returns>
        protected abstract MapAccess GetMapAccess(MapSchema readerSchema);

        /// <summary>
        /// Returns a <see cref="RecordAccess"/> implementation for the given schema.
        /// </summary>
        /// <param name="readerSchema">Schema for the record.</param>
        /// <returns>A <see cref="RecordAccess"/> implementation.</returns>
        protected abstract RecordAccess GetRecordAccess(RecordSchema readerSchema);

        /// <summary>
        /// Returns a <see cref="FixedAccess"/> implementation for the given schema.
        /// </summary>
        /// <param name="readerSchema">Schema for the fixed.</param>
        /// <returns>A <see cref="FixedAccess"/> implementation.</returns>
        protected abstract FixedAccess GetFixedAccess(FixedSchema readerSchema);

        /// <summary>
        /// Build a reader that accounts for schema differences between the reader and writer schemas.
        /// </summary>
        private ReadItem ResolveReader(Schema writerSchema, Schema readerSchema)
        {
            if (readerSchema.Tag == Schema.Type.Union && writerSchema.Tag != Schema.Type.Union)
            {
                readerSchema = FindBranch(readerSchema as UnionSchema, writerSchema);
            }
            switch (writerSchema.Tag)
            {
                case Schema.Type.Null:
                    return ReadNull;
                case Schema.Type.Boolean:
                    return ReadBoolean;
                case Schema.Type.Int:
                    {
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Long:
                                return Read(d => (long) d.ReadInt());
                            case Schema.Type.Float:
                                return Read(d => (float) d.ReadInt());
                            case Schema.Type.Double:
                                return Read(d => (double) d.ReadInt());
                            default:
                                return Read(d => d.ReadInt());
                        }
                    }
                case Schema.Type.Long:
                    {
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Float:
                                return Read(d => (float) d.ReadLong());
                            case Schema.Type.Double:
                                return Read(d => (double) d.ReadLong());
                            default:
                                return Read(d => d.ReadLong());
                        }
                    }
                case Schema.Type.Float:
                    {
                        switch (readerSchema.Tag)
                        {
                            case Schema.Type.Double:
                                return Read(d => (double) d.ReadFloat());
                            default:
                                return Read(d => d.ReadFloat());
                        }
                    }
                case Schema.Type.Double:
                    return Read(d => d.ReadDouble());
                case Schema.Type.String:
                    return Read(d => d.ReadString());
                case Schema.Type.Bytes:
                    return Read(d => d.ReadBytes());
                case Schema.Type.Error:
                case Schema.Type.Record:
                    return ResolveRecord((RecordSchema)writerSchema, (RecordSchema)readerSchema);
                case Schema.Type.Enumeration:
                    return ResolveEnum((EnumSchema)writerSchema, (EnumSchema)readerSchema);
                case Schema.Type.Fixed:
                    return ResolveFixed((FixedSchema)writerSchema, (FixedSchema)readerSchema);
                case Schema.Type.Array:
                    return ResolveArray((ArraySchema)writerSchema, (ArraySchema)readerSchema);
                case Schema.Type.Map:
                    return ResolveMap((MapSchema)writerSchema, (MapSchema)readerSchema);
                case Schema.Type.Union:
                    return ResolveUnion((UnionSchema)writerSchema, readerSchema);
                case Schema.Type.Logical:
                    return ResolveLogical((LogicalSchema)writerSchema, (LogicalSchema)readerSchema);
                default:
                    throw new AvroException("Unknown schema type: " + writerSchema);
            }
        }

        private ReadItem ResolveEnum(EnumSchema writerSchema, EnumSchema readerSchema)
        {
            var enumAccess = GetEnumAccess(readerSchema);

            if (readerSchema.Equals(writerSchema))
            {
                return (r, d) => enumAccess.CreateEnum(r, d.ReadEnum());
            }

            var translator = new int[writerSchema.Symbols.Count];

            var readerDefaultOrdinal = null != readerSchema.Default ? readerSchema.Ordinal(readerSchema.Default) : -1;

            foreach (var symbol in writerSchema.Symbols)
            { 
                var writerOrdinal = writerSchema.Ordinal(symbol);
                if (readerSchema.Contains(symbol))
                {
                    translator[writerOrdinal] = readerSchema.Ordinal(symbol);
                }
                else
                {
                    translator[writerOrdinal] = -1;
                }
            }

            return (r, d) =>
                        {
                            var writerOrdinal = d.ReadEnum();
                            var readerOrdinal = translator[writerOrdinal];
                            if (readerOrdinal == -1 && readerDefaultOrdinal != -1) //the symbol doesn't exist, but the default does
                            {
                                return enumAccess.CreateEnum(r, readerDefaultOrdinal);
                            }

                            if (readerOrdinal == -1)
                            {
                                throw new AvroException("No such symbol: " + writerSchema[writerOrdinal]);
                            }
                            return enumAccess.CreateEnum(r, readerOrdinal);
                        };
        }

        private ReadItem ResolveRecord(RecordSchema writerSchema, RecordSchema readerSchema)
        {
            var schemaPair = new SchemaPair(writerSchema, readerSchema);
            ReadItem recordReader;

            if (_recordReaders.TryGetValue(schemaPair, out recordReader))
            {
                return recordReader;
            }

            FieldReader[] fieldReaderArray = null;
            var recordAccess = GetRecordAccess(readerSchema);

            recordReader = (r, d) => ReadRecord(r, d, recordAccess, fieldReaderArray);
            _recordReaders.Add(schemaPair, recordReader);

            var readSteps = new List<FieldReader>();

            foreach (Field wf in writerSchema)
            {
                Field rf;
                if (readerSchema.TryGetFieldAlias(wf.Name, out rf))
                {
                    var readItem = ResolveReader(wf.Schema, rf.Schema);
                    if(IsReusable(rf.Schema.Tag))
                    {
                        readSteps.Add((rec,d) => recordAccess.AddField(rec, rf.Name, rf.Pos,
                            readItem(recordAccess.GetField(rec, rf.Name, rf.Pos), d)));
                    }
                    else
                    {
                        readSteps.Add((rec, d) => recordAccess.AddField(rec, rf.Name, rf.Pos,
                            readItem(null, d)));
                    }
                }
                else
                {
                    var skip = GetSkip(wf.Schema);
                    readSteps.Add((rec, d) => skip(d));
                }
            }

            // fill in defaults for any reader fields not in the writer schema
            foreach (Field rf in readerSchema)
            {
                if (writerSchema.Contains(rf.Name)) continue;

                var defaultStream = new MemoryStream();
                var defaultEncoder = new BinaryEncoder(defaultStream);

                defaultStream.Position = 0; // reset for writing
                Resolver.EncodeDefaultValue(defaultEncoder, rf.Schema, rf.DefaultValue);
                defaultStream.Flush();
	            var defaultBytes = defaultStream.ToArray();

                var readItem = ResolveReader(rf.Schema, rf.Schema);

                var rfInstance = rf;
                if(IsReusable(rf.Schema.Tag))
                {
                    readSteps.Add((rec, d) => recordAccess.AddField(rec, rfInstance.Name, rfInstance.Pos,
                        readItem(recordAccess.GetField(rec, rfInstance.Name, rfInstance.Pos),
                            new BinaryDecoder(new MemoryStream( defaultBytes)))));
                }
                else
                {
                    readSteps.Add((rec, d) => recordAccess.AddField(rec, rfInstance.Name, rfInstance.Pos,
                        readItem(null, new BinaryDecoder(new MemoryStream(defaultBytes)))));
                }
            }

            fieldReaderArray = readSteps.ToArray();
            return recordReader;
        }

        private object ReadRecord(object reuse, Decoder decoder, RecordAccess recordAccess, IEnumerable<FieldReader> readSteps )
        {
            var rec = recordAccess.CreateRecord(reuse);
            foreach (FieldReader fr in readSteps)
            {
                fr(rec, decoder);
                // TODO: on exception, report offending field
            }
            return rec;
        }

        private ReadItem ResolveUnion(UnionSchema writerSchema, Schema readerSchema)
        {
            var lookup = new ReadItem[writerSchema.Count];

            for (int i = 0; i < writerSchema.Count; i++)
            {
                var writerBranch = writerSchema[i];

                if (readerSchema is UnionSchema)
                {
                    var unionReader = (UnionSchema) readerSchema;
                    var readerBranch = unionReader.MatchingBranch(writerBranch);
                    if (readerBranch == -1)
                    {
                        lookup[i] = (r, d) => { throw new AvroException( "No matching schema for " + writerBranch + " in " + unionReader ); };
                    }
                    else
                    {
                        lookup[i] = ResolveReader(writerBranch, unionReader[readerBranch]);
                    }
                }
                else
                {
                    if (!readerSchema.CanRead(writerBranch))
                    {
                        lookup[i] = (r, d) => { throw new AvroException( "Schema mismatch Reader: " + ReaderSchema + ", writer: " + WriterSchema ); };
                    }
                    else
                    {
                        lookup[i] = ResolveReader(writerBranch, readerSchema);
                    }
                }
            }

            return (r, d) => ReadUnion(r, d, lookup);
        }

        private object ReadUnion(object reuse, Decoder d, ReadItem[] branchLookup)
        {
            return branchLookup[d.ReadUnionIndex()](reuse, d);
        }

        private ReadItem ResolveMap(MapSchema writerSchema, MapSchema readerSchema)
        {
            var rs = readerSchema.ValueSchema;
            var ws = writerSchema.ValueSchema;

            var reader = ResolveReader(ws, rs);
            var mapAccess = GetMapAccess(readerSchema);

            return (r,d) => ReadMap(r, d, mapAccess, reader);
        }

        private object ReadMap(object reuse, Decoder decoder, MapAccess mapAccess, ReadItem valueReader)
        {
            object map = mapAccess.Create(reuse);

            for (int n = (int)decoder.ReadMapStart(); n != 0; n = (int)decoder.ReadMapNext())
            {
                mapAccess.AddElements(map, n, valueReader, decoder, false);
            }
            return map;
        }

        private ReadItem ResolveArray(ArraySchema writerSchema, ArraySchema readerSchema)
        {
            var itemReader = ResolveReader(writerSchema.ItemSchema, readerSchema.ItemSchema);

            var arrayAccess = GetArrayAccess(readerSchema);
            return (r, d) => ReadArray(r, d, arrayAccess, itemReader, IsReusable(readerSchema.ItemSchema.Tag));
        }

        private object ReadArray(object reuse, Decoder decoder, ArrayAccess arrayAccess, ReadItem itemReader, bool itemReusable)
        {
            object array = arrayAccess.Create(reuse);
            int i = 0;
            for (int n = (int)decoder.ReadArrayStart(); n != 0; n = (int)decoder.ReadArrayNext())
            {
                arrayAccess.EnsureSize(ref array, i + n);
                arrayAccess.AddElements(array, n, i, itemReader, decoder, itemReusable);
                i += n;
            }
            arrayAccess.Resize(ref array, i);
            return array;
        }

        private ReadItem ResolveLogical(LogicalSchema writerSchema, LogicalSchema readerSchema)
        {
            var baseReader = ResolveReader(writerSchema.BaseSchema, readerSchema.BaseSchema);
            return (r, d) => readerSchema.LogicalType.ConvertToLogicalValue(baseReader(r, d), readerSchema);
        }

        private ReadItem ResolveFixed(FixedSchema writerSchema, FixedSchema readerSchema)
        {
            if (readerSchema.Size != writerSchema.Size)
            {
                throw new AvroException("Size mismatch between reader and writer fixed schemas. Writer: " + writerSchema +
                    ", reader: " + readerSchema);
            }
            var fixedAccess = GetFixedAccess(readerSchema);
            return (r, d) => ReadFixed(r, d, fixedAccess);
        }

        private object ReadFixed(object reuse, Decoder decoder, FixedAccess fixedAccess)
        {
            var fixedrec = fixedAccess.CreateFixed(reuse);
            decoder.ReadFixed(fixedAccess.GetFixedBuffer(fixedrec));
            return fixedrec;
        }

        /// <summary>
        /// Finds the branch of the union schema associated with the given schema.
        /// </summary>
        /// <param name="us">Union schema.</param>
        /// <param name="s">Schema to find in the union schema.</param>
        /// <returns>Schema branch in the union schema.</returns>
        protected static Schema FindBranch(UnionSchema us, Schema s)
        {
            int index = us.MatchingBranch(s);
            if (index >= 0) return us[index];
            throw new AvroException("No matching schema for " + s + " in " + us);
        }

        private object ReadNull(object reuse, Decoder decoder)
        {
            decoder.ReadNull();
            return null;
        }

        private object ReadBoolean(object reuse, Decoder decoder)
        {
            return decoder.ReadBoolean();
        }

        private ReadItem Read(DecoderRead decoderRead)
        {
            return (r, d) => decoderRead(d);
        }

        private DecoderSkip GetSkip(Schema writerSchema)
        {
            switch (writerSchema.Tag)
            {
                case Schema.Type.Null:
                    return d => d.SkipNull();
                case Schema.Type.Boolean:
                    return d => d.SkipBoolean();
                case Schema.Type.Int:
                    return d => d.SkipInt();
                case Schema.Type.Long:
                    return d => d.SkipLong();
                case Schema.Type.Float:
                    return d => d.SkipFloat();
                case Schema.Type.Double:
                    return d => d.SkipDouble();
                case Schema.Type.String:
                    return d => d.SkipString();
                case Schema.Type.Bytes:
                    return d => d.SkipBytes();
                case Schema.Type.Error:
                case Schema.Type.Record:
                    var recordSkips = new List<DecoderSkip>();
                    var recSchema = (RecordSchema)writerSchema;
                    recSchema.Fields.ForEach(r => recordSkips.Add(GetSkip(r.Schema)));
                    return d => recordSkips.ForEach(s=>s(d));
                case Schema.Type.Enumeration:
                    return d => d.SkipEnum();
                case Schema.Type.Fixed:
                    var size = ((FixedSchema)writerSchema).Size;
                    return d => d.SkipFixed(size);
                case Schema.Type.Array:
                    var itemSkip = GetSkip(((ArraySchema)writerSchema).ItemSchema);
                    return d =>
                    {
                        for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
                        {
                            for (long i = 0; i < n; i++) itemSkip(d);
                        }
                    };
                case Schema.Type.Map:
                    {
                        var valueSkip = GetSkip(((MapSchema)writerSchema).ValueSchema);
                        return d =>
                        {
                            for (long n = d.ReadMapStart(); n != 0; n = d.ReadMapNext())
                            {
                                for (long i = 0; i < n; i++) { d.SkipString(); valueSkip(d); }
                            }
                        };
                    }
                case Schema.Type.Union:
                    var unionSchema = (UnionSchema)writerSchema;
                    var lookup = new DecoderSkip[unionSchema.Count];
                    for (int i = 0; i < unionSchema.Count; i++)
                    {
                        lookup[i] = GetSkip( unionSchema[i] );
                    }
                    return d => lookup[d.ReadUnionIndex()](d);
                case Schema.Type.Logical:
                    var logicalSchema = (LogicalSchema)writerSchema;
                    return GetSkip(logicalSchema.BaseSchema);
                default:
                    throw new AvroException("Unknown schema type: " + writerSchema);
            }
        }

        /// <summary>
        /// Indicates if it's possible to reuse an object of the specified type. Generally
        /// false for immutable objects like int, long, string, etc but may differ between
        /// the Specific and Generic implementations. Used to avoid retrieving the existing
        /// value if it's not reusable.
        /// </summary>
        /// <param name="tag">Schema type to test for reusability.</param>
        protected virtual bool IsReusable(Schema.Type tag)
        {
            return true;
        }

        // interfaces to handle details of working with Specific vs Generic objects

        /// <summary>
        /// Defines the interface for a class that provides access to a record implementation.
        /// </summary>
        protected interface RecordAccess
        {
            /// <summary>
            /// Creates a new record object. Derived classes can override this to return an object of their choice.
            /// </summary>
            /// <param name="reuse">If appropriate, will reuse this object instead of constructing a new one</param>
            /// <returns></returns>
            object CreateRecord(object reuse);

            /// <summary>
            /// Used by the default implementation of ReadRecord() to get the existing field of a record object. The derived
            /// classes can override this to make their own interpretation of the record object.
            /// </summary>
            /// <param name="record">The record object to be probed into. This is guaranteed to be one that was returned
            /// by a previous call to CreateRecord.</param>
            /// <param name="fieldName">The name of the field to probe.</param>
            /// <param name="fieldPos">field number</param>
            /// <returns>The value of the field, if found. Null otherwise.</returns>
            object GetField(object record, string fieldName, int fieldPos);

            /// <summary>
            /// Used by the default implementation of ReadRecord() to add a field to a record object. The derived
            /// classes can override this to suit their own implementation of the record object.
            /// </summary>
            /// <param name="record">The record object to be probed into. This is guaranteed to be one that was returned
            /// by a previous call to CreateRecord.</param>
            /// <param name="fieldName">The name of the field to probe.</param>
            /// <param name="fieldPos">field number</param>
            /// <param name="fieldValue">The value to be added for the field</param>
            void AddField(object record, string fieldName, int fieldPos, object fieldValue);
        }

        /// <summary>
        /// Defines the interface for a class that provides access to an enum implementation.
        /// </summary>
        protected interface EnumAccess
        {
            /// <summary>
            /// Creates an enum value.
            /// </summary>
            /// <param name="reuse">Optional object to reuse as the enum value. May be null.</param>
            /// <param name="ordinal">Ordinal value of the enum entry.</param>
            /// <returns>An object representing the enum value.</returns>
            object CreateEnum(object reuse, int ordinal);
        }

        /// <summary>
        /// Defines the interface for a class that provides access to a fixed implementation.
        /// </summary>
        protected interface FixedAccess
        {
            /// <summary>
            /// Returns a fixed object.
            /// </summary>
            /// <param name="reuse">If appropriate, uses this object instead of creating a new one.</param>
            /// <returns>A fixed object with an appropriate buffer.</returns>
            object CreateFixed(object reuse);

            /// <summary>
            /// Returns a buffer of appropriate size to read data into.
            /// </summary>
            /// <param name="f">The fixed object. It is guaranteed that this is something that has been previously
            /// returned by CreateFixed</param>
            /// <returns>A byte buffer of fixed's size.</returns>
            byte[] GetFixedBuffer(object f);
        }

        /// <summary>
        /// Defines the interface for a class that provides access to an array implementation.
        /// </summary>
        protected interface ArrayAccess
        {
            /// <summary>
            /// Creates a new array object. The initial size of the object could be anything.
            /// </summary>
            /// <param name="reuse">If appropriate use this instead of creating a new one.</param>
            /// <returns>An object suitable to deserialize an avro array</returns>
            object Create(object reuse);

            /// <summary>
            /// Hint that the array should be able to handle at least targetSize elements. The array
            /// is not required to be resized
            /// </summary>
            /// <param name="array">Array object who needs to support targetSize elements. This is guaranteed to be somthing returned by
            /// a previous call to CreateArray().</param>
            /// <param name="targetSize">The new size.</param>
            void EnsureSize(ref object array, int targetSize);

            /// <summary>
            /// Resizes the array to the new value.
            /// </summary>
            /// <param name="array">Array object whose size is required. This is guaranteed to be somthing returned by
            /// a previous call to CreateArray().</param>
            /// <param name="targetSize">The new size.</param>
            void Resize(ref object array, int targetSize);

            /// <summary>
            /// Adds elements to the given array by reading values from the decoder.
            /// </summary>
            /// <param name="array">Array to add elements to.</param>
            /// <param name="elements">Number of elements to add.</param>
            /// <param name="index">Start adding elements to the array at this index.</param>
            /// <param name="itemReader">Delegate to read an item from the decoder.</param>
            /// <param name="decoder">Decoder to read from.</param>
            /// <param name="reuse">
            /// True to reuse each element in the array when deserializing. False to create a new
            /// object for each element.
            /// </param>
            void AddElements( object array, int elements, int index, ReadItem itemReader, Decoder decoder, bool reuse );
        }

        /// <summary>
        /// Defines the interface for a class that provides access to a map implementation.
        /// </summary>
        protected interface MapAccess
        {
            /// <summary>
            /// Creates a new map object.
            /// </summary>
            /// <param name="reuse">If appropriate, use this map object instead of creating a new one.</param>
            /// <returns>An empty map object.</returns>
            object Create(object reuse);

            /// <summary>
            /// Adds elements to the given map by reading values from the decoder.
            /// </summary>
            /// <param name="map">Map to add elements to.</param>
            /// <param name="elements">Number of elements to add.</param>
            /// <param name="itemReader">Delegate to read an item from the decoder.</param>
            /// <param name="decoder">Decoder to read from.</param>
            /// <param name="reuse">
            /// True to reuse each element in the map when deserializing. False to create a new
            /// object for each element.
            /// </param>
            void AddElements(object map, int elements, ReadItem itemReader, Decoder decoder, bool reuse);
        }

        private class SchemaPair
        {
            private Schema _writerSchema;
            private Schema _readerSchema;

            public SchemaPair( Schema writerSchema, Schema readerSchema )
            {
                _writerSchema = writerSchema;
                _readerSchema = readerSchema;
            }

            protected bool Equals( SchemaPair other )
            {
                return Equals( _writerSchema, other._writerSchema ) && Equals( _readerSchema, other._readerSchema );
            }

            public override bool Equals( object obj )
            {
                if( ReferenceEquals( null, obj ) ) return false;
                if( ReferenceEquals( this, obj ) ) return true;
                if( obj.GetType() != this.GetType() ) return false;
                return Equals( (SchemaPair) obj );
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ( ( _writerSchema != null ? _writerSchema.GetHashCode() : 0 ) * 397 ) ^ ( _readerSchema != null ? _readerSchema.GetHashCode() : 0 );
                }
            }
        }
    }
}
