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

namespace Avro.Generic
{
    /// <summary>
    /// <see cref="PreresolvingDatumReader{T}"/> for reading data to <see cref="GenericRecord"/>
    /// classes or primitives.
    /// <see cref="PreresolvingDatumReader{T}">For more information about performance considerations
    /// for choosing this implementation</see>.
    /// </summary>
    /// <typeparam name="T">Type to deserialize data into.</typeparam>
    public class GenericDatumReader<T> : PreresolvingDatumReader<T>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="GenericDatumReader{T}"/> class.
        /// </summary>
        /// <param name="writerSchema">Schema that was used to write the data.</param>
        /// <param name="readerSchema">Schema to use to read the data.</param>
        public GenericDatumReader(Schema writerSchema, Schema readerSchema) : base(writerSchema, readerSchema)
        {
        }

        /// <inheritdoc/>
        protected override bool IsReusable(Schema.Type tag)
        {
            switch (tag)
            {
                case Schema.Type.Double:
                case Schema.Type.Boolean:
                case Schema.Type.Int:
                case Schema.Type.Long:
                case Schema.Type.Float:
                case Schema.Type.Bytes:
                case Schema.Type.String:
                case Schema.Type.Null:
                    return false;
            }
            return true;
        }

        /// <inheritdoc/>
        protected override ArrayAccess GetArrayAccess(ArraySchema readerSchema)
        {
            return new GenericArrayAccess();
        }

        /// <inheritdoc/>
        protected override EnumAccess GetEnumAccess(EnumSchema readerSchema)
        {
            return new GenericEnumAccess(readerSchema);
        }

        /// <inheritdoc/>
        protected override MapAccess GetMapAccess(MapSchema readerSchema)
        {
            return new GenericMapAccess();
        }

        /// <inheritdoc/>
        protected override RecordAccess GetRecordAccess(RecordSchema readerSchema)
        {
            return new GenericRecordAccess(readerSchema);
        }

        /// <inheritdoc/>
        protected override FixedAccess GetFixedAccess(FixedSchema readerSchema)
        {
            return new GenericFixedAccess(readerSchema);
        }

        class GenericEnumAccess : EnumAccess
        {
            private EnumSchema schema;

            public GenericEnumAccess(EnumSchema schema)
            {
                this.schema = schema;
            }

            public object CreateEnum(object reuse, int ordinal)
            {
                if (reuse is GenericEnum)
                {
                    var ge = (GenericEnum) reuse;
                    if (ge.Schema.Equals(this.schema))
                    {
                        ge.Value = this.schema[ordinal];
                        return ge;
                    }
                }
                return new GenericEnum(this.schema, this.schema[ordinal]);
            }
        }

        internal class GenericRecordAccess : RecordAccess
        {
            private RecordSchema schema;

            public GenericRecordAccess(RecordSchema schema)
            {
                this.schema = schema;
            }

            public object CreateRecord(object reuse)
            {
                GenericRecord ru = (reuse == null || !(reuse is GenericRecord) || !(reuse as GenericRecord).Schema.Equals(this.schema)) ?
                    new GenericRecord(this.schema) :
                    reuse as GenericRecord;
                return ru;
            }

            public object GetField(object record, string fieldName, int fieldPos)
            {
                object result;
                if(!((GenericRecord)record).TryGetValue(fieldPos, out result))
                {
                    return null;
                }
                return result;
            }

            public void AddField(object record, string fieldName, int fieldPos, object fieldValue)
            {
                ((GenericRecord)record).Add(fieldName, fieldValue);
            }
        }

        class GenericFixedAccess : FixedAccess
        {
            private FixedSchema schema;

            public GenericFixedAccess(FixedSchema schema)
            {
                this.schema = schema;
            }

            public object CreateFixed(object reuse)
            {
                return (reuse is GenericFixed && (reuse as GenericFixed).Schema.Equals(this.schema)) ?
                    reuse : new GenericFixed(this.schema);
            }

            public byte[] GetFixedBuffer( object f )
            {
                return ((GenericFixed)f).Value;
            }
        }

        class GenericArrayAccess : ArrayAccess
        {
            public object Create(object reuse)
            {
                    return (reuse is object[]) ? reuse : new object[0];
            }

            public void EnsureSize(ref object array, int targetSize)
            {
                if (((object[])array).Length < targetSize)
                    SizeTo(ref array, targetSize);
            }

            public void Resize(ref object array, int targetSize)
            {
                SizeTo(ref array, targetSize);
            }

            public void AddElements( object arrayObj, int elements, int index, ReadItem itemReader, Decoder decoder, bool reuse )
            {
                var array = (object[]) arrayObj;
                for (int i = index; i < index + elements; i++)
                {
                    array[i] = reuse ? itemReader(array[i], decoder) : itemReader(null, decoder);
                }
            }

            private static void SizeTo(ref object array, int targetSize)
            {
                var o = (object[]) array;
                Array.Resize(ref o, targetSize);
                array = o;
            }
        }

        class GenericMapAccess : MapAccess
        {
            public object Create(object reuse)
            {
                if (reuse is IDictionary<string, object>)
                {
                    var result = (IDictionary<string, object>)reuse;
                    result.Clear();
                    return result;
                }
                return new Dictionary<string, object>();
            }

            public void AddElements(object mapObj, int elements, ReadItem itemReader, Decoder decoder, bool reuse)
            {
                var map = (IDictionary<string, object>)mapObj;
                for (int i = 0; i < elements; i++)
                {
                    var key = decoder.ReadString();
                    map[key] = itemReader(null, decoder);
                }
            }
        }
    }
}
