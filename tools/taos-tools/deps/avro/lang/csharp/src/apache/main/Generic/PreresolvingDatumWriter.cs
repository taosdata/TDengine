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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Encoder = Avro.IO.Encoder;

namespace Avro.Generic
{
    /// <summary>
    /// A general purpose writer of data from avro streams. This writer analyzes the writer schema
    /// when constructed so that writes can be more efficient. Once constructed, a writer can be reused or shared among threads
    /// to avoid incurring more resolution costs.
    /// </summary>
    public abstract class PreresolvingDatumWriter<T> : DatumWriter<T>
    {
        /// <inheritdoc/>
        public Schema Schema { get; private set; }

        /// <summary>
        /// Defines the signature for a method that writes a value to an encoder.
        /// </summary>
        /// <param name="value">Value to write</param>
        /// <param name="encoder">Encoder to write to</param>
        protected delegate void WriteItem(object value, Encoder encoder);

        private readonly WriteItem _writer;
        private readonly ArrayAccess _arrayAccess;
        private readonly MapAccess _mapAccess;

        private readonly Dictionary<RecordSchema,WriteItem> _recordWriters = new Dictionary<RecordSchema,WriteItem>();

        /// <inheritdoc/>
        public void Write(T datum, Encoder encoder)
        {
            _writer(datum, encoder);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PreresolvingDatumWriter{T}"/> class.
        /// </summary>
        /// <param name="schema">Schema used by this writer</param>
        /// <param name="arrayAccess">Object used to access array properties</param>
        /// <param name="mapAccess">Object used to access map properties</param>
        protected PreresolvingDatumWriter(Schema schema, ArrayAccess arrayAccess, MapAccess mapAccess)
        {
            Schema = schema;
            _arrayAccess = arrayAccess;
            _mapAccess = mapAccess;
            _writer = ResolveWriter(schema);
        }

        private WriteItem ResolveWriter( Schema schema )
        {
            switch (schema.Tag)
            {
                case Schema.Type.Null:
                    return WriteNull;
                case Schema.Type.Boolean:
                    return (v, e) => Write<bool>( v, schema.Tag, e.WriteBoolean );
                case Schema.Type.Int:
                    return (v, e) => Write<int>( v, schema.Tag, e.WriteInt );
                case Schema.Type.Long:
                    return (v, e) => Write<long>( v, schema.Tag, e.WriteLong );
                case Schema.Type.Float:
                    return (v, e) => Write<float>( v, schema.Tag, e.WriteFloat );
                case Schema.Type.Double:
                    return (v, e) => Write<double>( v, schema.Tag, e.WriteDouble );
                case Schema.Type.String:
                    return (v, e) => Write<string>( v, schema.Tag, e.WriteString );
                case Schema.Type.Bytes:
                    return (v, e) => Write<byte[]>( v, schema.Tag, e.WriteBytes );
                case Schema.Type.Error:
                case Schema.Type.Record:
                    return ResolveRecord((RecordSchema) schema);
                case Schema.Type.Enumeration:
                    return ResolveEnum(schema as EnumSchema);
                case Schema.Type.Fixed:
                    return (v, e) => WriteFixed(schema as FixedSchema, v, e);
                case Schema.Type.Array:
                    return ResolveArray((ArraySchema)schema);
                case Schema.Type.Map:
                    return ResolveMap((MapSchema)schema);
                case Schema.Type.Union:
                    return ResolveUnion((UnionSchema)schema);
                case Schema.Type.Logical:
                    return ResolveLogical((LogicalSchema)schema);
                default:
                    return (v, e) => Error(schema, v);
            }
        }

        /// <summary>
        /// Serializes a "null"
        /// </summary>
        /// <param name="value">The object to be serialized using null schema</param>
        /// <param name="encoder">The encoder to use while serialization</param>
        protected void WriteNull(object value, Encoder encoder)
        {
            if (value != null) throw TypeMismatch(value, "null", "null");
        }

        /// <summary>
        /// A generic method to serialize primitive Avro types.
        /// </summary>
        /// <typeparam name="TValue">Type of the C# type to be serialized</typeparam>
        /// <param name="value">The value to be serialized</param>
        /// <param name="tag">The schema type tag</param>
        /// <param name="writer">The writer which should be used to write the given type.</param>
        protected void Write<TValue>(object value, Schema.Type tag, Writer<TValue> writer)
        {
            if (!(value is TValue)) throw TypeMismatch(value, tag.ToString(), typeof(TValue).ToString());
            writer((TValue)value);
        }


        /// <summary>
        /// Serializes a record using the given RecordSchema. It uses GetField method
        /// to extract the field value from the given object.
        /// </summary>
        /// <param name="recordSchema">The RecordSchema to use for serialization</param>
        private WriteItem ResolveRecord(RecordSchema recordSchema)
        {
            WriteItem recordResolver;
            if (_recordWriters.TryGetValue(recordSchema, out recordResolver))
            {
                return recordResolver;
            }
            var writeSteps = new RecordFieldWriter[recordSchema.Fields.Count];
            recordResolver = (v, e) => WriteRecordFields(v, writeSteps, e);

            _recordWriters.Add(recordSchema, recordResolver);

            int index = 0;
            foreach (Field field in recordSchema)
            {
                var record = new RecordFieldWriter
                                 {
                                     WriteField = ResolveWriter(field.Schema),
                                     Field = field
                                 };
                writeSteps[index++] = record;
            }

            return recordResolver;
        }

        /// <summary>
        /// Writes each field of a record to the encoder.
        /// </summary>
        /// <param name="record">Record to write</param>
        /// <param name="writers">Writers for each field in the record</param>
        /// <param name="encoder">Encoder to write to</param>
        protected abstract void WriteRecordFields(object record, RecordFieldWriter[] writers, Encoder encoder);

        /// <summary>
        /// Correlates a record field with the writer used to serialize that field.
        /// </summary>
        protected class RecordFieldWriter
        {
            /// <summary>
            /// Delegate used to write the <see cref="Field"/> to an encoder.
            /// </summary>
            public WriteItem WriteField { get; set; }

            /// <summary>
            /// Field this object is associated with.
            /// </summary>
            public Field Field { get; set; }
        }

        /// <summary>
        /// Ensures that the given value is a record and that it corresponds to the given schema.
        /// Throws an exception if either of those assertions are false.
        /// </summary>
        /// <param name="recordSchema">Schema associated with the record</param>
        /// <param name="value">Ensure this object is a record</param>
        protected abstract void EnsureRecordObject(RecordSchema recordSchema, object value);

        /// <summary>
        /// Writes a field from the <paramref name="record"/> to the <paramref name="encoder"/>
        /// using the <paramref name="writer"/>.
        /// </summary>
        /// <param name="record">The record value from which the field needs to be extracted</param>
        /// <param name="fieldName">The name of the field in the record</param>
        /// <param name="fieldPos">The position of field in the record</param>
        /// <param name="writer">Used to write the field value to the encoder</param>
        /// <param name="encoder">Encoder to write to</param>
        protected abstract void WriteField(object record, string fieldName, int fieldPos, WriteItem writer, Encoder encoder );

        /// <summary>
        /// Serializes an enumeration.
        /// </summary>
        /// <param name="es">The EnumSchema for serialization</param>
        protected abstract WriteItem ResolveEnum(EnumSchema es);

        /// <summary>
        /// Creates a <see cref="WriteItem"/> delegate that serializes an array.
        /// The default implementation calls EnsureArrayObject() to ascertain that the
        /// given value is an array. It then calls GetArrayLength() and GetArrayElement()
        /// to access the members of the array and then serialize them.
        /// </summary>
        /// <param name="schema">The ArraySchema for serialization</param>
        /// <returns>A <see cref="WriteItem"/> that serializes an array.</returns>
        protected WriteItem ResolveArray(ArraySchema schema)
        {
            var itemWriter = ResolveWriter(schema.ItemSchema);
            return (d,e) => WriteArray(itemWriter, d, e);
        }

        private void WriteArray(WriteItem itemWriter, object array, Encoder encoder)
        {
            _arrayAccess.EnsureArrayObject(array);
            long l = _arrayAccess.GetArrayLength(array);
            encoder.WriteArrayStart();
            encoder.SetItemCount(l);
            _arrayAccess.WriteArrayValues(array, itemWriter, encoder);
            encoder.WriteArrayEnd();
        }

        /// <summary>
        /// Serializes a logical value object by using the underlying logical type to convert the value
        /// to its base value.
        /// </summary>
        /// <param name="schema">The logical schema.</param>
        protected WriteItem ResolveLogical(LogicalSchema schema)
        {
            var baseWriter = ResolveWriter(schema.BaseSchema);
            return (d, e) => baseWriter(schema.LogicalType.ConvertToBaseValue(d, schema), e);
        }

        private WriteItem ResolveMap(MapSchema mapSchema)
        {
            var itemWriter = ResolveWriter(mapSchema.ValueSchema);
            return (v, e) => WriteMap(itemWriter, v, e);
        }

        /// <summary>
        /// Serializes a map. The default implementation first ensure that the value is indeed a map and then uses
        /// GetMapSize() and GetMapElements() to access the contents of the map.
        /// </summary>
        /// <param name="itemWriter">Delegate used to write each map item.</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected void WriteMap(WriteItem itemWriter, object value, Encoder encoder)
        {
            _mapAccess.EnsureMapObject(value);
            encoder.WriteMapStart();
            encoder.SetItemCount(_mapAccess.GetMapSize(value));
            _mapAccess.WriteMapValues(value, itemWriter, encoder);
            encoder.WriteMapEnd();
        }


        private WriteItem ResolveUnion(UnionSchema unionSchema)
        {
            var branchSchemas = unionSchema.Schemas.ToArray();
            var branchWriters = new WriteItem[branchSchemas.Length];
            int branchIndex = 0;
            foreach (var branch in branchSchemas)
            {
                branchWriters[branchIndex++] = ResolveWriter(branch);
            }


            return (v, e) => WriteUnion(unionSchema, branchSchemas, branchWriters, v, e);
        }

        /// <summary>
        /// Resolves the given value against the given UnionSchema and serializes the object against
        /// the resolved schema member.
        /// </summary>
        /// <param name="unionSchema">The UnionSchema to resolve against</param>
        /// <param name="branchSchemas">Schemas for each type in the union</param>
        /// <param name="branchWriters">Writers for each type in the union</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        private void WriteUnion(UnionSchema unionSchema, Schema[] branchSchemas, WriteItem[] branchWriters, object value, Encoder encoder)
        {
            int index = ResolveUnion(unionSchema, branchSchemas, value);
            encoder.WriteUnionIndex(index);
            branchWriters[index](value, encoder);
        }

        /// <summary>
        /// Finds the branch within the given UnionSchema that matches the given object. The default implementation
        /// calls Matches() method in the order of branches within the UnionSchema. If nothing matches, throws
        /// an exception.
        /// </summary>
        /// <param name="us">The UnionSchema to resolve against</param>
        /// <param name="branchSchemas">Schemas for types within the union</param>
        /// <param name="obj">The object that should be used in matching</param>
        /// <returns>
        /// Index of the schema in <paramref name="branchSchemas"/> that matches the <paramref name="obj"/>.
        /// </returns>
        /// <exception cref="AvroException">
        /// No match found for the object in the union schema.
        /// </exception>
        protected int ResolveUnion(UnionSchema us, Schema[] branchSchemas, object obj)
        {
            for (int i = 0; i < branchSchemas.Length; i++)
            {
                if (UnionBranchMatches(branchSchemas[i], obj)) return i;
            }
            throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
        }

        /// <summary>
        /// Serialized a fixed object. The default implementation requires that the value is
        /// a GenericFixed object with an identical schema as es.
        /// </summary>
        /// <param name="es">The schema for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected abstract void WriteFixed(FixedSchema es, object value, Encoder encoder);

        /// <summary>
        /// Creates a new <see cref="AvroException"/> and uses the provided parameters to build an
        /// exception message indicathing there was a type mismatch.
        /// </summary>
        /// <param name="obj">Object whose type does not the expected type</param>
        /// <param name="schemaType">Schema that we tried to write against</param>
        /// <param name="type">Type that we expected</param>
        /// <returns>A new <see cref="AvroException"/> indicating a type mismatch.</returns>
        protected static AvroException TypeMismatch(object obj, string schemaType, string type)
        {
            return new AvroException(type + " required to write against " + schemaType + " schema but found " + (null == obj ? "null" : obj.GetType().ToString()) );
        }

        private void Error(Schema schema, Object value)
        {
            throw new AvroTypeException("Not a " + schema + ": " + value);
        }

        /// <summary>
        /// Tests whether the given schema an object are compatible.
        /// </summary>
        /// <param name="sc">Schema to compare</param>
        /// <param name="obj">Object to compare</param>
        /// <returns>True if the two parameters are compatible, false otherwise.</returns>
        protected abstract bool UnionBranchMatches(Schema sc, object obj);

        /// <summary>
        /// Obsolete - This will be removed from the public API in a future version.
        /// </summary>
        [Obsolete("This will be removed from the public API in a future version.")]
        protected interface EnumAccess
        {
            /// <summary>
            /// Obsolete - This will be removed from the public API in a future version.
            /// </summary>
            [Obsolete("This will be removed from the public API in a future version.")]
            void WriteEnum(object value);
        }

        /// <summary>
        /// Defines the interface for a class that provides access to an array implementation.
        /// </summary>
        protected interface ArrayAccess
        {
            /// <summary>
            /// Checks if the given object is an array. If it is a valid array, this function returns normally. Otherwise,
            /// it throws an exception. The default implementation checks if the value is an array.
            /// </summary>
            /// <param name="value"></param>
            void EnsureArrayObject(object value);

            /// <summary>
            /// Returns the length of an array. The default implementation requires the object
            /// to be an array of objects and returns its length. The defaul implementation
            /// gurantees that EnsureArrayObject() has been called on the value before this
            /// function is called.
            /// </summary>
            /// <param name="value">The object whose array length is required</param>
            /// <returns>The array length of the given object</returns>
            long GetArrayLength(object value);

            /// <summary>
            /// Writes each value in the given array to the <paramref name="encoder"/> using the
            /// <paramref name="valueWriter"/>. The default implementation of this method requires
            /// that the <paramref name="array"/> is an object array.
            /// </summary>
            /// <param name="array">The array object</param>
            /// <param name="valueWriter">Value writer to send the array to.</param>
            /// <param name="encoder">Encoder to the write the array values to.</param>
            void WriteArrayValues(object array, WriteItem valueWriter, Encoder encoder);
        }

        /// <summary>
        /// Defines the interface for a class that provides access to a map implementation.
        /// </summary>
        /// <seealso cref="DictionaryMapAccess"/>
        protected interface MapAccess
        {
            /// <summary>
            /// Checks if the given object is a map. If it is a valid map, this function returns normally. Otherwise,
            /// it throws an exception. The default implementation checks if the value is an IDictionary&lt;string, object&gt;.
            /// </summary>
            /// <param name="value">Ensures that this object is a valid map.</param>
            void EnsureMapObject(object value);

            /// <summary>
            /// Returns the size of the map object. The default implementation gurantees that EnsureMapObject has been
            /// successfully called with the given value. The default implementation requires the value
            /// to be an IDictionary&lt;string, object&gt; and returns the number of elements in it.
            /// </summary>
            /// <param name="value">The map object whose size is desired</param>
            /// <returns>The size of the given map object</returns>
            long GetMapSize(object value);

            /// <summary>
            /// Writes each value in the given map to the <paramref name="encoder"/> using the
            /// <paramref name="valueWriter"/>. The default implementation of this method requires
            /// that the <paramref name="map"/> is an IDictionary&lt;string, object&gt;.
            /// </summary>
            /// <param name="map">Map object to write the contents of.</param>
            /// <param name="valueWriter">Value writer to send the map to.</param>
            /// <param name="encoder">Encoder to the write the map values to.</param>
            void WriteMapValues(object map, WriteItem valueWriter, Encoder encoder);
        }

        /// <summary>
        /// Provides access to map properties from an <see cref="IDictionary"/>.
        /// </summary>
        protected class DictionaryMapAccess : MapAccess
        {
            /// <inheritdoc/>
            public void EnsureMapObject(object value)
            {
                if (value as IDictionary == null)
                {
                    throw TypeMismatch( value, "map", "IDictionary" );
                }
            }

            /// <inheritdoc/>
            public long GetMapSize(object value)
            {
                return ((IDictionary) value).Count;
            }

            /// <inheritdoc/>
            public void WriteMapValues(object map, WriteItem valueWriter, Encoder encoder)
            {
                foreach (DictionaryEntry entry in (IDictionary)map)
                {
                    encoder.StartItem();
                    encoder.WriteString(entry.Key.ToString());
                    valueWriter(entry.Value, encoder);
                }
            }
        }
    }
}
