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
using Avro.IO;
using Avro.Specific;

namespace Avro.Reflect
{
    /// <summary>
    /// Class for writing data from any specific objects
    /// </summary>
    public class ReflectDefaultWriter : SpecificDefaultWriter
    {
        private ClassCache _classCache = new ClassCache();

        /// <summary>
        /// Class cache
        /// </summary>
        public ClassCache ClassCache { get => _classCache; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="objType"></param>
        /// <param name="schema"></param>
        /// <param name="cache"></param>
        public ReflectDefaultWriter(Type objType, Schema schema, ClassCache cache)
            : base(schema)
        {
            if (cache != null)
            {
                _classCache = cache;
            }

            _classCache.LoadClassCache(objType, schema);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="schema"></param>
        public ReflectDefaultWriter(Schema schema)
            : base(schema)
        {
        }

        /// <summary>
        /// Serialized a record using the given RecordSchema. It uses GetField method
        /// to extract the field value from the given object.
        /// </summary>
        /// <param name="schema">The RecordSchema to use for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The Encoder for serialization</param>

        protected override void WriteRecord(RecordSchema schema, object value, Encoder encoder)
        {
            foreach (Field field in schema)
            {
                try
                {
                    var v = _classCache.GetClass(schema).GetValue(value, field);

                    Write(field.Schema, v, encoder);
                }
                catch (Exception ex)
                {
                    throw new AvroException(ex.Message + " in field " + field.Name, ex);
                }
            }
        }

        /// <summary>
        /// Validates that the record is a fixed record object and that the schema in the object is the
        /// same as the given writer schema. Writes the given fixed record into the given encoder
        /// </summary>
        /// <param name="schema">writer schema</param>
        /// <param name="value">fixed object to write</param>
        /// <param name="encoder">encoder to write to</param>
        protected override void WriteFixed(FixedSchema schema, object value, Encoder encoder)
        {
            var fixedrec = value as byte[];
            if (fixedrec == null)
            {
                throw new AvroTypeException("Fixed object is not derived from byte[]");
            }

            if (fixedrec.Length != schema.Size)
            {
                throw new AvroTypeException($"Fixed object length is not the same as schema length {schema.Size}");
            }

            encoder.WriteFixed(fixedrec);
        }

        /// <summary>
        /// Serialized an array. The default implementation calls EnsureArrayObject() to ascertain that the
        /// given value is an array. It then calls GetArrayLength() and GetArrayElement()
        /// to access the members of the array and then serialize them.
        /// </summary>
        /// <param name="schema">The ArraySchema for serialization</param>
        /// <param name="value">The value being serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected override void WriteArray(ArraySchema schema, object value, Encoder encoder)
        {
            var arr = value as IEnumerable;
            if (arr == null)
            {
                throw new AvroTypeException("Array does not implement have registered ReflectArray derived type");
            }

            var arrayHelper = _classCache.GetArrayHelper(schema, (IEnumerable)value);
            long l = arrayHelper.Count();
            encoder.WriteArrayStart();
            encoder.SetItemCount(l);
            foreach (var v in arr)
            {
                encoder.StartItem();
                Write(schema.ItemSchema, v, encoder);
            }

            encoder.WriteArrayEnd();
        }

        /// <summary>
        /// Writes the given map into the given encoder.
        /// </summary>
        /// <param name="schema">writer schema</param>
        /// <param name="value">map to write</param>
        /// <param name="encoder">encoder to write to</param>
        protected override void WriteMap(MapSchema schema, object value, Encoder encoder)
        {
            if (value == null)
            {
                throw new AvroTypeException("Map is null - use a union for nullable types");
            }

            base.WriteMap(schema, value, encoder);
        }

        /// <summary>
        /// Determines whether an object matches a schema. In the case of enums and records the code looks up the
        /// Enum and class caches respectively. Used when writing unions.
        /// </summary>
        /// <param name="sc"></param>
        /// <param name="obj"></param>
        /// <returns></returns>
        protected override bool Matches(Schema sc, object obj)
        {
            if (obj == null && sc.Tag != Schema.Type.Null)
            {
                return false;
            }

            switch (sc.Tag)
            {
                case Schema.Type.Null:
                    return obj == null;
                case Schema.Type.Boolean:
                    return obj is bool;
                case Schema.Type.Int:
                    return obj is int;
                case Schema.Type.Long:
                    return obj is long;
                case Schema.Type.Float:
                    return obj is float;
                case Schema.Type.Double:
                    return obj is double;
                case Schema.Type.Bytes:
                    return obj is byte[];
                case Schema.Type.String:
                    return obj is string;
                case Schema.Type.Error:
                case Schema.Type.Record:
                    return _classCache.GetClass(sc as RecordSchema).GetClassType() == obj.GetType();
                case Schema.Type.Enumeration:
                    return EnumCache.GetEnumeration(sc as EnumSchema) == obj.GetType();
                case Schema.Type.Array:
                    return obj is IEnumerable;
                case Schema.Type.Map:
                    return obj is IDictionary;
                case Schema.Type.Union:
                    return false;   // Union directly within another union not allowed!
                case Schema.Type.Fixed:
                    return obj is byte[];
                default:
                    throw new AvroException("Unknown schema type: " + sc.Tag);
            }
        }
    }
}
