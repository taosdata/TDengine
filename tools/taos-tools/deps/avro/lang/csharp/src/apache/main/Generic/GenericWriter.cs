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
    /// Defines the signature for a function that writes an object.
    /// </summary>
    /// <typeparam name="T">Type of object to write.</typeparam>
    /// <param name="t">Object to write.</param>
    public delegate void Writer<T>(T t);

    /// <summary>
    /// A typesafe wrapper around DefaultWriter. While a specific object of DefaultWriter
    /// allows the client to serialize a generic type, an object of this class allows
    /// only a single type of object to be serialized through it.
    /// </summary>
    /// <typeparam name="T">The type of object to be serialized.</typeparam>
    public class GenericWriter<T> : DatumWriter<T>
    {
        private readonly DefaultWriter writer;

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericWriter{T}"/> class.
        /// </summary>
        /// <param name="schema">Schema to use when writing.</param>
        public GenericWriter(Schema schema) : this(new DefaultWriter(schema))
        {

        }

        /// <inheritdoc/>
        public Schema Schema { get { return writer.Schema; } }

        /// <summary>
        /// Initializes a new instance of the <see cref="GenericWriter{T}"/> class from a
        /// <see cref="DefaultWriter"/>.
        /// </summary>
        /// <param name="writer">Write to initialize this new writer from.</param>
        public GenericWriter(DefaultWriter writer)
        {
            this.writer = writer;
        }

        /// <summary>
        /// Serializes the given object using this writer's schema.
        /// </summary>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder to use for serializing</param>
        public void Write(T value, Encoder encoder)
        {
            writer.Write(value, encoder);
        }
    }

    /// <summary>
    /// A General purpose writer for serializing objects into a Stream using
    /// Avro. This class implements a default way of serializing objects. But
    /// one can derive a class from this and override different methods to
    /// acheive results that are different from the default implementation.
    /// </summary>
    public class DefaultWriter
    {
        /// <summary>
        /// Schema that this object uses to write datum.
        /// </summary>
        public Schema Schema { get; private set; }

        /// <summary>
        /// Constructs a generic writer for the given schema.
        /// </summary>
        /// <param name="schema">The schema for the object to be serialized</param>
        public DefaultWriter(Schema schema)
        {
            Schema = schema;
        }

        /// <summary>
        /// Examines the <see cref="Schema"/> and dispatches the actual work to one
        /// of the other methods of this class. This allows the derived
        /// classes to override specific methods and get custom results.
        /// </summary>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder to use during serialization</param>
        public void Write<T>(T value, Encoder encoder)
        {
            Write(Schema, value, encoder);
        }

        /// <summary>
        /// Examines the schema and dispatches the actual work to one
        /// of the other methods of this class. This allows the derived
        /// classes to override specific methods and get custom results.
        /// </summary>
        /// <param name="schema">The schema to use for serializing</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder to use during serialization</param>
        public virtual void Write(Schema schema, object value, Encoder encoder)
        {
            switch (schema.Tag)
            {
                case Schema.Type.Null:
                    WriteNull(value, encoder);
                    break;
                case Schema.Type.Boolean:
                    Write<bool>(value, schema.Tag, encoder.WriteBoolean);
                    break;
                case Schema.Type.Int:
                    Write<int>(value, schema.Tag, encoder.WriteInt);
                    break;
                case Schema.Type.Long:
                    Write<long>(value, schema.Tag, encoder.WriteLong);
                    break;
                case Schema.Type.Float:
                    Write<float>(value, schema.Tag, encoder.WriteFloat);
                    break;
                case Schema.Type.Double:
                    Write<double>(value, schema.Tag, encoder.WriteDouble);
                    break;
                case Schema.Type.String:
                    Write<string>(value, schema.Tag, encoder.WriteString);
                    break;
                case Schema.Type.Bytes:
                    Write<byte[]>(value, schema.Tag, encoder.WriteBytes);
                    break;
                case Schema.Type.Record:
                case Schema.Type.Error:
                    WriteRecord(schema as RecordSchema, value, encoder);
                    break;
                case Schema.Type.Enumeration:
                    WriteEnum(schema as EnumSchema, value, encoder);
                    break;
                case Schema.Type.Fixed:
                    WriteFixed(schema as FixedSchema, value, encoder);
                    break;
                case Schema.Type.Array:
                    WriteArray(schema as ArraySchema, value, encoder);
                    break;
                case Schema.Type.Map:
                    WriteMap(schema as MapSchema, value, encoder);
                    break;
                case Schema.Type.Union:
                    WriteUnion(schema as UnionSchema, value, encoder);
                    break;
                case Schema.Type.Logical:
                    WriteLogical(schema as LogicalSchema, value, encoder);
                    break;
                default:
                    Error(schema, value);
                    break;
            }
        }

        /// <summary>
        /// Serializes a "null"
        /// </summary>
        /// <param name="value">The object to be serialized using null schema</param>
        /// <param name="encoder">The encoder to use while serialization</param>
        protected virtual void WriteNull(object value, Encoder encoder)
        {
            if (value != null) throw TypeMismatch(value, "null", "null");
        }

        /// <summary>
        /// A generic method to serialize primitive Avro types.
        /// </summary>
        /// <typeparam name="T">Type of the C# type to be serialized</typeparam>
        /// <param name="value">The value to be serialized</param>
        /// <param name="tag">The schema type tag</param>
        /// <param name="writer">The writer which should be used to write the given type.</param>
        protected virtual void Write<T>(object value, Schema.Type tag, Writer<T> writer)
        {
            if (!(value is T)) throw TypeMismatch(value, tag.ToString(), typeof(T).ToString());
            writer((T)value);
        }

        /// <summary>
        /// Serialized a record using the given RecordSchema. It uses GetField method
        /// to extract the field value from the given object.
        /// </summary>
        /// <param name="schema">The RecordSchema to use for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The Encoder for serialization</param>

        protected virtual void WriteRecord(RecordSchema schema, object value, Encoder encoder)
        {
            EnsureRecordObject(schema, value);
            foreach (Field field in schema)
            {
                try
                {
                    object obj = GetField(value, field.Name, field.Pos);
                    Write(field.Schema, obj, encoder);
                }
                catch (Exception ex)
                {
                    throw new AvroException(ex.Message + " in field " + field.Name, ex);
                }
            }
        }

        /// <summary>
        /// Ensures that the given value is a record and that it corresponds to the given schema.
        /// Throws an exception if either of those assertions are false.
        /// </summary>
        /// <param name="s">Schema associated with the record</param>
        /// <param name="value">Ensure this object is a record</param>
        protected virtual void EnsureRecordObject(RecordSchema s, object value)
        {
            if (value == null || !(value is GenericRecord) || !(value as GenericRecord).Schema.Equals(s))
            {
                throw TypeMismatch(value, "record", "GenericRecord");
            }
        }

        /// <summary>
        /// Extracts the field value from the given object. In this default implementation,
        /// value should be of type GenericRecord.
        /// </summary>
        /// <param name="value">The record value from which the field needs to be extracted</param>
        /// <param name="fieldName">The name of the field in the record</param>
        /// <param name="fieldPos">The position of field in the record</param>
        /// <returns></returns>
        protected virtual object GetField(object value, string fieldName, int fieldPos)
        {
            GenericRecord d = value as GenericRecord;
            return d.GetValue(fieldPos);
        }

        /// <summary>
        /// Serializes an enumeration. The default implementation expectes the value to be string whose
        /// value is the name of the enumeration.
        /// </summary>
        /// <param name="es">The EnumSchema for serialization</param>
        /// <param name="value">Value to be written</param>
        /// <param name="encoder">Encoder for serialization</param>
        protected virtual void WriteEnum(EnumSchema es, object value, Encoder encoder)
        {
            if (value == null || !(value is GenericEnum) || !(value as GenericEnum).Schema.Equals(es))
                throw TypeMismatch(value, "enum", "GenericEnum");
            encoder.WriteEnum(es.Ordinal((value as GenericEnum).Value));
        }

        /// <summary>
        /// Serialized an array. The default implementation calls EnsureArrayObject() to ascertain that the
        /// given value is an array. It then calls GetArrayLength() and GetArrayElement()
        /// to access the members of the array and then serialize them.
        /// </summary>
        /// <param name="schema">The ArraySchema for serialization</param>
        /// <param name="value">The value being serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected virtual void WriteArray(ArraySchema schema, object value, Encoder encoder)
        {
            EnsureArrayObject(value);
            long l = GetArrayLength(value);
            encoder.WriteArrayStart();
            encoder.SetItemCount(l);
            for (long i = 0; i < l; i++)
            {
                encoder.StartItem();
                Write(schema.ItemSchema, GetArrayElement(value, i), encoder);
            }
            encoder.WriteArrayEnd();
        }

        /// <summary>
        /// Checks if the given object is an array. If it is a valid array, this function returns normally. Otherwise,
        /// it throws an exception. The default implementation checks if the value is an array.
        /// </summary>
        /// <param name="value"></param>
        protected virtual void EnsureArrayObject(object value)
        {
            if (value == null || !(value is Array)) throw TypeMismatch(value, "array", "Array");
        }

        /// <summary>
        /// Returns the length of an array. The default implementation requires the object
        /// to be an array of objects and returns its length. The defaul implementation
        /// gurantees that EnsureArrayObject() has been called on the value before this
        /// function is called.
        /// </summary>
        /// <param name="value">The object whose array length is required</param>
        /// <returns>The array length of the given object</returns>
        protected virtual long GetArrayLength(object value)
        {
            return (value as Array).Length;
        }

        /// <summary>
        /// Returns the element at the given index from the given array object. The default implementation
        /// requires that the value is an object array and returns the element in that array. The defaul implementation
        /// gurantees that EnsureArrayObject() has been called on the value before this
        /// function is called.
        /// </summary>
        /// <param name="value">The array object</param>
        /// <param name="index">The index to look for</param>
        /// <returns>The array element at the index</returns>
        protected virtual object GetArrayElement(object value, long index)
        {
            return (value as Array).GetValue(index);
        }

        /// <summary>
        /// Serialized a map. The default implementation first ensure that the value is indeed a map and then uses
        /// GetMapSize() and GetMapElements() to access the contents of the map.
        /// </summary>
        /// <param name="schema">The MapSchema for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected virtual void WriteMap(MapSchema schema, object value, Encoder encoder)
        {
            EnsureMapObject(value);
            IDictionary<string, object> vv = (IDictionary<string, object>)value;
            encoder.WriteMapStart();
            encoder.SetItemCount(GetMapSize(value));
            foreach (KeyValuePair<string, object> obj in GetMapValues(vv))
            {
                encoder.StartItem();
                encoder.WriteString(obj.Key);
                Write(schema.ValueSchema, obj.Value, encoder);
            }
            encoder.WriteMapEnd();
        }

        /// <summary>
        /// Checks if the given object is a map. If it is a valid map, this function returns normally. Otherwise,
        /// it throws an exception. The default implementation checks if the value is an IDictionary&lt;string, object&gt;.
        /// </summary>
        /// <param name="value"></param>
        protected virtual void EnsureMapObject(object value)
        {
            if (value == null || !(value is IDictionary<string, object>)) throw TypeMismatch(value, "map", "IDictionary<string, object>");
        }

        /// <summary>
        /// Returns the size of the map object. The default implementation gurantees that EnsureMapObject has been
        /// successfully called with the given value. The default implementation requires the value
        /// to be an IDictionary&lt;string, object&gt; and returns the number of elements in it.
        /// </summary>
        /// <param name="value">The map object whose size is desired</param>
        /// <returns>The size of the given map object</returns>
        protected virtual long GetMapSize(object value)
        {
            return (value as IDictionary<string, object>).Count;
        }

        /// <summary>
        /// Returns the contents of the given map object. The default implementation guarantees that EnsureMapObject
        /// has been called with the given value. The defualt implementation of this method requires that
        /// the value is an IDictionary&lt;string, object&gt; and returns its contents.
        /// </summary>
        /// <param name="value">The map object whose size is desired</param>
        /// <returns>The contents of the given map object</returns>
        protected virtual IEnumerable<KeyValuePair<string, object>> GetMapValues(object value)
        {
            return value as IDictionary<string, object>;
        }

        /// <summary>
        /// Resolves the given value against the given UnionSchema and serializes the object against
        /// the resolved schema member. The default implementation of this method uses
        /// ResolveUnion to find the member schema within the UnionSchema.
        /// </summary>
        /// <param name="us">The UnionSchema to resolve against</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected virtual void WriteUnion(UnionSchema us, object value, Encoder encoder)
        {
            int index = ResolveUnion(us, value);
            encoder.WriteUnionIndex(index);
            Write(us[index], value, encoder);
        }

        /// <summary>
        /// Finds the branch within the given UnionSchema that matches the given object. The default implementation
        /// calls Matches() method in the order of branches within the UnionSchema. If nothing matches, throws
        /// an exception.
        /// </summary>
        /// <param name="us">The UnionSchema to resolve against</param>
        /// <param name="obj">The object that should be used in matching</param>
        /// <returns></returns>
        protected virtual int ResolveUnion(UnionSchema us, object obj)
        {
            for (int i = 0; i < us.Count; i++)
            {
                if (Matches(us[i], obj)) return i;
            }
            throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
        }

        /// <summary>
        /// Serializes a logical value object by using the underlying logical type to convert the value
        /// to its base value.
        /// </summary>
        /// <param name="ls">The schema for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected virtual void WriteLogical(LogicalSchema ls, object value, Encoder encoder)
        {
            Write(ls.BaseSchema, ls.LogicalType.ConvertToBaseValue(value, ls), encoder);
        }

        /// <summary>
        /// Serialized a fixed object. The default implementation requires that the value is
        /// a GenericFixed object with an identical schema as es.
        /// </summary>
        /// <param name="es">The schema for serialization</param>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder for serialization</param>
        protected virtual void WriteFixed(FixedSchema es, object value, Encoder encoder)
        {
            if (value == null || !(value is GenericFixed) || !(value as GenericFixed).Schema.Equals(es))
            {
                throw TypeMismatch(value, "fixed", "GenericFixed");
            }
            GenericFixed ba = (GenericFixed)value;
            encoder.WriteFixed(ba.Value);
        }

        /// <summary>
        /// Creates a new <see cref="AvroException"/> and uses the provided parameters to build an
        /// exception message indicathing there was a type mismatch.
        /// </summary>
        /// <param name="obj">Object whose type does not the expected type</param>
        /// <param name="schemaType">Schema that we tried to write against</param>
        /// <param name="type">Type that we expected</param>
        /// <returns>A new <see cref="AvroException"/> indicating a type mismatch.</returns>
        protected AvroException TypeMismatch(object obj, string schemaType, string type)
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
        /// <remarks>
        /// FIXME: This method of determining the Union branch has problems. If the data is IDictionary&lt;string, object&gt;
        /// if there are two branches one with record schema and the other with map, it choose the first one. Similarly if
        /// the data is byte[] and there are fixed and bytes schemas as branches, it choose the first one that matches.
        /// Also it does not recognize the arrays of primitive types.
        /// </remarks>
        /// <param name="sc">Schema to compare</param>
        /// <param name="obj">Object to compare</param>
        /// <returns>True if the two parameters are compatible, false otherwise.</returns>
        protected virtual bool Matches(Schema sc, object obj)
        {
            if (obj == null && sc.Tag != Avro.Schema.Type.Null) return false;
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
                case Schema.Type.Record:
                    //return obj is GenericRecord && (obj as GenericRecord).Schema.Equals(s);
                    return obj is GenericRecord && (obj as GenericRecord).Schema.SchemaName.Equals((sc as RecordSchema).SchemaName);
                case Schema.Type.Enumeration:
                    //return obj is GenericEnum && (obj as GenericEnum).Schema.Equals(s);
                    return obj is GenericEnum && (obj as GenericEnum).Schema.SchemaName.Equals((sc as EnumSchema).SchemaName);
                case Schema.Type.Array:
                    return obj is Array && !(obj is byte[]);
                case Schema.Type.Map:
                    return obj is IDictionary<string, object>;
                case Schema.Type.Union:
                    return false;   // Union directly within another union not allowed!
                case Schema.Type.Fixed:
                    //return obj is GenericFixed && (obj as GenericFixed).Schema.Equals(s);
                    return obj is GenericFixed && (obj as GenericFixed).Schema.SchemaName.Equals((sc as FixedSchema).SchemaName);
                case Schema.Type.Logical:
                    return (sc as LogicalSchema).LogicalType.IsInstanceOfLogicalType(obj);
                default:
                    throw new AvroException("Unknown schema type: " + sc.Tag);
            }
        }
    }
}
