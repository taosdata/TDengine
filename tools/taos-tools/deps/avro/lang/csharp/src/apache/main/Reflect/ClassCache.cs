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
using System.Collections.Concurrent;

namespace Avro.Reflect
{
    /// <summary>
    /// Class holds a cache of C# classes and their properties. The key for the cache is the schema full name.
    /// </summary>
    public class ClassCache
    {
        private static ConcurrentBag<IAvroFieldConverter> _defaultConverters = new ConcurrentBag<IAvroFieldConverter>();

        private ConcurrentDictionary<string, DotnetClass> _nameClassMap = new ConcurrentDictionary<string, DotnetClass>();

        private ConcurrentDictionary<string, Type> _nameArrayMap = new ConcurrentDictionary<string, Type>();
        private ConcurrentDictionary<string, Schema> _previousFields = new ConcurrentDictionary<string, Schema>();

        private void AddClassNameMapItem(RecordSchema schema, Type dotnetClass)
        {
            if (schema != null && GetClass(schema) != null)
            {
                return;
            }

            if (!dotnetClass.IsClass)
            {
                throw new AvroException($"Type {dotnetClass.Name} is not a class");
            }

            _nameClassMap.TryAdd(schema.Fullname, new DotnetClass(dotnetClass, schema, this));
        }

        /// <summary>
        /// Add a default field converter
        /// </summary>
        /// <param name="converter"></param>
        public static void AddDefaultConverter(IAvroFieldConverter converter)
        {
            _defaultConverters.Add(converter);
        }

        /// <summary>
        /// Add a converter defined using Func&lt;&gt;. The converter will be used whenever the source and target types
        /// match and a specific attribute is not defined.
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <typeparam name="TAvro"></typeparam>
        /// <typeparam name="TProperty"></typeparam>
        public static void AddDefaultConverter<TAvro, TProperty>(Func<TAvro, Schema, TProperty> from, Func<TProperty, Schema, TAvro> to)
        {
            _defaultConverters.Add(new FuncFieldConverter<TAvro, TProperty>(from, to));
        }

        /// <summary>
        /// Find a default converter
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="propType"></param>
        /// <returns>The first matching converter - null if there isnt one</returns>
        public IAvroFieldConverter GetDefaultConverter(Avro.Schema.Type tag, Type propType)
        {
            Type avroType;
            switch (tag)
            {
                case Avro.Schema.Type.Null:
                    return null;
                case Avro.Schema.Type.Boolean:
                    avroType = typeof(bool);
                    break;
                case Avro.Schema.Type.Int:
                    avroType = typeof(int);
                    break;
                case Avro.Schema.Type.Long:
                    avroType = typeof(long);
                    break;
                case Avro.Schema.Type.Float:
                    avroType = typeof(float);
                    break;
                case Avro.Schema.Type.Double:
                    avroType = typeof(double);
                    break;
                case Avro.Schema.Type.Bytes:
                    avroType = typeof(byte[]);
                    break;
                case Avro.Schema.Type.String:
                    avroType = typeof(string);
                    break;
                case Avro.Schema.Type.Record:
                    return null;
                case Avro.Schema.Type.Enumeration:
                    return null;
                case Avro.Schema.Type.Array:
                    return null;
                case Avro.Schema.Type.Map:
                    return null;
                case Avro.Schema.Type.Union:
                    return null;
                case Avro.Schema.Type.Fixed:
                    avroType = typeof(byte[]);
                    break;
                case Avro.Schema.Type.Error:
                    return null;
                default:
                    return null;
            }

            foreach (var c in _defaultConverters)
            {
                if (c.GetAvroType() == avroType && c.GetPropertyType() == propType)
                {
                    return c;
                }
            }

            return null;
        }

        /// <summary>
        /// Add an array helper. Array helpers are used for collections that are not generic lists.
        /// </summary>
        /// <param name="name">Name of the helper. Corresponds to metadata "helper" field in the schema.</param>
        /// <param name="helperType">Type of helper. Inherited from ArrayHelper</param>
        public void AddArrayHelper(string name, Type helperType)
        {
            if (!typeof(ArrayHelper).IsAssignableFrom(helperType))
            {
                throw new AvroException($"{helperType.Name} is not an ArrayHelper");
            }

            _nameArrayMap.TryAdd(name, helperType);
        }

        /// <summary>
        /// Find an array helper for an array schema node.
        /// </summary>
        /// <param name="schema">Schema</param>
        /// <param name="enumerable">The array object. If it is null then Add(), Count() and Clear methods will throw exceptions.</param>
        /// <returns></returns>
        public ArrayHelper GetArrayHelper(ArraySchema schema, IEnumerable enumerable)
        {
            Type h;
            // note ArraySchema is unamed and doesnt have a FulllName, use "helper" metadata
            // metadata is json string, strip quotes
            string s = null;
            s = schema.GetHelper();

            if (s != null && _nameArrayMap.TryGetValue(s, out h))
            {
                return (ArrayHelper)Activator.CreateInstance(h, enumerable);
            }

            return (ArrayHelper)Activator.CreateInstance(typeof(ArrayHelper), enumerable);
        }

        /// <summary>
        /// Find a class that matches the schema full name.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public DotnetClass GetClass(RecordSchema schema)
        {
            DotnetClass c;
            if (!_nameClassMap.TryGetValue(schema.Fullname, out c))
            {
               return null;
            }

            return c;
        }

        /// <summary>
        /// Add an entry to the class cache.
        /// </summary>
        /// <param name="objType">Type of the C# class</param>
        /// <param name="s">Schema</param>
        public void LoadClassCache(Type objType, Schema s)
        {
            switch (s)
            {
                case RecordSchema rs:
                    if (!objType.IsClass)
                    {
                        throw new AvroException($"Cant map scalar type {objType.Name} to record {rs.Fullname}");
                    }

                    if (typeof(byte[]).IsAssignableFrom(objType)
                        || typeof(string).IsAssignableFrom(objType)
                        || typeof(IEnumerable).IsAssignableFrom(objType)
                        || typeof(IDictionary).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to record {rs.Fullname}");
                    }

                    AddClassNameMapItem(rs, objType);
                    var c = GetClass(rs);
                    foreach (var f in rs.Fields)
                    {
                        /*              
                        //.StackOverflowException
                        var t = c.GetPropertyType(f);
                        LoadClassCache(t, f.Schema);
                        */
                        if (_previousFields.TryAdd(f.Name, f.Schema))
                        {
                            var t = c.GetPropertyType(f);
                            LoadClassCache(t, f.Schema);
                        }
                    }

                    break;
                case ArraySchema ars:
                    if (!typeof(IEnumerable).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to array {ars.Name}");
                    }

                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"{objType.Name} needs to be a generic type");
                    }

                    LoadClassCache(objType.GenericTypeArguments[0], ars.ItemSchema);
                    break;
                case MapSchema ms:
                    if (!typeof(IDictionary).IsAssignableFrom(objType))
                    {
                        throw new AvroException($"Cant map type {objType.Name} to map {ms.Name}");
                    }

                    if (!objType.IsGenericType)
                    {
                        throw new AvroException($"Cant map non-generic type {objType.Name} to map {ms.Name}");
                    }

                    if (!typeof(string).IsAssignableFrom(objType.GenericTypeArguments[0]))
                    {
                        throw new AvroException($"First type parameter of {objType.Name} must be assignable to string");
                    }

                    LoadClassCache(objType.GenericTypeArguments[1], ms.ValueSchema);
                    break;
                case NamedSchema ns:
                    EnumCache.AddEnumNameMapItem(ns, objType);
                    break;
                case UnionSchema us:
                    if (us.Schemas.Count == 2 && (us.Schemas[0].Tag == Schema.Type.Null || us.Schemas[1].Tag == Schema.Type.Null) && objType.IsClass)
                    {
                        // in this case objType will match the non null type in the union
                        foreach (var o in us.Schemas)
                        {
                            if (o.Tag != Schema.Type.Null)
                            {
                                LoadClassCache(objType, o);
                            }
                        }

                    }
                    else
                    {
                        // check the schema types are registered
                        foreach (var o in us.Schemas)
                        {
                            if (o.Tag == Schema.Type.Record && GetClass(o as RecordSchema) == null)
                            {
                                throw new AvroException($"Class for union record type {o.Fullname} is not registered. Create a ClassCache object and call LoadClassCache");
                            }
                        }
                    }

                    break;
            }
        }
    }
}
