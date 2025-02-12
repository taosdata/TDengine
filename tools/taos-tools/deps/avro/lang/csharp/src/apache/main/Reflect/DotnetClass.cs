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
using System.Reflection;
using System.Collections.Concurrent;
using Avro;

namespace Avro.Reflect
{
    /// <summary>
    /// Collection of DotNetProperty objects to repre
    /// </summary>
    public class DotnetClass
    {
        private ConcurrentDictionary<string, DotnetProperty> _propertyMap = new ConcurrentDictionary<string, DotnetProperty>();

        private Type _type;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="t">type of the class</param>
        /// <param name="r">record schema</param>
        /// <param name="cache">class cache - can be reused</param>
        public DotnetClass(Type t, RecordSchema r, ClassCache cache)
        {
            _type = t;
            foreach (var f in r.Fields)
            {
                bool hasAttribute = false;
                PropertyInfo prop = GetPropertyInfo(f);

                foreach (var attr in prop.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroFieldAttribute;
                    if (avroAttr != null)
                    {
                        hasAttribute = true;
                        _propertyMap.TryAdd(f.Name, new DotnetProperty(prop, f.Schema.Tag, avroAttr.Converter, cache));
                        break;
                    }
                }

                if (!hasAttribute)
                {
                    _propertyMap.TryAdd(f.Name, new DotnetProperty(prop, f.Schema.Tag, cache));
                }
            }
        }

        private PropertyInfo GetPropertyInfo(Field f)
        {
            var prop = _type.GetProperty(f.Name);
            if (prop != null)
            {
                return prop;
            }
            foreach (var p in _type.GetProperties())
            {
                foreach (var attr in p.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroFieldAttribute;
                    if (avroAttr != null && avroAttr.FieldName != null && avroAttr.FieldName == f.Name)
                    {
                        return p;
                    }
                }
            }

            throw new AvroException($"Class {_type.Name} doesnt contain property {f.Name}");
        }

        /// <summary>
        /// Return the value of a property from an object referenced by a field
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="f">FieldSchema used to look up the property</param>
        /// <returns></returns>
        public object GetValue(object o, Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }

            return p.GetValue(o, f.Schema);
        }

        /// <summary>
        /// Set the value of a property in a C# object
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="f">field schema</param>
        /// <param name="v">value for the proprty referenced by the field schema</param>
        public void SetValue(object o, Field f, object v)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }

            p.SetValue(o, v, f.Schema);
        }

        /// <summary>
        /// Return the type of the Class
        /// </summary>
        /// <returns>The </returns>
        public Type GetClassType()
        {
            return _type;
        }

        /// <summary>
        /// Return the type of a property referenced by a field
        /// </summary>
        /// <param name="f"></param>
        /// <returns></returns>
        public Type GetPropertyType(Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Name, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property {f.Name}");
            }

            return p.GetPropertyType();
        }
    }
}
