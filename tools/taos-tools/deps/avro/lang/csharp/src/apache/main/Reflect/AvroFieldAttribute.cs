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

namespace Avro.Reflect
{
    /// <summary>
    /// Attribute that specifies the mapping between an Avro field and C# class property.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class AvroFieldAttribute : Attribute
    {
        /// <summary>
        /// Name of the field in the Avro Schema
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        /// Convert the property into a standard Avro type - e.g. DateTimeOffset to long
        /// </summary>
        public IAvroFieldConverter Converter { get; set; }

        /// <summary>
        /// Attribute to hold a field name and optionally a converter
        /// </summary>
        /// <param name="fieldName"></param>
        /// <param name="converter"></param>
        public AvroFieldAttribute(string fieldName, Type converter = null)
        {
            FieldName = fieldName;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }

        /// <summary>
        /// Used in property name mapping to specify a property type converter for the attribute.
        /// </summary>
        /// <param name="converter"></param>
        public AvroFieldAttribute(Type converter)
        {
            FieldName = null;
            if (converter != null)
            {
                Converter = (IAvroFieldConverter)Activator.CreateInstance(converter);
            }
        }
    }
}
