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
using System.Text;
using Newtonsoft.Json.Linq;

namespace Avro
{
    /// <summary>
    /// Class for map schemas
    /// </summary>
    public class MapSchema : UnnamedSchema
    {
        /// <summary>
        /// Schema for map values type
        /// </summary>
        public Schema ValueSchema { get; set; }

        /// <summary>
        /// Creates a new <see cref="MapSchema"/> from the given schema.
        /// </summary>
        /// <param name="type">Schema to create the map schema from.</param>
        /// <returns>A new <see cref="MapSchema"/>.</returns>
        public static MapSchema CreateMap(Schema type)
        {
            return new MapSchema(type,null);
        }

        /// <summary>
        /// Static function to return new instance of map schema
        /// </summary>
        /// <param name="jtok">JSON object for the map schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the map schema</param>
        /// <returns></returns>
        internal static MapSchema NewInstance(JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            JToken jvalue = jtok["values"];
            if (null == jvalue) throw new AvroTypeException($"Map does not have 'values' at '{jtok.Path}'");
            try
            {
                return new MapSchema(ParseJson(jvalue, names, encspace), props);
            }
            catch (Exception e)
            {
                throw new SchemaParseException($"Error creating MapSchema at '{jtok.Path}'", e);
            }
        }

        /// <summary>
        /// Constructor for map schema class
        /// </summary>
        /// <param name="valueSchema">schema for map values type</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        private MapSchema(Schema valueSchema, PropertyMap props) : base(Type.Map, props)
        {
            if (null == valueSchema) throw new ArgumentNullException(nameof(valueSchema), "valueSchema cannot be null.");
            this.ValueSchema = valueSchema;
        }

        /// <summary>
        /// Writes map schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the map schema</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WritePropertyName("values");
            ValueSchema.WriteJson(writer, names, encspace);
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema.Tag != Tag) return false;

            MapSchema that = writerSchema as MapSchema;
            return ValueSchema.CanRead(that.ValueSchema);
        }

        /// <summary>
        /// Compares equality of two map schemas
        /// </summary>
        /// <param name="obj">map schema to compare against this schema</param>
        /// <returns>true if two schemas are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (this == obj) return true;

            if (obj != null && obj is MapSchema)
            {
                MapSchema that = obj as MapSchema;
                if (ValueSchema.Equals(that.ValueSchema))
                    return areEqual(that.Props, this.Props);
            }
            return false;
        }

        /// <summary>
        /// Hashcode function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return 29 * ValueSchema.GetHashCode() + getHashCode(Props);
        }
    }
}
