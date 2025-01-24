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
using Newtonsoft.Json;

namespace Avro
{
    /// <summary>
    /// Class for schemas of primitive types
    /// </summary>
    public sealed class PrimitiveSchema : UnnamedSchema
    {
        /// <summary>
        /// Constructor for primitive schema
        /// </summary>
        /// <param name="type"></param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        private PrimitiveSchema(Type type, PropertyMap props) : base(type, props)
        {
        }

        /// <summary>
        /// Static function to return new instance of primitive schema
        /// </summary>
        /// <param name="type">primitive type</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <returns></returns>
        public static PrimitiveSchema NewInstance(string type, PropertyMap props = null)
        {
            const string q = "\"";
            if (type.StartsWith(q, StringComparison.Ordinal)
                && type.EndsWith(q, StringComparison.Ordinal))
            {
                type = type.Substring(1, type.Length - 2);
            }

            switch (type)
            {
                case "null":
                    return new PrimitiveSchema(Schema.Type.Null, props);
                case "boolean":
                    return new PrimitiveSchema(Schema.Type.Boolean, props);
                case "int":
                    return new PrimitiveSchema(Schema.Type.Int, props);
                case "long":
                    return new PrimitiveSchema(Schema.Type.Long, props);
                case "float":
                    return new PrimitiveSchema(Schema.Type.Float, props);
                case "double":
                    return new PrimitiveSchema(Schema.Type.Double, props);
                case "bytes":
                    return new PrimitiveSchema(Schema.Type.Bytes, props);
                case "string":
                    return new PrimitiveSchema(Schema.Type.String, props);
                default:
                    return null;
            }
        }

        /// <summary>
        /// Writes primitive schema in JSON format
        /// </summary>
        /// <param name="w"></param>
        /// <param name="names"></param>
        /// <param name="encspace"></param>
        protected internal override void WriteJson(JsonTextWriter w, SchemaNames names, string encspace)
        {
            w.WriteValue(Name);
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema is UnionSchema || Tag == writerSchema.Tag) return true;
            Type t = writerSchema.Tag;
            switch (Tag)
            {
                case Type.Double:
                    return t == Type.Int || t == Type.Long || t == Type.Float;
                case Type.Float:
                    return t == Type.Int || t == Type.Long;
                case Type.Long:
                    return t == Type.Int;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Function to compare equality of two primitive schemas
        /// </summary>
        /// <param name="obj">other primitive schema</param>
        /// <returns>true two schemas are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (this == obj) return true;

            if (obj != null && obj is PrimitiveSchema)
            {
                var that = obj as PrimitiveSchema;
                if (this.Tag == that.Tag)
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
            return 13 * Tag.GetHashCode() + getHashCode(Props);
        }

        /// <summary>
        /// Returns the canonical JSON representation of this schema.
        /// </summary>
        /// <returns>The canonical JSON representation of this schema.</returns>
        public override string ToString()
        {
            using (System.IO.StringWriter sw = new System.IO.StringWriter())
            using (Newtonsoft.Json.JsonTextWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("type");

                WriteJson(writer, new SchemaNames(), null); // stand alone schema, so no enclosing name space

                writer.WriteEndObject();

                return sw.ToString();
            }
        }
    }
}
