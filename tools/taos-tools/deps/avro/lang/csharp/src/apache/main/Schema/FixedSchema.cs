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
    /// Class for fixed schemas
    /// </summary>
    public class FixedSchema : NamedSchema
    {
        /// <summary>
        /// Fixed size for the bytes
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Static function to return new instance of the fixed schema class
        /// </summary>
        /// <param name="jtok">JSON object for the fixed schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schema already parsed in</param>
        /// <param name="encspace">enclosing namespace of the fixed schema</param>
        /// <returns></returns>
        internal static FixedSchema NewInstance(JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            SchemaName name = NamedSchema.GetName(jtok, encspace);
            var aliases = NamedSchema.GetAliases(jtok, name.Space, name.EncSpace);
            try
            {
                return new FixedSchema(name, aliases, JsonHelper.GetRequiredInteger(jtok, "size"), props, names,
                    JsonHelper.GetOptionalString(jtok, "doc"));
            }
            catch (Exception e)
            {
                throw new SchemaParseException($"{e.Message} at '{jtok.Path}'", e);
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="name">name of the fixed schema</param>
        /// <param name="aliases">list of aliases for the name</param>
        /// <param name="size">fixed size</param>
        /// <param name="props">custom properties on this schema</param>
        /// <param name="names">list of named schema already parsed in</param>
        /// <param name="doc">documentation for this named schema</param>
        private FixedSchema(SchemaName name, IList<SchemaName> aliases, int size, PropertyMap props, SchemaNames names,
            string doc)
                            : base(Type.Fixed, name, aliases, props, names, doc)
        {
            if (null == name.Name) throw new SchemaParseException("name cannot be null for fixed schema.");
            if (size <= 0) throw new ArgumentOutOfRangeException(nameof(size), "size must be greater than zero.");
            this.Size = size;
        }

        /// <summary>
        /// Writes the fixed schema class in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schema already written</param>
        /// <param name="encspace">enclosing namespace for the fixed schema</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            base.WriteJsonFields(writer, names, encspace);
            writer.WritePropertyName("size");
            writer.WriteValue(this.Size);
        }

        /// <summary>
        /// Compares two fixed schemas
        /// </summary>
        /// <param name="obj">fixed schema to compare against this schema</param>
        /// <returns>true if two schemas are the same, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;

            if (obj != null && obj is FixedSchema)
            {
                FixedSchema that = obj as FixedSchema;
                return SchemaName.Equals(that.SchemaName) && Size == that.Size && areEqual(that.Props, this.Props);
            }
            return false;
        }

        /// <summary>
        /// Hash code function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return 53 * SchemaName.GetHashCode() + 47 * Size + getHashCode(Props);
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema.Tag != Tag) return false;
            FixedSchema that = writerSchema as FixedSchema;
            if (that.Size != Size) return false;
            if (that.SchemaName.Equals(SchemaName))
                return true;
            else
                return InAliases(that.SchemaName);
        }
    }
}
