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
using Newtonsoft.Json;

namespace Avro
{
    /// <summary>
    /// Class for union schemas
    /// </summary>
    public class UnionSchema : UnnamedSchema
    {
        /// <summary>
        /// List of schemas in the union
        /// </summary>
        public IList<Schema> Schemas { get; private set; }

        /// <summary>
        /// Count of schemas in the union
        /// </summary>
        public int Count { get { return Schemas.Count; } }

        /// <summary>
        /// Static function to return instance of the union schema
        /// </summary>
        /// <param name="jarr">JSON object for the union schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <returns>new UnionSchema object</returns>
        internal static UnionSchema NewInstance(JArray jarr, PropertyMap props, SchemaNames names, string encspace)
        {
            List<Schema> schemas = new List<Schema>();
            IDictionary<string, string> uniqueSchemas = new Dictionary<string, string>();

            foreach (JToken jvalue in jarr)
            {
                Schema unionType = Schema.ParseJson(jvalue, names, encspace);
                if (null == unionType)
                    throw new SchemaParseException($"Invalid JSON in union {jvalue.ToString()} at '{jvalue.Path}'");

                string name = unionType.Fullname;
                if (uniqueSchemas.ContainsKey(name))
                    throw new SchemaParseException($"Duplicate type in union: {name} at '{jvalue.Path}'");

                uniqueSchemas.Add(name, name);
                schemas.Add(unionType);
            }
            return new UnionSchema(schemas, props);
        }

        /// <summary>
        /// Contructor for union schema
        /// </summary>
        /// <param name="schemas"></param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        private UnionSchema(List<Schema> schemas, PropertyMap props) : base(Type.Union, props)
        {
            if (schemas == null)
                throw new ArgumentNullException(nameof(schemas));
            this.Schemas = schemas;
        }

        /// <summary>
        /// Returns the schema at the given branch.
        /// </summary>
        /// <param name="index">Index to the branch, starting with 0.</param>
        /// <returns>The branch corresponding to the given index.</returns>
        public Schema this[int index]
        {
            get
            {
                return Schemas[index];
            }
        }

        /// <summary>
        /// Writes union schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        protected internal override void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WriteStartArray();
            foreach (Schema schema in this.Schemas)
                schema.WriteJson(writer, names, encspace);
            writer.WriteEndArray();
        }

        /// <summary>
        /// Returns the index of a branch that can read the data written by the given schema s.
        /// </summary>
        /// <param name="s">The schema to match the branches against.</param>
        /// <returns>The index of the matching branch. If non matches a -1 is returned.</returns>
        public int MatchingBranch(Schema s)
        {
            if (s is UnionSchema) throw new AvroException("Cannot find a match against union schema");
            // Try exact match.
            //for (int i = 0; i < Count; i++) if (Schemas[i].Equals(s)) return i; // removed this for performance's sake
            for (int i = 0; i < Count; i++) if (Schemas[i].CanRead(s)) return i;
            return -1;
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            return writerSchema.Tag == Schema.Type.Union || MatchingBranch(writerSchema) >= 0;
        }

        /// <summary>
        /// Compares two union schema objects
        /// </summary>
        /// <param name="obj">union schema object to compare against this schema</param>
        /// <returns>true if objects are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is UnionSchema)
            {
                UnionSchema that = obj as UnionSchema;
                if (that.Count == Count)
                {
                    for (int i = 0; i < Count; i++) if (!that[i].Equals(this[i])) return false;
                    return areEqual(that.Props, this.Props);
                }
            }
            return false;
        }

        /// <summary>
        /// Hash code function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int result = 53;
            foreach (Schema schema in Schemas) result += 89 * schema.GetHashCode();
            result += getHashCode(Props);
            return result;
        }
    }
}
