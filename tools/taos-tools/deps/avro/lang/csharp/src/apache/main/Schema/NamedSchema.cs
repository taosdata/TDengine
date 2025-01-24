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
    /// Base class for all named schemas: fixed, enum, record
    /// </summary>
    public abstract class NamedSchema : Schema
    {
        /// <summary>
        /// Name of the schema, contains name, namespace and enclosing namespace
        /// </summary>
        public SchemaName SchemaName { get; private set; }

        /// <summary>
        /// Name of the schema
        /// </summary>
        public override string Name
        {
            get { return SchemaName.Name; }
        }

        /// <summary>
        /// Namespace of the schema
        /// </summary>
        public string Namespace
        {
            get { return SchemaName.Namespace; }
        }

        /// <summary>
        /// Namespace.Name of the schema
        /// </summary>
        public override string Fullname
        {
            get { return SchemaName.Fullname; }
        }

        /// <summary>
        /// Documentation for the schema, if any. Null if there is no documentation.
        /// </summary>
        public string Documentation { get; private set; }

        /// <summary>
        /// List of aliases for this named schema
        /// </summary>
        private readonly IList<SchemaName> aliases;

        /// <summary>
        /// Static function to return a new instance of the named schema
        /// </summary>
        /// <param name="jo">JSON object of the named schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the named schema</param>
        /// <returns></returns>
        internal static NamedSchema NewInstance(JObject jo, PropertyMap props, SchemaNames names, string encspace)
        {
            string type = JsonHelper.GetRequiredString(jo, "type");
            string doc = JsonHelper.GetOptionalString(jo, "doc");
            switch (type)
            {
                case "fixed":
                    return FixedSchema.NewInstance(jo, props, names, encspace);
                case "enum":
                    return EnumSchema.NewInstance(jo, props, names, encspace);
                case "record":
                    return RecordSchema.NewInstance(Type.Record, jo, props, names, encspace);
                case "error":
                    return RecordSchema.NewInstance(Type.Error, jo, props, names, encspace);
                default:
                    NamedSchema result;
                    if (names.TryGetValue(type, null, encspace, doc, out result))
                        return result;
                    return null;
            }
        }

        /// <summary>
        /// Constructor for named schema class
        /// </summary>
        /// <param name="type">schema type</param>
        /// <param name="name">name</param>
        /// <param name="aliases">aliases for this named schema</param>
        /// <param name="props">custom properties on this schema</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="doc">documentation for this named schema</param>
        protected NamedSchema(Type type, SchemaName name, IList<SchemaName> aliases, PropertyMap props, SchemaNames names,
            string doc)
                                : base(type, props)
        {
            this.SchemaName = name;
            this.Documentation = doc;
            this.aliases = aliases;
            if (null != name.Name)  // Added this check for anonymous records inside Message
                if (!names.Add(name, this))
                    throw new AvroException("Duplicate schema name " + name.Fullname);
        }

        /// <summary>
        /// Parses the name and namespace from the given JSON schema object then creates
        /// SchemaName object including the given enclosing namespace
        /// </summary>
        /// <param name="jtok">JSON object to read</param>
        /// <param name="encspace">enclosing namespace</param>
        /// <returns>new SchemaName object</returns>
        protected static SchemaName GetName(JToken jtok, string encspace)
        {
            String n = JsonHelper.GetOptionalString(jtok, "name");      // Changed this to optional string for anonymous records in messages
            String ns = JsonHelper.GetOptionalString(jtok, "namespace");
            String d = JsonHelper.GetOptionalString(jtok, "doc");
            return new SchemaName(n, ns, encspace, d);
        }

        /// <summary>
        /// Parses the 'aliases' property from the given JSON token
        /// </summary>
        /// <param name="jtok">JSON object to read</param>
        /// <param name="space">namespace of the name this alias is for</param>
        /// <param name="encspace">enclosing namespace of the name this alias is for</param>        
        /// <returns>List of SchemaName that represents the list of alias. If no 'aliases' specified, then it returns null.</returns>
        protected static IList<SchemaName> GetAliases(JToken jtok, string space, string encspace)
        {
            JToken jaliases = jtok["aliases"];
            if (null == jaliases)
                return null;

            if (jaliases.Type != JTokenType.Array)
                throw new SchemaParseException($"Aliases must be of format JSON array of strings at '{jtok.Path}'");

            var aliases = new List<SchemaName>();
            foreach (JToken jalias in jaliases)
            {
                if (jalias.Type != JTokenType.String)
                    throw new SchemaParseException($"Aliases must be of format JSON array of strings at '{jtok.Path}'");

                aliases.Add(new SchemaName((string)jalias, space, encspace, null));
            }
            return aliases;
        }

        /// <summary>
        /// Determines whether the given schema name is one of this <see cref="NamedSchema"/>'s
        /// aliases.
        /// </summary>
        /// <param name="name">Schema name to test.</param>
        /// <returns>
        /// True if <paramref name="name"/> is one of this schema's aliases; false otherwise.
        /// </returns>
        protected bool InAliases(SchemaName name)
        {
            if (null != aliases)
            {
                foreach (SchemaName alias in aliases)
                    if (name.Equals(alias)) return true;
            }
            return false;
        }

        /// <summary>
        /// Writes named schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the named schema</param>
        protected internal override void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            if (!names.Add(this))
            {
                // schema is already in the list, write name only
                SchemaName schemaName = this.SchemaName;
                string name;
                if (schemaName.Namespace != encspace)
                    name = schemaName.Namespace + "." + schemaName.Name;  // we need to add the qualifying namespace of the target schema if it's not the same as current namespace
                else
                    name = schemaName.Name;
                writer.WriteValue(name);
            }
            else
                // schema is not in the list, write full schema definition
                base.WriteJson(writer, names, encspace);
        }

        /// <summary>
        /// Writes named schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the named schema</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            this.SchemaName.WriteJson(writer, names, encspace);

            if (null != aliases)
            {
                writer.WritePropertyName("aliases");
                writer.WriteStartArray();
                foreach (SchemaName name in aliases)
                {
                    string fullname = (null != name.Space) ? name.Space + "." + name.Name : name.Name;
                    writer.WriteValue(fullname);
                }
                writer.WriteEndArray();
            }
        }
    }
}
