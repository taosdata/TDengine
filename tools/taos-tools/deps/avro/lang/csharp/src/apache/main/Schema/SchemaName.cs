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

namespace Avro
{
    /// <summary>
    /// Class to store schema name, namespace, enclosing namespace and documentation
    /// </summary>
    public class SchemaName
    {
        // cache the full name, so it won't allocate new strings on each call
        private String fullName;
        
        /// <summary>
        /// Name of the schema
        /// </summary>
        public String Name { get; private set; }

        /// <summary>
        /// Namespace specified within the schema
        /// </summary>
        public String Space { get; private set; }

        /// <summary>
        /// Namespace from the most tightly enclosing schema
        /// </summary>
        public String EncSpace { get; private set; }

        /// <summary>
        /// Documentation for the schema
        /// </summary>
        public String Documentation { get; private set; }

        /// <summary>
        /// Namespace.Name of the schema
        /// </summary>
        public String Fullname { get { return fullName; } }

        /// <summary>
        /// Namespace of the schema
        /// </summary>
        public String Namespace { get { return string.IsNullOrEmpty(this.Space) ? this.EncSpace : this.Space; } }

        /// <summary>
        /// Constructor for SchemaName
        /// </summary>
        /// <param name="name">name of the schema</param>
        /// <param name="space">namespace of the schema</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <param name="documentation">documentation o fthe schema</param>
        public SchemaName(String name, String space, String encspace, String documentation)
        {
            if (name == null)
            {                         // anonymous
                this.Name = this.Space = null;
                this.EncSpace = encspace;   // need to save enclosing namespace for anonymous types, so named types within the anonymous type can be resolved
            }
#pragma warning disable CA1307 // Specify StringComparison
            else if (!name.Contains("."))
#pragma warning restore CA1307 // Specify StringComparison
            {                          // unqualified name
                this.Space = space;    // use default space
                this.Name = name;
                this.EncSpace = encspace;
            }
            else
            {
                string[] parts = name.Split('.');
                this.Space = string.Join(".", parts, 0, parts.Length - 1);
                this.Name = parts[parts.Length - 1];
                this.EncSpace = encspace;
            }
            this.Documentation = documentation;
            fullName = string.IsNullOrEmpty(Namespace) ? this.Name : Namespace + "." + this.Name;
        }

        /// <summary>
        /// Returns the full name of the schema
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Fullname;
        }

        /// <summary>
        /// Writes the schema name in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        internal void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            if (null != this.Name)  // write only if not anonymous
            {
                JsonHelper.writeIfNotNullOrEmpty(writer, "name", this.Name);
                JsonHelper.writeIfNotNull(writer, "doc", this.Documentation);
                if (!String.IsNullOrEmpty(this.Space))
                    JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.Space);
                else if (!String.IsNullOrEmpty(this.EncSpace)) // need to put enclosing name space for code generated classes
                    JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.EncSpace);
            }
        }

        /// <summary>
        /// Compares two schema names
        /// </summary>
        /// <param name="obj">SchameName object to compare against this object</param>
        /// <returns>true or false</returns>
        public override bool Equals(Object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is SchemaName)
            {
                SchemaName that = (SchemaName)obj;
                return areEqual(that.Name, Name) && areEqual(that.Namespace, Namespace);
            }
            return false;
        }

        /// <summary>
        /// Compares two objects
        /// </summary>
        /// <param name="obj1">first object</param>
        /// <param name="obj2">second object</param>
        /// <returns>true or false</returns>
        private static bool areEqual(object obj1, object obj2)
        {
            return obj1 == null ? obj2 == null : obj1.Equals(obj2);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
#pragma warning disable CA1307 // Specify StringComparison
            return string.IsNullOrEmpty(Fullname) ? 0 : 29 * Fullname.GetHashCode();
#pragma warning restore CA1307 // Specify StringComparison
        }
    }

    /// <summary>
    /// A class that contains a list of named schemas. This is used when reading or writing a schema/protocol.
    /// This prevents reading and writing of duplicate schema definitions within a protocol or schema file
    /// </summary>
    public class SchemaNames
    {
        /// <summary>
        /// Map of schema name and named schema objects
        /// </summary>
        public IDictionary<SchemaName, NamedSchema> Names { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public SchemaNames()
        {
            Names = new Dictionary<SchemaName, NamedSchema>();
        }

        /// <summary>
        /// Checks if given name is in the map
        /// </summary>
        /// <param name="name">schema name</param>
        /// <returns>true or false</returns>
        public bool Contains(SchemaName name)
        {
            if (Names.ContainsKey(name))
                return true;
            return false;
        }

        /// <summary>
        /// Adds a schema name to the map if it doesn't exist yet
        /// </summary>
        /// <param name="name">schema name</param>
        /// <param name="schema">schema object</param>
        /// <returns>true if schema was added to the list, false if schema is already in the list</returns>
        public bool Add(SchemaName name, NamedSchema schema)
        {
            if (Names.ContainsKey(name))
                return false;

            Names.Add(name, schema);
            return true;
        }

        /// <summary>
        /// Adds a named schema to the list
        /// </summary>
        /// <param name="schema">schema object</param>
        /// <returns>true if schema was added to the list, false if schema is already in the list</returns>
        public bool Add(NamedSchema schema)
        {
            SchemaName name = schema.SchemaName;
            return Add(name, schema);
        }

        /// <summary>
        /// Tries to get the value for the given name fields
        /// </summary>
        /// <param name="name">name of the schema</param>
        /// <param name="space">namespace of the schema</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        /// <param name="documentation">documentation for the schema</param>
        /// <param name="schema">schema object found</param>
        /// <returns>true if name is found in the map, false otherwise</returns>
        public bool TryGetValue(string name, string space, string encspace, string documentation, out NamedSchema schema)
        {
            SchemaName schemaname = new SchemaName(name, space, encspace, documentation);
            return Names.TryGetValue(schemaname, out schema);
        }

        /// <summary>
        /// Returns the enumerator for the map
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<SchemaName, NamedSchema>> GetEnumerator()
        {
            return Names.GetEnumerator();
        }
    }
}
