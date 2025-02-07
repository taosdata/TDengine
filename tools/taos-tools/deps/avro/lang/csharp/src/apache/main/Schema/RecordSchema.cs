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
using Newtonsoft.Json.Linq;

namespace Avro
{
    internal delegate T Function<T>();

    /// <summary>
    /// Class for record schemas
    /// </summary>
    public class RecordSchema : NamedSchema
    {
        /// <summary>
        /// List of fields in the record
        /// </summary>
        public List<Field> Fields { get; private set; }

        /// <summary>
        /// Number of fields in the record
        /// </summary>
        public int Count { get { return Fields.Count; } }

        /// <summary>
        /// Map of field name and Field object for faster field lookups
        /// </summary>
        private readonly IDictionary<string, Field> fieldLookup;

        private readonly IDictionary<string, Field> fieldAliasLookup;
        private bool request;

        /// <summary>
        /// Static function to return new instance of the record schema
        /// </summary>
        /// <param name="type">type of record schema, either record or error</param>
        /// <param name="jtok">JSON object for the record schema</param>
        /// <param name="props">dictionary that provides access to custom properties</param>
        /// <param name="names">list of named schema already read</param>
        /// <param name="encspace">enclosing namespace of the records schema</param>
        /// <returns>new RecordSchema object</returns>
        internal static RecordSchema NewInstance(Type type, JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            bool request = false;
            JToken jfields = jtok["fields"];    // normal record
            if (null == jfields)
            {
                jfields = jtok["request"];      // anonymous record from messages
                if (null != jfields) request = true;
            }
            if (null == jfields)
                throw new SchemaParseException($"'fields' cannot be null for record at '{jtok.Path}'");
            if (jfields.Type != JTokenType.Array)
                throw new SchemaParseException($"'fields' not an array for record at '{jtok.Path}'");

            var name = GetName(jtok, encspace);
            var aliases = NamedSchema.GetAliases(jtok, name.Space, name.EncSpace);
            var fields = new List<Field>();
            var fieldMap = new Dictionary<string, Field>();
            var fieldAliasMap = new Dictionary<string, Field>();
            RecordSchema result;
            try
            {
                result = new RecordSchema(type, name, aliases, props, fields, request, fieldMap, fieldAliasMap, names,
                    JsonHelper.GetOptionalString(jtok, "doc"));
            }
            catch (SchemaParseException e)
            {
                throw new SchemaParseException($"{e.Message} at '{jtok.Path}'", e);
            }

            int fieldPos = 0;
            foreach (JObject jfield in jfields)
            {
                string fieldName = JsonHelper.GetRequiredString(jfield, "name");
                Field field = createField(jfield, fieldPos++, names, name.Namespace);  // add record namespace for field look up
                fields.Add(field);
                try
                {
                    addToFieldMap(fieldMap, fieldName, field);
                    addToFieldMap(fieldAliasMap, fieldName, field);

                    if (null != field.Aliases)    // add aliases to field lookup map so reader function will find it when writer field name appears only as an alias on the reader field
                        foreach (string alias in field.Aliases)
                            addToFieldMap(fieldAliasMap, alias, field);
                }
                catch (SchemaParseException e)
                {
                    throw new SchemaParseException($"{e.Message} at '{jfield.Path}'", e);
                }
            }
            return result;
        }

        /// <summary>
        /// Constructor for the record schema
        /// </summary>
        /// <param name="type">type of record schema, either record or error</param>
        /// <param name="name">name of the record schema</param>
        /// <param name="aliases">list of aliases for the record name</param>
        /// <param name="props">custom properties on this schema</param>
        /// <param name="fields">list of fields for the record</param>
        /// <param name="request">true if this is an anonymous record with 'request' instead of 'fields'</param>
        /// <param name="fieldMap">map of field names and field objects</param>
        /// <param name="fieldAliasMap">map of field aliases and field objects</param>
        /// <param name="names">list of named schema already read</param>
        /// <param name="doc">documentation for this named schema</param>
        private RecordSchema(Type type, SchemaName name, IList<SchemaName> aliases,  PropertyMap props,
                                List<Field> fields, bool request, IDictionary<string, Field> fieldMap,
                                IDictionary<string, Field> fieldAliasMap, SchemaNames names, string doc)
                                : base(type, name, aliases, props, names, doc)
        {
            if (!request && null == name.Name) throw new SchemaParseException("name cannot be null for record schema.");
            this.Fields = fields;
            this.request = request;
            this.fieldLookup = fieldMap;
            this.fieldAliasLookup = fieldAliasMap;
        }

        /// <summary>
        /// Creates a new field for the record
        /// </summary>
        /// <param name="jfield">JSON object for the field</param>
        /// <param name="pos">position number of the field</param>
        /// <param name="names">list of named schemas already read</param>
        /// <param name="encspace">enclosing namespace of the records schema</param>
        /// <returns>new Field object</returns>
        private static Field createField(JToken jfield, int pos, SchemaNames names, string encspace)
        {
            var name = JsonHelper.GetRequiredString(jfield, "name");
            var doc = JsonHelper.GetOptionalString(jfield, "doc");

            var jorder = JsonHelper.GetOptionalString(jfield, "order");
            Field.SortOrder sortorder = Field.SortOrder.ignore;
            if (null != jorder)
                sortorder = (Field.SortOrder) Enum.Parse(typeof(Field.SortOrder), jorder);

            var aliases = Field.GetAliases(jfield);
            var props = Schema.GetProperties(jfield);
            var defaultValue = jfield["default"];

            JToken jtype = jfield["type"];
            if (null == jtype)
                throw new SchemaParseException($"'type' was not found for field: name at '{jfield.Path}'");
            var schema = Schema.ParseJson(jtype, names, encspace);
            return new Field(schema, name, aliases, pos, doc, defaultValue, sortorder, props);
        }

        private static void addToFieldMap(Dictionary<string, Field> map, string name, Field field)
        {
            if (map.ContainsKey(name))
                throw new SchemaParseException("field or alias " + name + " is a duplicate name");
            map.Add(name, field);
        }

        /// <summary>
        /// Returns the field with the given name.
        /// </summary>
        /// <param name="name">field name</param>
        /// <returns>Field object</returns>
        public Field this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
                Field field;
                return fieldLookup.TryGetValue(name, out field) ? field : null;
            }
        }

        /// <summary>
        /// Returns true if and only if the record contains a field by the given name.
        /// </summary>
        /// <param name="fieldName">The name of the field</param>
        /// <returns>true if the field exists, false otherwise</returns>
        public bool Contains(string fieldName)
        {
            return fieldLookup.ContainsKey(fieldName);
        }

        /// <summary>
        /// Gets a field with a specified name.
        /// </summary>
        /// <param name="fieldName">Name of the field to get.</param>
        /// <param name="field">
        /// When this method returns true, contains the field with the specified name. When this
        /// method returns false, null.
        /// </param>
        /// <returns>True if a field with the specified name exists; false otherwise.</returns>
        public bool TryGetField(string fieldName, out Field field)
        {
            return fieldLookup.TryGetValue(fieldName, out field);
        }

        /// <summary>
        /// Gets a field with a specified alias.
        /// </summary>
        /// <param name="fieldName">Alias of the field to get.</param>
        /// <param name="field">
        /// When this method returns true, contains the field with the specified alias. When this
        /// method returns false, null.
        /// </param>
        /// <returns>True if a field with the specified alias exists; false otherwise.</returns>
        public bool TryGetFieldAlias(string fieldName, out Field field)
        {
            return fieldAliasLookup.TryGetValue(fieldName, out field);
        }

        /// <summary>
        /// Returns an enumerator which enumerates over the fields of this record schema
        /// </summary>
        /// <returns>Enumerator over the field in the order of their definition</returns>
        public IEnumerator<Field> GetEnumerator()
        {
            return Fields.GetEnumerator();
        }

        /// <summary>
        /// Writes the records schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the record schema</param>
        protected internal override void WriteJsonFields(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            base.WriteJsonFields(writer, names, encspace);

            // we allow reading for empty fields, so writing of records with empty fields are allowed as well
            if (request)
                writer.WritePropertyName("request");
            else
                writer.WritePropertyName("fields");
            writer.WriteStartArray();

            if (null != this.Fields && this.Fields.Count > 0)
            {
                foreach (Field field in this)
                    field.writeJson(writer, names, this.Namespace); // use the namespace of the record for the fields
            }
            writer.WriteEndArray();
        }

        /// <summary>
        /// Compares equality of two record schemas
        /// </summary>
        /// <param name="obj">record schema to compare against this schema</param>
        /// <returns>true if the two schemas are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (obj != null && obj is RecordSchema)
            {
                RecordSchema that = obj as RecordSchema;
                return protect(() => true, () =>
                {
                    if (this.SchemaName.Equals(that.SchemaName) && this.Count == that.Count)
                    {
                        for (int i = 0; i < Fields.Count; i++) if (!Fields[i].Equals(that.Fields[i])) return false;
                        return areEqual(that.Props, this.Props);
                    }
                    return false;
                }, that);
            }
            return false;
        }

        /// <summary>
        /// Hash code function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return protect(() => 0, () =>
            {
                int result = SchemaName.GetHashCode();
                foreach (Field f in Fields) result += 29 * f.GetHashCode();
                result += getHashCode(Props);
                return result;
            }, this);
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if ((writerSchema.Tag != Type.Record) && (writerSchema.Tag != Type.Error)) return false;

            RecordSchema that = writerSchema as RecordSchema;
            return protect(() => true, () =>
            {
                if (!that.SchemaName.Equals(SchemaName))
                    if (!InAliases(that.SchemaName))
                        return false;

                foreach (Field f in this)
                {
                    Field f2 = that[f.Name];
                    if (null == f2) // reader field not in writer field, check aliases of reader field if any match with a writer field
                        if (null != f.Aliases)
                            foreach (string alias in f.Aliases)
                            {
                                f2 = that[alias];
                                if (null != f2) break;
                            }

                    if (f2 == null && f.DefaultValue != null)
                        continue;         // Writer field missing, reader has default.

                    if (f2 != null && f.Schema.CanRead(f2.Schema)) continue;    // Both fields exist and are compatible.
                    return false;
                }
                return true;
            }, that);
        }

        private class RecordSchemaPair
        {
            public readonly RecordSchema first;
            public readonly RecordSchema second;

            public RecordSchemaPair(RecordSchema first, RecordSchema second)
            {
                this.first = first;
                this.second = second;
            }
        }

        [ThreadStatic]
        private static List<RecordSchemaPair> seen;

        /**
         * We want to protect against infinite recursion when the schema is recursive. We look into a thread local
         * to see if we have been into this if so, we execute the bypass function otherwise we execute the main function.
         * Before executing the main function, we ensure that we create a marker so that if we come back here recursively
         * we can detect it.
         *
         * The infinite loop happens in ToString(), Equals() and GetHashCode() methods.
         * Though it does not happen for CanRead() because of the current implemenation of UnionSchema's can read,
         * it could potenitally happen.
         * We do a linear seach for the marker as we don't expect the list to be very long.
         */
        private T protect<T>(Function<T> bypass, Function<T> main, RecordSchema that)
        {
            if (seen == null)
                seen = new List<RecordSchemaPair>();

            else if (seen.Find((RecordSchemaPair rs) => rs.first == this && rs.second == that) != null)
                return bypass();

            RecordSchemaPair p = new RecordSchemaPair(this, that);
            seen.Add(p);
            try { return main(); }
            finally { seen.Remove(p); }
        }

    }
}
