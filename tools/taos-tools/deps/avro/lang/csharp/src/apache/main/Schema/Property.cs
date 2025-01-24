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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Avro
{
    /// <summary>
    /// Provides access to custom properties (those not defined in the Avro spec) in a JSON object.
    /// </summary>
    public class PropertyMap : Dictionary<string, string>
    {
        /// <summary>
        /// Set of reserved schema property names, any other properties not defined in this set are custom properties and can be added to this map
        /// </summary>
        private static readonly HashSet<string> ReservedProps = new HashSet<string>() { "type", "name", "namespace", "fields", "items", "size", "symbols", "values", "aliases", "order", "doc", "default", "logicalType" };

        /// <summary>
        /// Parses the custom properties from the given JSON object and stores them
        /// into the schema's list of custom properties
        /// </summary>
        /// <param name="jtok">JSON object to prase</param>
        public void Parse(JToken jtok)
        {
            JObject jo = jtok as JObject;
            foreach (JProperty prop in jo.Properties())
            {
                if (ReservedProps.Contains(prop.Name))
                    continue;
                if (!ContainsKey(prop.Name))
                    Add(prop.Name, JsonConvert.SerializeObject(prop.Value));
            }
        }

        /// <summary>
        /// Adds a custom property to the schema
        /// </summary>
        /// <param name="key">custom property name</param>
        /// <param name="value">custom property value</param>
        public void Set(string key, string value)
        {
            if (ReservedProps.Contains(key))
                throw new AvroException("Can't set reserved property: " + key);

            string oldValue;
            if (TryGetValue(key, out oldValue))
            {
                if (!oldValue.Equals(value, StringComparison.Ordinal))
                {
                    throw new AvroException("Property cannot be overwritten: " + key);
                }
            }
            else
                Add(key, value);
        }

        /// <summary>
        /// Writes the schema's custom properties in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        public void WriteJson(JsonTextWriter writer)
        {
            foreach (KeyValuePair<string, string> kp in this)
            {
                if (ReservedProps.Contains(kp.Key)) continue;

                writer.WritePropertyName(kp.Key);
                writer.WriteRawValue(kp.Value);
            }
        }

        /// <summary>
        /// Function to compare equality of two PropertyMaps
        /// </summary>
        /// <param name="obj">other PropertyMap</param>
        /// <returns>true if contents of the two maps are the same, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (this == obj) return true;

            if (obj != null && obj is PropertyMap)
            {
                var that = obj as PropertyMap;
                if (this.Count != that.Count)
                    return false;
                foreach (KeyValuePair<string, string> pair in this)
                {
                    if (!that.ContainsKey(pair.Key))
                        return false;
                    if (!pair.Value.Equals(that[pair.Key], StringComparison.Ordinal))
                        return false;
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Hashcode function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int hash = this.Count;
            int index = 1;
            foreach (KeyValuePair<string, string> pair in this)
#pragma warning disable CA1307 // Specify StringComparison
                hash += (pair.Key.GetHashCode() + pair.Value.GetHashCode()) * index++;
#pragma warning restore CA1307 // Specify StringComparison
            return hash;
        }
    }
}
