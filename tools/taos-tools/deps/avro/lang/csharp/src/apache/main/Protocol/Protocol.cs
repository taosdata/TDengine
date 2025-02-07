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
    /// A set of messages forming an application protocol.
    /// </summary>
    public class Protocol
    {
        /// <summary>
        /// Name of the protocol
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Namespace of the protocol
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Documentation for the protocol
        /// </summary>
        public string Doc { get; set; }

        /// <summary>
        /// List of schemas objects representing the different schemas defined under the 'types' attribute
        /// </summary>
        public IList<Schema> Types { get; set; }

        /// <summary>
        /// List of message objects representing the different schemas defined under the 'messages' attribute
        /// </summary>
        public IDictionary<string,Message> Messages { get; set; }

        private byte[] md5;

        /// <summary>
        /// MD5 hash of the text of this protocol.
        /// </summary>
        public byte[] MD5
        {
            get
            {
                try
                {
                    if (md5 == null)
                        md5 = System.Security.Cryptography.MD5.Create().ComputeHash(Encoding.UTF8.GetBytes(ToString()));
                }
                catch (Exception ex)
                {
                    throw new AvroRuntimeException("MD5 get exception", ex);
                }
                return md5;
            }
        }

        /// <summary>
        /// Constructor for Protocol class
        /// </summary>
        /// <param name="name">required name of protocol</param>
        /// <param name="space">optional namespace</param>
        /// <param name="doc">optional documentation</param>
        /// <param name="types">required list of types</param>
        /// <param name="messages">required list of messages</param>
        public Protocol(string name, string space,
                        string doc, IEnumerable<Schema> types,
                        IDictionary<string,Message> messages)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name), "name cannot be null.");
            if (null == types) throw new ArgumentNullException(nameof(types), "types cannot be null.");
            if (null == messages) throw new ArgumentNullException(nameof(messages), "messages cannot be null.");

            this.Name = name;
            this.Namespace = space;
            this.Doc = doc;
            this.Types = new List<Schema>(types);
            this.Messages = new Dictionary<string, Message>(messages);
        }

        /// <summary>
        /// Parses the given JSON string to create a Protocol object
        /// </summary>
        /// <param name="jstring">JSON string</param>
        /// <returns>Protocol object</returns>
        public static Protocol Parse(string jstring)
        {
            if (string.IsNullOrEmpty(jstring)) throw new ArgumentNullException("json", "json cannot be null.");

            JToken jtok = null;
            try
            {
                jtok = JObject.Parse(jstring);
            }
            catch (Exception ex)
            {
                throw new ProtocolParseException($"Invalid JSON format: {jstring} at '{jtok.Path}'", ex);
            }
            return Parse(jtok);
        }

        /// <summary>
        /// Parses the given JSON object to create a Protocol object
        /// </summary>
        /// <param name="jtok">JSON object</param>
        /// <returns>Protocol object</returns>
        private static Protocol Parse(JToken jtok)
        {
            string name = JsonHelper.GetRequiredString(jtok, "protocol");
            string space = JsonHelper.GetOptionalString(jtok, "namespace");
            string doc = JsonHelper.GetOptionalString(jtok, "doc");

            var names = new SchemaNames();

            JToken jtypes = jtok["types"];
            var types = new List<Schema>();
            if (jtypes is JArray)
            {
                foreach (JToken jtype in jtypes)
                {
                    var schema = Schema.ParseJson(jtype, names, space);
                    types.Add(schema);
                }
            }

            var messages = new Dictionary<string,Message>();
            JToken jmessages = jtok["messages"];
            if (null != jmessages)
            {
                foreach (JProperty jmessage in jmessages)
                {
                    var message = Message.Parse(jmessage, names, space);
                    messages.Add(message.Name, message);
                }
            }
            return new Protocol(name, space, doc, types, messages);
        }

        /// <summary>
        /// Writes Protocol in JSON format
        /// </summary>
        /// <returns>JSON string</returns>
        public override string ToString()
        {
            using (System.IO.StringWriter sw = new System.IO.StringWriter())
            {
                using (Newtonsoft.Json.JsonTextWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
                {
#if DEBUG
                    writer.Formatting = Newtonsoft.Json.Formatting.Indented;
#endif

                    WriteJson(writer, new SchemaNames());
                    writer.Flush();
                    return sw.ToString();
                }
            }
        }

        /// <summary>
        /// Writes Protocol in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        internal void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names)
        {
            writer.WriteStartObject();

            JsonHelper.writeIfNotNullOrEmpty(writer, "protocol", this.Name);
            JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.Namespace);
            JsonHelper.writeIfNotNullOrEmpty(writer, "doc", this.Doc);

            writer.WritePropertyName("types");
            writer.WriteStartArray();

            foreach (Schema type in this.Types)
                type.WriteJson(writer, names, this.Namespace);

            writer.WriteEndArray();

            writer.WritePropertyName("messages");
            writer.WriteStartObject();

            foreach (KeyValuePair<string,Message> message in this.Messages)
            {
                writer.WritePropertyName(message.Key);
                message.Value.writeJson(writer, names, this.Namespace);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        /// <summary>
        /// Tests equality of this protocol object with the passed object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == this) return true;
            if (!(obj is Protocol)) return false;

            Protocol that = obj as Protocol;

            return this.Name.Equals(that.Name, StringComparison.Ordinal)
                && this.Namespace.Equals(that.Namespace, StringComparison.Ordinal)
                && TypesEquals(that.Types)
                && MessagesEquals(that.Messages);
        }

        /// <summary>
        /// Test equality of this protocols Types list with the passed Types list.
        /// Order of schemas does not matter, as long as all types in this protocol
        /// are also defined in the passed protocol
        /// </summary>
        /// <param name="that"></param>
        /// <returns></returns>
        private bool TypesEquals(IList<Schema> that)
        {
            if (Types.Count != that.Count) return false;
            foreach (Schema schema in Types)
                if (!that.Contains(schema)) return false;
            return true;
        }

        /// <summary>
        /// Test equality of this protocols Message map with the passed Message map
        /// Order of messages does not matter, as long as all messages in this protocol
        /// are also defined in the passed protocol
        /// </summary>
        /// <param name="that"></param>
        /// <returns></returns>
        private bool MessagesEquals(IDictionary<string, Message> that)
        {
            if (Messages.Count != that.Count) return false;
            foreach (KeyValuePair<string, Message> pair in Messages)
            {
                if (!that.ContainsKey(pair.Key))
                    return false;
                if (!pair.Value.Equals(that[pair.Key]))
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Returns the hash code of this protocol object
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
#pragma warning disable CA1307 // Specify StringComparison
            return Name.GetHashCode() + Namespace.GetHashCode() +
#pragma warning restore CA1307 // Specify StringComparison
                   GetTypesHashCode() + GetMessagesHashCode();
        }

        /// <summary>
        /// Returns the hash code of the Types list
        /// </summary>
        /// <returns></returns>
        private int GetTypesHashCode()
        {
            int hash = Types.Count;
            foreach (Schema schema in Types)
                hash += schema.GetHashCode();
            return hash;
        }

        /// <summary>
        /// Returns the hash code of the Messages map
        /// </summary>
        /// <returns></returns>
        private int GetMessagesHashCode()
        {
            int hash = Messages.Count;
            foreach (KeyValuePair<string, Message> pair in Messages)
#pragma warning disable CA1307 // Specify StringComparison
                hash += pair.Key.GetHashCode() + pair.Value.GetHashCode();
#pragma warning restore CA1307 // Specify StringComparison
            return hash;
        }
    }
}
