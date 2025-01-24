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
using Avro.Util;
using Newtonsoft.Json.Linq;

namespace Avro
{
    /// <summary>
    /// Class for logical type schemas.
    /// </summary>
    public class LogicalSchema : UnnamedSchema
    {
        /// <summary>
        /// Schema for the underlying type that the logical type is based on.
        /// </summary>
        public Schema BaseSchema { get; private set; }

        /// <summary>
        /// The logical type name.
        /// </summary>
        public string LogicalTypeName { get; private set; }

        /// <summary>
        /// The logical type implementation that supports this logical type.
        /// </summary>
        public LogicalType LogicalType { get; private set; }

        internal static LogicalSchema NewInstance(JToken jtok, PropertyMap props, SchemaNames names, string encspace)
        {
            JToken jtype = jtok["type"];
            if (null == jtype) throw new AvroTypeException("Logical Type does not have 'type'");

            return new LogicalSchema(Schema.ParseJson(jtype, names, encspace), JsonHelper.GetRequiredString(jtok, "logicalType"),  props);
        }

        private LogicalSchema(Schema baseSchema, string logicalTypeName,  PropertyMap props) : base(Type.Logical, props)
        {
            if (null == baseSchema) throw new ArgumentNullException(nameof(baseSchema));
            BaseSchema = baseSchema;
            LogicalTypeName = logicalTypeName;
            LogicalType = LogicalTypeFactory.Instance.GetFromLogicalSchema(this);
        }

        /// <summary>
        /// Writes logical schema in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        /// <param name="encspace">enclosing namespace of the schema</param>
        protected internal override void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("type");
            BaseSchema.WriteJson(writer, names, encspace);
            writer.WritePropertyName("logicalType");
            writer.WriteValue(LogicalTypeName);
            if (null != Props)
                Props.WriteJson(writer);
            writer.WriteEndObject();
        }

        /// <summary>
        /// Checks if this schema can read data written by the given schema. Used for decoding data.
        /// </summary>
        /// <param name="writerSchema">writer schema</param>
        /// <returns>true if this and writer schema are compatible based on the AVRO specification, false otherwise</returns>
        public override bool CanRead(Schema writerSchema)
        {
            if (writerSchema.Tag != Tag) return false;

            LogicalSchema that = writerSchema as LogicalSchema;
            return BaseSchema.CanRead(that.BaseSchema);
        }

        /// <summary>
        /// Function to compare equality of two logical schemas
        /// </summary>
        /// <param name="obj">other logical schema</param>
        /// <returns>true if two schemas are equal, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (this == obj) return true;

            if (obj != null && obj is LogicalSchema that)
            {
                if (BaseSchema.Equals(that.BaseSchema))
                    return areEqual(that.Props, Props);
            }
            return false;
        }

        /// <summary>
        /// Hashcode function
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return 29 * BaseSchema.GetHashCode() + getHashCode(Props);
        }
    }
}
