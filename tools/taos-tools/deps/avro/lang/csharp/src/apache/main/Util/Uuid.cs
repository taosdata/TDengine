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

namespace Avro.Util
{
    /// <summary>
    /// UUid logical type
    /// </summary>
    public class Uuid : LogicalType
    {
        /// <summary>
        /// Logical type name
        /// </summary>
        public static readonly string LogicalTypeName = "uuid";

        /// <summary>
        /// Constructs Uuid object
        /// </summary>
        public Uuid() : base(LogicalTypeName) { }


        /// <inheritdoc />
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            return logicalValue.ToString();
        }

        /// <inheritdoc />
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            return new Guid((string) baseValue);
        }

        /// <inheritdoc />
        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(Guid?) : typeof(Guid);
        }

        /// <inheritdoc />
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is Guid;
        }

        /// <inheritdoc />
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.String != schema.BaseSchema.Tag)
                throw new AvroTypeException("'uuid' can only be used with an underlying string type");
        }
    }
}
