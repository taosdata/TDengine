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
    /// The 'date' logical type.
    /// </summary>
    public class Date : LogicalUnixEpochType<DateTime>
    {
        /// <summary>
        /// The logical type name for Date.
        /// </summary>
        public static readonly string LogicalTypeName = "date";

        /// <summary>
        /// Initializes a new Date logical type.
        /// </summary>
        public Date() : base(LogicalTypeName)
        { }


        /// <inheritdoc/>
        public override void ValidateSchema(LogicalSchema schema)
        {
            if (Schema.Type.Int != schema.BaseSchema.Tag)
                throw new AvroTypeException("'date' can only be used with an underlying int type");
        }

        /// <inheritdoc/>
        public override object ConvertToBaseValue(object logicalValue, LogicalSchema schema)
        {
            var date = ((DateTime)logicalValue).Date;
            return (date - UnixEpochDateTime).Days;
        }

        /// <inheritdoc/>
        public override object ConvertToLogicalValue(object baseValue, LogicalSchema schema)
        {
            var noDays = (int)baseValue;
            return UnixEpochDateTime.AddDays(noDays);
        }
    }
}
