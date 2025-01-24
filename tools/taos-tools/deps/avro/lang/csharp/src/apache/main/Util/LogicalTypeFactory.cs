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

using System.Collections.Generic;

namespace Avro.Util
{
    /// <summary>
    /// A factory for logical type implementations.
    /// </summary>
    public class LogicalTypeFactory
    {
        private readonly IDictionary<string, LogicalType> _logicalTypes;

        /// <summary>
        /// Returns the <see cref="LogicalTypeFactory" /> singleton.
        /// </summary>
        /// <returns>The <see cref="LogicalTypeFactory" /> singleton. </returns>
        public static LogicalTypeFactory Instance { get; } = new LogicalTypeFactory();

        private LogicalTypeFactory()
        {
            _logicalTypes = new Dictionary<string, LogicalType>()
            {
                { Decimal.LogicalTypeName, new Decimal() },
                { Date.LogicalTypeName, new Date() },
                { TimeMillisecond.LogicalTypeName, new TimeMillisecond() },
                { TimeMicrosecond.LogicalTypeName, new TimeMicrosecond() },
                { TimestampMillisecond.LogicalTypeName, new TimestampMillisecond() },
                { TimestampMicrosecond.LogicalTypeName, new TimestampMicrosecond() },
                { Uuid.LogicalTypeName, new Uuid() }
            };
        }

        /// <summary>
        /// Registers or replaces a logical type implementation.
        /// </summary>
        /// <param name="logicalType">The <see cref="LogicalType"/> implementation that should be registered.</param>
        public void Register(LogicalType logicalType)
        {
            _logicalTypes[logicalType.Name] = logicalType;
        }

        /// <summary>
        /// Retrieves a logical type implementation for a given logical schema.
        /// </summary>
        /// <param name="schema">The schema.</param>
        /// <param name="ignoreInvalidOrUnknown">A flag to indicate if an exception should be thrown for invalid
        /// or unknown logical types.</param>
        /// <returns>A <see cref="LogicalType" />.</returns>
        public LogicalType GetFromLogicalSchema(LogicalSchema schema, bool ignoreInvalidOrUnknown = false)
        {
            try
            {
                if (!_logicalTypes.TryGetValue(schema.LogicalTypeName, out LogicalType logicalType))
                    throw new AvroTypeException("Logical type '" + schema.LogicalTypeName + "' is not supported.");

                logicalType.ValidateSchema(schema);

                return logicalType;
            }
            catch (AvroTypeException)
            {
                if (!ignoreInvalidOrUnknown)
                    throw;
            }

            return null;
        }
    }
}
