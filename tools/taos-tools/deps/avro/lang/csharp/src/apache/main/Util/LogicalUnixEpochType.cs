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
    /// Base for all logical type implementations that are based on the Unix Epoch date/time.
    /// </summary>
    public abstract class LogicalUnixEpochType<T> : LogicalType
        where T : struct
    {
        /// <summary>
        /// The date and time of the Unix Epoch.
        /// </summary>
        protected static readonly DateTime UnixEpochDateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Initializes the base logical type.
        /// </summary>
        /// <param name="name">The logical type name.</param>
        protected LogicalUnixEpochType(string name)
            : base(name)
        { }

        /// <inheritdoc/>
        public override Type GetCSharpType(bool nullible)
        {
            return nullible ? typeof(T?) : typeof(T);
        }

        /// <inheritdoc/>
        public override bool IsInstanceOfLogicalType(object logicalValue)
        {
            return logicalValue is T;
        }
    }
}
