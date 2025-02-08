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

namespace Avro.Specific
{
    /// <summary>
    /// Base class for specific exceptions.
    /// </summary>
    public abstract class SpecificException : Exception, ISpecificRecord
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SpecificException"/> class.
        /// </summary>
        public SpecificException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpecificException"/> class.
        /// </summary>
        /// <param name="message">
        /// The error message that explains the reason for the exception.
        /// </param>
        public SpecificException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SpecificException"/> class.
        /// </summary>
        /// <param name="message">
        /// The error message that explains the reason for the exception.
        /// </param>
        /// <param name="innerException">
        /// The exception that is the cause of the current exception, or a null reference if no
        /// inner exception is specified.
        /// </param>
        public SpecificException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <inheritdoc/>
        public abstract Schema Schema { get; }

        /// <inheritdoc/>
        public abstract object Get(int fieldPos);

        /// <inheritdoc/>
        public abstract void Put(int fieldPos, object fieldValue);
    }
}
