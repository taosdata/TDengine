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

namespace Avro.Specific
{
    /// <summary>
    /// Interface class for generated classes
    /// </summary>
    public interface ISpecificRecord
    {
        /// <summary>
        /// Schema of this instance.
        /// </summary>
        Schema Schema { get; }

        /// <summary>
        /// Return the value of a field given its position in the schema.
        /// This method is not meant to be called by user code, but only by
        /// <see cref="SpecificDatumReader{T}"/> implementations.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <returns>Value of the field.</returns>
        object Get(int fieldPos);

        /// <summary>
        /// Set the value of a field given its position in the schema.
        /// This method is not meant to be called by user code, but only by
        /// <see cref="SpecificDatumWriter{T}"/> implementations.
        /// </summary>
        /// <param name="fieldPos">Position of the field.</param>
        /// <param name="fieldValue">Value of the field.</param>
        void Put(int fieldPos, object fieldValue);
    }
}
