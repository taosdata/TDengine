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

using Avro.IO;
using Avro.Generic;

namespace Avro.Reflect
{
    /// <summary>
    /// Generic wrapper class for writing data from specific objects
    /// </summary>
    /// <typeparam name="T">type name of specific object</typeparam>
    public class ReflectWriter<T> : DatumWriter<T>
    {
        /// <summary>
        /// Default writer
        /// </summary>
        public ReflectDefaultWriter Writer { get => _writer; }

        private readonly ReflectDefaultWriter _writer;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="cache"></param>
        public ReflectWriter(Schema schema, ClassCache cache = null)
            : this(new ReflectDefaultWriter(typeof(T), schema, cache))
        {
        }

        /// <summary>
        /// The schema
        /// </summary>
        public Schema Schema { get => _writer.Schema; }

        /// <summary>
        /// Constructor with already created default writer.
        /// </summary>
        /// <param name="writer"></param>
        public ReflectWriter(ReflectDefaultWriter writer)
        {
            _writer = writer;
        }

        /// <summary>
        /// Serializes the given object using this writer's schema.
        /// </summary>
        /// <param name="value">The value to be serialized</param>
        /// <param name="encoder">The encoder to use for serializing</param>
        public void Write(T value, Encoder encoder)
        {
            _writer.Write(value, encoder);
        }
    }
}
