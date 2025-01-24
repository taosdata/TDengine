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

namespace Avro.Reflect
{
    /// <summary>
    /// Converters can be added to properties with an AvroField attribute. Converters convert between the
    /// property type and the avro type.
    /// </summary>
    public interface IAvroFieldConverter
    {
        /// <summary>
        /// Convert from the C# type to the avro type
        /// </summary>
        /// <param name="o">Value to convert</param>
        /// <param name="s">Schema</param>
        /// <returns>Converted value</returns>
        object ToAvroType(object o, Schema s);

        /// <summary>
        /// Convert from the avro type to the C# type
        /// </summary>
        /// <param name="o">Value to convert</param>
        /// <param name="s">Schema</param>
        /// <returns>Converted value</returns>
        object FromAvroType(object o, Schema s);

        /// <summary>
        /// Avro type
        /// </summary>
        /// <returns></returns>
        Type GetAvroType();

        /// <summary>
        /// Property type
        /// </summary>
        /// <returns></returns>
        Type GetPropertyType();
    }
}
