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
    /// Convert C# DateTimeOffset properties to long unix time
    /// </summary>
    public class DateTimeOffsetToLongConverter : IAvroFieldConverter
    {
        /// <summary>
        /// Convert from DateTimeOffset to Unix long
        /// </summary>
        /// <param name="o">DateTimeOffset</param>
        /// <param name="s">Schema</param>
        /// <returns></returns>
        public object ToAvroType(object o, Schema s)
        {
            var dt = (DateTimeOffset)o;
            return dt.ToUnixTimeMilliseconds();
        }

        /// <summary>
        /// Convert from Unix long to DateTimeOffset
        /// </summary>
        /// <param name="o">long</param>
        /// <param name="s">Schema</param>
        /// <returns></returns>
        public object FromAvroType(object o, Schema s)
        {
            var dt = DateTimeOffset.FromUnixTimeMilliseconds((long)o);
            return dt;
        }

        /// <summary>
        /// Avro type
        /// </summary>
        /// <returns></returns>
        public Type GetAvroType()
        {
            return typeof(long);
        }

        /// <summary>
        /// Property type
        /// </summary>
        /// <returns></returns>
        public Type GetPropertyType()
        {
            return typeof(DateTimeOffset);
        }
    }
}
