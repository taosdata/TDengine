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

namespace Avro.Reflect
{
    /// <summary>
    /// Extension methods for ArraySchema - make helper metadata look more like a property
    /// </summary>
    public static class ArraySchemaExtensions
    {
        /// <summary>
        /// Return the name of the array helper
        /// </summary>
        /// <param name="ars">this</param>
        /// <returns>value of the helper metadata - null if it isnt present</returns>
        public static string GetHelper(this ArraySchema ars)
        {
            string s = null;
            s = ars.GetProperty("helper");
            if (s != null && s.Length > 2)
            {
                s = s.Substring(1, s.Length - 2);
            }
            else
            {
                s = null;
            }

            return s;
        }
    }
}
