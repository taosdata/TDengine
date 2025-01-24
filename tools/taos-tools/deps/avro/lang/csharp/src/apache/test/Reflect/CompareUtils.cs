/**
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
using System.Collections.Generic;
using System.Linq;
namespace Avro.Test
{
    public static class ExtensionMethods
    {
        public static bool SequenceEqual(this byte[] source, byte[] target)
        {
            if (source.Length != target.Length)
            {
                return false;
            }
            for (int i = 0; i < source.Length; i++)
            {
                if (source[i] != target[i])
                {
                    return false;
                }
            }
            return true;
        }

        public static void ForEach<T1,T2>( this IEnumerable<T1> e1, IEnumerable<T2> e2, Action<T1,T2> action)
        {
            foreach(var items in e1.Zip(e2, Tuple.Create))
            {
                action(items.Item1, items.Item2);
            }
        }
    }
}
