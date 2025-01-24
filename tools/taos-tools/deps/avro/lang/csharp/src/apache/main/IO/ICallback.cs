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

namespace Avro.IO
{
    /// <summary>
    /// Obsolete - This will be removed from the public API in a future version.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Obsolete("This will be removed from the public API in a future version.")]
    public interface ICallback<in T>
    {
        /// <summary>
        /// Receives a callback result.
        /// </summary>
        /// <param name="result">Result returned in the callback.</param>
        void HandleResult(T result);

        /// <summary>
        /// Receives an error.
        /// </summary>
        /// <param name="exception">Error returned in the callback.</param>
        void HandleException(Exception exception);
    }
}
