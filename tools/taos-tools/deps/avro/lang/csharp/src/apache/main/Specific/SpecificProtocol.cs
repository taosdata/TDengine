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
    /// Defines the interface for a class that implements a specific protocol.
    /// TODO: This interface needs better documentation.
    /// </summary>
    public interface ISpecificProtocol
    {
        /// <summary>
        /// Protocol for this instance.
        /// </summary>
        Protocol Protocol { get; }

        /// <summary>
        /// Execute a request.
        /// </summary>
        /// <param name="requestor">Callback requestor.</param>
        /// <param name="messageName">Name of the message.</param>
        /// <param name="args">Arguments for the message.</param>
        /// <param name="callback">Callback.</param>
        void Request(ICallbackRequestor requestor, string messageName, object[] args, object callback);
    }

    /// <summary>
    /// TODO: This interface needs better documentation.
    /// </summary>
    public interface ICallbackRequestor
    {
        /// <summary>
        /// Request
        /// </summary>
        /// <typeparam name="T">Type</typeparam>
        /// <param name="messageName">Name of the message.</param>
        /// <param name="args">Arguments for the message.</param>
        /// <param name="callback">Callback.</param>
        void Request<T>(string messageName, object[] args, object callback);
    }

}
