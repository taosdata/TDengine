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

namespace Avro.File
{
    /// <summary>
    /// Defines the interface for an object that stores in a file a sequence of data conforming to
    /// a schema.
    /// </summary>
    /// <typeparam name="T">Type that we will serialize to the file.</typeparam>
    public interface IFileWriter<T> : IDisposable
    {
        /// <summary>
        /// Append datum to a file or stream.
        /// </summary>
        /// <param name="datum">Datum to append.</param>
        void Append(T datum);

        /// <summary>
        /// Closes the file or stream.
        /// </summary>
        void Close();

        /// <summary>
        /// Flush out any buffered data.
        /// </summary>
        void Flush();

        /// <summary>
        /// Returns true if parameter is a reserved Avro metadata value.
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <returns>
        /// True if parameter is a reserved Avro metadata value, false otherwise.
        /// </returns>
        bool IsReservedMeta(string key);

        /// <summary>
        /// Set metadata pair.
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        void SetMeta(string key, byte[] value);

        /// <summary>
        /// Set metadata pair (long value).
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        void SetMeta(string key, long value);

        /// <summary>
        /// Set metadata pair (string value).
        /// </summary>
        /// <param name="key">Metadata key.</param>
        /// <param name="value">Metadata value.</param>
        void SetMeta(string key, string value);

        /// <summary>
        /// Set the synchronization interval for this file or stream, in bytes. Valid values range
        /// from 32 to 2^30. Suggested values are between 2K and 2M.
        /// </summary>
        /// <param name="syncInterval">
        /// Approximate number of uncompressed bytes to write in each block.
        /// </param>
        void SetSyncInterval(int syncInterval);

        /// <summary>
        /// Forces the end of the current block, emitting a synchronization marker.
        /// </summary>
        /// <returns>
        /// Current position as a value that may be passed to
        /// <see cref="IFileReader{T}.Seek(long)"/>.
        /// </returns>
        long Sync();
    }
}
