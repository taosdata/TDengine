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

namespace Avro.File
{
    /// <summary>
    /// Constants used in data files.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design",
        "CA1052:Static holder types should be Static or NotInheritable",
        Justification = "Maintain public API")]
    public class DataFileConstants
    {
        /// <summary>
        /// Key for the 'sync' metadata entry.
        /// </summary>
        public const string MetaDataSync = "avro.sync";

        /// <summary>
        /// Key for the 'codec' metadata entry.
        /// </summary>
        public const string MetaDataCodec = "avro.codec";

        /// <summary>
        /// Key for the 'schema' metadata entry.
        /// </summary>
        public const string MetaDataSchema = "avro.schema";

        /// <summary>
        /// Identifier for the null codec.
        /// </summary>
        public const string NullCodec = "null";

        /// <summary>
        /// Identifier for the deflate codec.
        /// </summary>
        public const string DeflateCodec = "deflate";

        /// <summary>
        /// Reserved 'avro' metadata key.
        /// </summary>
        public const string MetaDataReserved = "avro";

        /// <summary>
        /// Avro specification version.
        /// </summary>
        public const int Version = 1;

        /// <summary>
        /// Magic bytes at the beginning of an Avro data file.
        /// </summary>
        public static byte[] Magic = { (byte)'O',
                                       (byte)'b',
                                       (byte)'j',
                                       Version };

        /// <summary>
        /// Hash code for the null codec.
        /// </summary>
        /// <seealso cref="NullCodec.GetHashCode()"/>
        public const int NullCodecHash = 2;

        /// <summary>
        /// Hash code for the deflate codec.
        /// </summary>
        /// <seealso cref="DeflateCodec.GetHashCode()"/>
        public const int DeflateCodecHash = 0;

        /// <summary>
        /// Size of a sync token in bytes.
        /// </summary>
        public const int SyncSize = 16;

        /// <summary>
        /// Default interval for sync tokens.
        /// </summary>
        public const int DefaultSyncInterval = 4000 * SyncSize;
    }
}
